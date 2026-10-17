/// Everything between "the filesystem said something" and "a message goes out":
/// the name filters, the stability wait, the change-must-be-real check, the
/// reconcile and the sweep.
///
/// It lived inside publisher.ts's main(), next to the MQTT client, where
/// nothing could test it without a broker. Out here it takes a function to
/// publish through, so a test hands it an array.

import { stat } from "node:fs/promises";
import { readdirSync, statSync } from "node:fs";
import path from "node:path";
import {
  catchUpVerdict, changeIsReal, kernelSettlesPending, unlinkWhilePending,
  type Announced, type PendingOrigin,
} from "./events.js";
import type { MqttChangePayload } from "./shared.js";

export type StabilityMode = "await-write-finish" | "add-only";

export interface PipelineConfig {
  /// Normalised targets. Files named here bypass the extension filter.
  watchPaths: string[];
  watchExtensions: Set<string>;
  stabilityThresholdSeconds: number;
  stabilityMode: StabilityMode;
  sweepFolders: string[];
  sweepIntervalSeconds: number;
  catchUpWindowSeconds: number;
  catchUpIntervalSeconds: number;
}

export type Stats = { size?: number; mtimeMs?: number };

export interface Pipeline {
  handleEvent(event: string, filePath: string, stats?: Stats, origin?: PendingOrigin): void;
  /// An event read from the kernel: CLOSE_WRITE or MOVED_TO behind it, so the
  /// write is known to be finished. See `kernelSettlesPending`.
  handleKernelEvent(event: string, filePath: string): void;
  /// Announce what changed in the window under `paths` (default: every watch
  /// path). Returns how many files it announced.
  catchUp(reason: string, paths?: string[]): number;
  startTimers(): void;
  stop(): void;
}

export interface PipelineDeps {
  publish: (payload: MqttChangePayload) => void;
  log: (message: string) => void;
  now?: () => number;
}

export function matchesExtension(filePath: string, extensions: Set<string>): boolean {
  if (extensions.size === 0) return true;
  const ext = path.extname(filePath).slice(1).toLowerCase();
  return extensions.has(ext);
}

// Glob → basename regex: `*` → `.*`, `?` → `.`, everything else literal.
export function globToBasenameRegex(glob: string): RegExp {
  const escaped = glob.replace(/[.+^${}()|[\]\\]/g, "\\$&");
  const pattern = escaped.replace(/\*/g, ".*").replace(/\?/g, ".");
  return new RegExp(`^${pattern}$`);
}

export function createPipeline(config: PipelineConfig, deps: PipelineDeps): Pipeline {
  const now = deps.now ?? Date.now;
  const timers = new Set<ReturnType<typeof setTimeout>>();
  const later = (fn: () => void, ms: number): void => {
    const t = setTimeout(() => { timers.delete(t); fn(); }, ms);
    timers.add(t);
  };
  const intervals: ReturnType<typeof setInterval>[] = [];

  // Paths named in the watch list bypass the extension filter -- if the user
  // pointed at a specific file (master.db), they want events for it whatever
  // its extension.
  const explicitFiles = new Set(config.watchPaths);

  //: What has been announced for each path, and at which mtime.
  //
  // Written by *both* paths -- the live one and the reconcile -- which is what
  // stops the reconcile re-announcing what the kernel already told us about.
  const announcedAt = new Map<string, Announced>();

  /// Record a live announcement, so the reconcile knows it is already done.
  /// If the file is already gone -- an unlink, or a rename that beat us here --
  /// there is nothing to remember and the entry is dropped instead.
  function rememberAnnounced(event: string, filePath: string): void {
    if (event === "unlink") { announcedAt.delete(filePath); return; }
    try {
      const st = statSync(filePath);
      announcedAt.set(filePath, { mtimeMs: st.mtimeMs, size: st.size });
    } catch { announcedAt.delete(filePath); }
  }

  function publish(event: string, filePath: string): void {
    // A `change` has to prove it is one. CLOSE_WRITE says a file open for
    // writing was closed; it does not say anything was written, and both a tag
    // editor's second write cycle and the phantom close fifteen seconds after a
    // publish exploit that. Only `change`: an arrival is news whatever the
    // bytes say, and a removal has no file left to stat.
    if (event === "change") {
      let st;
      try { st = statSync(filePath); } catch { return; }   // gone; nothing to say
      if (!changeIsReal({ mtimeMs: st.mtimeMs, size: st.size }, announcedAt.get(filePath))) {
        deps.log(`change: ${filePath} — nothing written, not announced`);
        return;
      }
    }
    deps.log(`${event}: ${filePath}`);
    deps.publish({ event, path: filePath, timestamp: new Date(now()).toISOString() });
    // The reconcile's whole job is announcing what the live path missed. Told
    // here, it stops re-announcing what the live path *made*.
    rememberAnnounced(event, filePath);
  }

  // add-only mode: files that have just appeared but have not proven stable
  // yet. Change events during this window are suppressed (same write).
  const pendingAdds = new Map<string, { size: number; mtimeMs: number; origin: PendingOrigin }>();

  async function verifyStability(filePath: string): Promise<void> {
    const prev = pendingAdds.get(filePath);
    if (!prev) return;
    let current;
    try {
      current = await stat(filePath);
    } catch {
      pendingAdds.delete(filePath);
      return;
    }
    if (current.size === prev.size && current.mtimeMs === prev.mtimeMs) {
      pendingAdds.delete(filePath);
      publish("add", filePath);
      return;
    }
    pendingAdds.set(filePath, { size: current.size, mtimeMs: current.mtimeMs, origin: prev.origin });
    later(() => { void verifyStability(filePath); }, config.stabilityThresholdSeconds * 1000);
  }

  function handleEvent(event: string, filePath: string, stats?: Stats,
                       origin: PendingOrigin = "live"): void {
    if (!explicitFiles.has(filePath) && !matchesExtension(filePath, config.watchExtensions)) return;

    if (config.stabilityMode === "add-only") {
      if (event === "add") {
        if (pendingAdds.has(filePath)) return;
        if (stats?.size === undefined || stats?.mtimeMs === undefined) {
          // No stats: an inotify arrival (CLOSE_WRITE after CREATE, or
          // MOVED_TO), which is already proof the write finished -- or the
          // sweep. Publish now; a stability wait would only add latency.
          publish(event, filePath);
          return;
        }
        pendingAdds.set(filePath, { size: stats.size, mtimeMs: stats.mtimeMs, origin });
        later(() => { void verifyStability(filePath); }, config.stabilityThresholdSeconds * 1000);
        return;
      }
      if (event === "change" && pendingAdds.has(filePath)) {
        // Still in the initial write window — suppress; verifyStability will publish the add.
        return;
      }
      const pending = pendingAdds.get(filePath);
      if (event === "unlink" && pending) {
        // Whether this removal is news depends on whether anything downstream
        // has heard of the file -- see unlinkWhilePending, which is where that
        // rule is tested. A live add nobody published is not news; a catch-up
        // re-announcement of a file that has been in the library all along
        // very much is, and dropping it leaves an orphan.
        pendingAdds.delete(filePath);
        if (unlinkWhilePending(pending.origin) === "drop") return;
        publish(event, filePath);
        return;
      }
    }

    publish(event, filePath);
  }

  function handleKernelEvent(event: string, filePath: string): void {
    if (config.stabilityMode === "add-only" && kernelSettlesPending(event, pendingAdds.has(filePath))) {
      pendingAdds.delete(filePath);
      publish("add", filePath);
      return;
    }
    handleEvent(event, filePath);
  }

  /// Announce whatever changed while nobody was listening.
  ///
  /// Two gaps this closes, both silent and both the same shape -- an event that
  /// is never delivered and never reported:
  ///
  ///   * **The startup gap.** Between this process starting and inotify watches
  ///     being established, anything that lands is missed. Every restart is a
  ///     small window of exactly that -- and so is a watched path appearing
  ///     after startup, between its creation and its watch.
  ///   * **Queue overflow.** The kernel queue is finite (16384 events here).
  ///     Past it, inotify drops events and says *nothing* (measured 2026-08-25:
  ///     1599 of 1600 lost, no output at all).
  ///
  /// Bounded by mtime rather than re-announcing everything: the library is
  /// thousands of files. Announced as `add` even though most are edits,
  /// because from here the two are indistinguishable and the consumer settles
  /// it. Each (path, mtime) is announced at most once.
  function catchUp(reason: string, paths: string[] = config.watchPaths): number {
    if (config.catchUpWindowSeconds <= 0) return 0;
    const cutoff = now() - config.catchUpWindowSeconds * 1000;
    let announced = 0;

    const consider = (filePath: string, name: string): void => {
      const named = explicitFiles.has(filePath);
      if (!named && name.startsWith(".")) return;
      if (!named && !matchesExtension(name, config.watchExtensions)) return;
      let st;
      try { st = statSync(filePath); } catch { return; }
      if (catchUpVerdict(st.mtimeMs, announcedAt.get(filePath)?.mtimeMs, cutoff) === "skip") return;
      announcedAt.set(filePath, { mtimeMs: st.mtimeMs, size: st.size });
      announced++;
      handleEvent("add", filePath, { size: st.size, mtimeMs: st.mtimeMs }, "catchup");
    };

    const walk = (dir: string): void => {
      let entries;
      try { entries = readdirSync(dir, { withFileTypes: true }); } catch { return; }
      for (const entry of entries) {
        const full = path.join(dir, entry.name);
        if (entry.isDirectory()) { if (!entry.name.startsWith(".")) walk(full); continue; }
        consider(full, entry.name);
      }
    };

    for (const target of paths) {
      try {
        // A watch path can name a file directly -- master.db is watched that way.
        if (statSync(target).isDirectory()) walk(target);
        else consider(target, path.basename(target));
      } catch { /* not there (yet); the watcher reports that */ }
    }
    // Forget what has aged out of the window, so this cannot grow without bound.
    for (const [seen, a] of announcedAt) if (a.mtimeMs < cutoff) announcedAt.delete(seen);
    if (announced) {
      deps.log(`catch-up (${reason}): re-announced ${announced} file(s) `
        + `modified in the last ${config.catchUpWindowSeconds}s`);
    }
    return announced;
  }

  function startTimers(): void {
    // The reconcile runs on a timer because a dropped event is never reported.
    // The window is twice the interval so consecutive runs overlap.
    if (config.catchUpIntervalSeconds > 0 && config.catchUpWindowSeconds > 0) {
      const t = setInterval(() => catchUp("periodic"), config.catchUpIntervalSeconds * 1000);
      t.unref?.();
      intervals.push(t);
      deps.log(`catch-up every ${config.catchUpIntervalSeconds}s `
        + `over files modified in the last ${config.catchUpWindowSeconds}s`);
    }
    // Re-announce whatever is still sitting in a folder that should drain.
    // Only for folders that empty by design: re-announcing a library folder
    // would be thousands of messages. The consumer refuses a path already in
    // the chain and skips one that has left, so a repeat is cheap.
    if (config.sweepFolders.length > 0 && config.sweepIntervalSeconds > 0) {
      const t = setInterval(() => {
        for (const dir of config.sweepFolders) {
          let names: string[];
          try { names = readdirSync(dir); } catch { continue; }
          for (const name of names) {
            if (name.startsWith(".")) continue;
            if (!matchesExtension(name, config.watchExtensions)) continue;
            deps.log(`sweep: re-announcing ${name}`);
            handleEvent("add", path.join(dir, name));
          }
        }
      }, config.sweepIntervalSeconds * 1000);
      t.unref?.();
      intervals.push(t);
      deps.log(`sweep every ${config.sweepIntervalSeconds}s over ${config.sweepFolders.join(", ")}`);
    }
  }

  function stop(): void {
    for (const t of timers) clearTimeout(t);
    timers.clear();
    for (const t of intervals) clearInterval(t);
  }

  return { handleEvent, handleKernelEvent, catchUp, startTimers, stop };
}
