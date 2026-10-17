/// Read arrivals straight from the kernel, for whatever a watch list names.
///
/// CLOSE_WRITE means the writer closed the file; MOVED_TO means it arrived by
/// rename and was complete before it had that name. Between them they cover
/// every way a file lands here -- cp, scp, mv either way, rsync, Resilio --
/// which is measured in docs/file-arrival-detection.md.
///
/// CREATE is watched for one reason: to tell a *new* file from a *rewritten*
/// one (see classifyInotify).
///
/// What is watched is decided by watchplan.ts. This module only runs it: one
/// inotifywait per watch, started and stopped as the plan changes, so a path
/// appearing or disappearing never restarts the watches that did not change --
/// a restart of the library's recursive watch is minutes of re-establishing
/// thousands of watches, and anything landing meanwhile is caught only by the
/// reconcile.

import { spawn as nodeSpawn, type ChildProcessByStdio } from "node:child_process";
import type { Readable } from "node:stream";
import { statSync } from "node:fs";
import path from "node:path";
import { classifyInotify, type WatchEvent } from "./events.js";
import {
  describe, planFor, route, sameWatches,
  type Origin, type Plan, type Probe, type Target, type TargetState,
} from "./watchplan.js";

export const probeFs: Probe = (p) => {
  try { return statSync(p).isDirectory() ? "dir" : "file"; } catch { return "missing"; }
};

export interface InotifyOptions {
  targets: Target[];
  /// Basename regexes to drop. Named files are exempt -- naming one is asking
  /// for it.
  ignoreRegexes: RegExp[];
  onEvent: (event: WatchEvent, filePath: string) => void;
  /// A target came into existence, or changed shape, after the watches were
  /// first set up. Anything written to it between appearing and being watched
  /// was not seen by any watch; the caller reconciles it.
  onSettled?: (state: TargetState) => void;
  log: (message: string) => void;
  logError: (message: string) => void;
  probe?: Probe;
  /// Swappable so a test can stand in a fake inotifywait.
  spawn?: (cmd: string, args: string[]) => ChildProcessByStdio<null, Readable, Readable>;
  /// How often to re-probe targets even when no event said to. Inotify cannot
  /// see a path appear in the gap between probing it and the watch on its
  /// parent being established; this closes that gap. Default 60 s.
  recheckMs?: number;
  /// How long to wait before replacing an inotifywait that exited. Default 5 s.
  restartDelayMs?: number;
}

interface Child {
  key: string;
  origin: Origin;
  proc: ChildProcessByStdio<null, Readable, Readable>;
  ready: Promise<void>;
  stopping: boolean;
}

const EVENTS = ["close_write", "create", "moved_to", "delete", "moved_from", "delete_self", "move_self"];
// How long a CREATE waits for its CLOSE_WRITE before the pair is forgotten.
const CREATE_WINDOW_MS = 10 * 60 * 1000;

export class InotifySource {
  private plan: Plan;
  private readonly targets: Target[];
  private readonly children = new Map<string, Child>();
  // Paths seen as CREATE and not yet closed -- read and pruned by
  // classifyInotify. Shared by every watch, because a file can be created
  // under one plan and closed under the next.
  private readonly created = new Map<string, number>();
  private readonly probe: Probe;
  private readonly spawnFn: (cmd: string, args: string[]) => ChildProcessByStdio<null, Readable, Readable>;
  private recheckTimer: ReturnType<typeof setInterval> | null = null;
  private replanning: Promise<void> = Promise.resolve();
  private replanQueued = false;
  private stopped = false;

  constructor(private readonly opts: InotifyOptions) {
    this.probe = opts.probe ?? probeFs;
    this.spawnFn = opts.spawn ?? ((cmd, args) => nodeSpawn(cmd, args, { stdio: ["ignore", "pipe", "pipe"] }));
    this.targets = opts.targets.map((t) => ({ ...t }));
    this.plan = planFor(this.targets, this.probe);
    this.rememberFolders(this.plan);
  }

  /// A target once seen as a folder stays one. Without this, a library folder
  /// that is deleted (or unmounted) would be re-planned as a *file* not there
  /// yet -- which recovers, since a folder appearing under that name is a
  /// trigger either way, but says the wrong thing in the log at exactly the
  /// moment someone is reading it.
  private rememberFolders(plan: Plan): void {
    plan.states.forEach((s, i) => { if (s.kind === "folder") this.targets[i]!.wantDir = true; });
  }

  /// Resolves once every watch reports it is established, and once the plan has
  /// been re-probed after that -- a path that appeared while the watches were
  /// being set up is picked up before this returns.
  async start(): Promise<void> {
    for (const s of this.plan.states) {
      (s.kind === "awaiting" ? this.opts.logError : this.opts.log)(`watch: ${describe(s)}`);
    }
    await this.apply(this.plan);
    this.opts.log(`inotify: ${this.summary()}`);
    await this.replan("after start");
    const ms = this.opts.recheckMs ?? 60_000;
    if (ms > 0) {
      this.recheckTimer = setInterval(() => { void this.replan("periodic recheck"); }, ms);
      this.recheckTimer.unref?.();
    }
  }

  async stop(): Promise<void> {
    this.stopped = true;
    if (this.recheckTimer) clearInterval(this.recheckTimer);
    await this.replanning;
    await Promise.all([...this.children.values()].map((c) => this.kill(c)));
    this.children.clear();
  }

  states(): TargetState[] { return this.plan.states; }

  summary(): string {
    const files = this.plan.states.filter((s) => s.kind === "file").length;
    const waiting = this.plan.states.filter((s) => s.kind === "awaiting").length;
    return `${this.plan.recursive.length} folder(s) recursively, `
      + `${files} file(s) via ${this.plan.flat.size} parent folder(s)`
      + (waiting ? `, ${waiting} path(s) waiting to appear` : "");
  }

  /// Re-probe every target and change the watches if the answer changed.
  /// Serialised: two events arriving together must not start two watchers.
  ///
  /// `force` re-applies the plan even when nothing changed: a watcher that died
  /// or whose directory was replaced leaves the plan intact and the watch gone.
  replan(reason: string, force = false): Promise<void> {
    if (this.replanQueued && !force) return this.replanning;
    this.replanQueued = true;
    this.replanning = this.replanning.then(async () => {
      this.replanQueued = false;
      if (this.stopped) return;
      // Bounded, because a directory created and removed in a loop would
      // otherwise keep this busy forever. Three passes settle anything real:
      // each one can only move a target one step closer to existing.
      for (let pass = 0; pass < 3; pass++) {
        const next = planFor(this.targets, this.probe);
        const before = this.plan.states;
        const changed = next.states.filter((s, i) => !sameState(s, before[i]!));
        if (changed.length === 0 && !force) break;
        // Something we were listening to is no longer there: that is the line
        // to find in the log when a downstream run did not happen.
        for (const s of changed) {
          (s.kind === "awaiting" ? this.opts.logError : this.opts.log)(`watch: ${describe(s)} (${reason})`);
        }
        // Forced: a watch died or is on the wrong inode. apply() starts only
        // what is missing, so this replaces that one and touches nothing else.
        const watchesChange = force || !sameWatches(this.plan, next);
        force = false;
        this.rememberFolders(next);
        // The new plan is in force before the new watches exist, so routing
        // already knows about a file target whose parent just appeared.
        this.plan = next;
        if (watchesChange) {
          await this.apply(next);
          this.opts.log(`inotify: ${this.summary()}`);
        }
        for (const s of changed) {
          if (s.kind !== "awaiting") this.opts.onSettled?.(s);
        }
      }
    });
    return this.replanning;
  }

  private async apply(plan: Plan): Promise<void> {
    const wanted = new Map<string, Origin>();
    for (const root of plan.recursive) wanted.set(`R:${root}`, { kind: "recursive", root });
    for (const dir of plan.flat.keys()) wanted.set(`F:${dir}`, { kind: "flat", dir });

    const gone = [...this.children.values()].filter((c) => !wanted.has(c.key));
    for (const c of gone) this.children.delete(c.key);
    await Promise.all(gone.map((c) => this.kill(c)));

    const started: Promise<void>[] = [];
    for (const [key, origin] of wanted) {
      if (this.children.has(key)) continue;
      const child = this.spawnChild(key, origin);
      this.children.set(key, child);
      started.push(child.ready);
    }
    await Promise.all(started);
  }

  private spawnChild(key: string, origin: Origin): Child {
    const target = origin.kind === "recursive" ? origin.root : origin.dir;
    const args = ["-m", ...(origin.kind === "recursive" ? ["-r"] : [])];
    for (const e of EVENTS) args.push("-e", e);
    // ATTRIB on flat watches only, which exist for named files. A timestamp
    // set without new bytes raises nothing else: measured 2026-10-17, an
    // rbnudge reached xavi as Resilio's OnNotifyAttrChange on the same inode,
    // and the publisher said nothing -- where the polling watcher, comparing
    // mtimes, had always seen it. On a recursive library watch it would be a
    // stream of permission and timestamp noise for no reader.
    if (origin.kind === "flat") args.push("-e", "attrib");
    // Not -q: "Watches established." on stderr is how we know the watch is
    // live, and a caller waiting on start() is waiting on exactly that.
    args.push("--format", "%e|%w%f", target);
    const proc = this.spawnFn("inotifywait", args);

    let markReady!: () => void;
    const ready = new Promise<void>((resolve) => { markReady = resolve; });
    const child: Child = { key, origin, proc, ready, stopping: false };

    let buffered = "";
    proc.stdout.on("data", (chunk: Buffer) => {
      buffered += chunk.toString();
      const lines = buffered.split("\n");
      buffered = lines.pop() ?? "";
      for (const line of lines) this.onLine(child, line);
    });

    let errBuffered = "";
    proc.stderr.on("data", (chunk: Buffer) => {
      errBuffered += chunk.toString();
      const lines = errBuffered.split("\n");
      errBuffered = lines.pop() ?? "";
      for (const line of lines) {
        if (!line.trim()) continue;
        if (line.startsWith("Watches established")) { markReady(); continue; }
        // inotifywait's narration, not trouble.
        if (line.startsWith("Setting up watches") || line.startsWith("Watching new directory")) continue;
        this.opts.logError(`inotifywait ${target}: ${line.trim()}`);
      }
    });

    proc.on("exit", (code) => {
      markReady();   // nothing will ever be established; do not hang start()
      if (child.stopping || this.stopped) return;
      // Going deaf silently is the failure that matters here, so say so and
      // come back rather than sitting there looking healthy. A watch whose
      // directory is gone is expected to end, and the replan says what next.
      const gone = this.probe(target) !== "dir";
      (gone ? this.opts.log : this.opts.logError)(`inotifywait ${target} exited (${code})`
        + `${gone ? ", its folder is gone" : ""}; replanning in ${(this.opts.restartDelayMs ?? 5000) / 1000}s`);
      if (this.children.get(key) === child) this.children.delete(key);
      setTimeout(() => {
        if (this.stopped) return;
        // The plan may be unchanged -- the process died, the path did not --
        // so apply() is what puts the watch back.
        void this.replan("watcher exited", true);
      }, this.opts.restartDelayMs ?? 5000).unref?.();
    });
    return child;
  }

  private onLine(child: Child, line: string): void {
    const origin = child.origin;
    if (!line) return;
    const sep = line.indexOf("|");
    if (sep === -1) return;
    const events = line.slice(0, sep);
    const filePath = line.slice(sep + 1);

    const { pass, replan } = route(this.plan, origin, events, filePath);
    if (replan && (events.includes("DELETE_SELF") || events.includes("MOVE_SELF"))) {
      // This watch is on a directory that is gone or now lives elsewhere. Drop
      // it even if the plan comes out the same: a folder removed and
      // recreated under the same name is a new inode this watch is not on.
      if (this.children.get(child.key) === child) this.children.delete(child.key);
      child.stopping = true;
      void this.kill(child);
      void this.replan(`${events} ${filePath}`, true);
      return;
    }
    if (replan) void this.replan(`${events} ${filePath}`);
    if (!pass) return;

    const named = this.plan.files.has(filePath);
    if (!named) {
      const base = path.basename(filePath);
      // Temp files, ours and other tools'. rsync writes .name.XXXXXX and
      // Resilio writes .!sync, and both CLOSE_WRITE the temp a moment before
      // renaming it into place -- acting on that ingests a partial file under
      // a name nobody meant.
      if (base.startsWith(".")) return;
      if (this.opts.ignoreRegexes.some((r) => r.test(base))) return;
    }

    // What the line means is decided in events.ts, where it is unit tested.
    const kind = classifyInotify(events, filePath, this.created, Date.now(), CREATE_WINDOW_MS);
    if (kind) this.opts.onEvent(kind, filePath);
  }

  private kill(c: Child): Promise<void> {
    c.stopping = true;
    if (c.proc.exitCode !== null || c.proc.signalCode !== null) return Promise.resolve();
    return new Promise((resolve) => {
      c.proc.once("exit", () => resolve());
      c.proc.kill("SIGTERM");
    });
  }
}

/// Whether a target is watched the same way. A file target coming and going is
/// not a change of plan -- its events say so, and `master.db-wal` does it on
/// every Rekordbox session -- so `present` is deliberately not compared.
function sameState(a: TargetState, b: TargetState): boolean {
  if (a.kind !== b.kind || a.path !== b.path) return false;
  if (a.kind === "awaiting" && b.kind === "awaiting") return a.dir === b.dir && a.next === b.next;
  return true;
}
