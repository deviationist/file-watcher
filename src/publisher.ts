import "dotenv/config";
import { stat } from "node:fs/promises";
import path from "node:path";
import { classifyInotify, unlinkWhilePending, type PendingOrigin } from "./events.js";
import { spawn } from "node:child_process";
import { statSync, readdirSync } from "node:fs";
import { watch, type FSWatcher } from "chokidar";
import type { EventName } from "chokidar/handler.js";
import { program } from "commander";
import mqtt, { type MqttClient } from "mqtt";
import { configureFileLogger, log, logError, parseCommaSeparated, type MqttChangePayload } from "./shared.js";

const LABEL = "publisher";

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

program
  .name("file-watcher-publisher")
  .description("Watch folders for file changes and publish events to MQTT")
  .option("-w, --watch-folders <paths>", "comma-separated paths to watch", parseCommaSeparated, [])
  .option("-e, --watch-extensions <exts>", "comma-separated extensions (no dots)", parseCommaSeparated, [])
  .option("-i, --ignore-patterns <globs>", "comma-separated basename globs to ignore (e.g. ._*)", parseCommaSeparated, [])
  .option("--watch-events <events>", "comma-separated chokidar events", parseCommaSeparated, [])
  .option("-p, --poll-interval-seconds <n>", "chokidar polling interval in seconds", parseFloat)
  .option("--event-source <name>", "chokidar | inotify")
  .option("--sweep-folders <paths>", "comma-separated folders to re-announce periodically")
  .option("--sweep-interval-seconds <n>", "how often to sweep", parseFloat)
  .option("-s, --stability-threshold-seconds <n>", "seconds file must be stable before emitting event", parseFloat)
  .option("--stability-mode <mode>", "stability strategy: 'await-write-finish' (chokidar gates add+change) or 'add-only' (gate only new files)")
  .option("--use-polling <bool>", "use polling mode (required for CIFS/NAS, default: true)")
  .option("-b, --mqtt-broker-url <url>", "MQTT broker URL (e.g. mqtt://broker.example.com:1883)")
  .option("-t, --mqtt-topic <topic>", "MQTT topic to publish to")
  .option("-u, --mqtt-username <user>", "MQTT username (optional)")
  .option("--mqtt-password <pass>", "MQTT password (optional)")
  .option("--log-file <path>", "also append logs to this file (parent dir auto-created)")
  .parse();

const opts = program.opts<{
  watchFolders: string[];
  watchExtensions: string[];
  ignorePatterns: string[];
  watchEvents: string[];
  pollIntervalSeconds?: number;
  stabilityThresholdSeconds?: number;
  stabilityMode?: string;
  eventSource?: string;
  sweepFolders?: string;
  sweepIntervalSeconds?: number;
  usePolling?: string;
  mqttBrokerUrl?: string;
  mqttTopic?: string;
  mqttUsername?: string;
  mqttPassword?: string;
  logFile?: string;
}>();

// ---------------------------------------------------------------------------
// Config — CLI args override env vars
// ---------------------------------------------------------------------------

type StabilityMode = "await-write-finish" | "add-only";

interface PublisherConfig {
  watchFolders: string[];
  watchExtensions: Set<string>;
  ignorePatterns: string[];
  watchEvents: string[];
  pollIntervalSeconds: number;
  stabilityThresholdSeconds: number;
  stabilityMode: StabilityMode;
  eventSource: string;
  sweepFolders: string[];
  sweepIntervalSeconds: number;
  catchUpWindowSeconds: number;
  catchUpIntervalSeconds: number;
  usePolling: boolean;
  mqttBrokerUrl: string;
  mqttTopic: string;
  mqttUsername?: string;
  mqttPassword?: string;
  logFile?: string;
}

function loadConfig(): PublisherConfig {
  const watchFolders = opts.watchFolders.length > 0
    ? opts.watchFolders
    : (process.env["WATCH_FOLDERS"] ?? "").split(",").map((f) => f.trim()).filter(Boolean);

  const mqttBrokerUrl = opts.mqttBrokerUrl
    ?? process.env["MQTT_BROKER_URL"]?.trim()
    ?? "";

  if (watchFolders.length === 0) {
    logError(LABEL, "WATCH_FOLDERS is required — pass --watch-folders or set WATCH_FOLDERS in env");
    process.exit(1);
  }

  if (!mqttBrokerUrl) {
    logError(LABEL, "MQTT_BROKER_URL is required — pass --mqtt-broker-url or set MQTT_BROKER_URL in env");
    process.exit(1);
  }

  const extensionsList = opts.watchExtensions.length > 0
    ? opts.watchExtensions
    : (process.env["WATCH_EXTENSIONS"] ?? "").split(",").map((e) => e.trim().toLowerCase()).filter(Boolean);
  const watchExtensions = new Set(extensionsList);

  const ignorePatterns = opts.ignorePatterns.length > 0
    ? opts.ignorePatterns
    : (process.env["WATCH_IGNORE_PATTERNS"] ?? "._*").split(",").map((p) => p.trim()).filter(Boolean);

  const watchEvents = opts.watchEvents.length > 0
    ? opts.watchEvents
    : (process.env["WATCH_EVENTS"] ?? "add,unlink,change").split(",").map((e) => e.trim()).filter(Boolean);

  const pollIntervalSeconds = opts.pollIntervalSeconds
    ?? Number(process.env["POLL_INTERVAL_SECONDS"] ?? "10");

  const stabilityThresholdSeconds = opts.stabilityThresholdSeconds
    ?? Number(process.env["STABILITY_THRESHOLD_SECONDS"] ?? "60");

  const stabilityModeRaw = opts.stabilityMode
    ?? process.env["STABILITY_MODE"]?.trim()
    ?? "add-only";
  if (stabilityModeRaw !== "await-write-finish" && stabilityModeRaw !== "add-only") {
    logError(LABEL, `STABILITY_MODE must be 'await-write-finish' or 'add-only' — got '${stabilityModeRaw}'`);
    process.exit(1);
  }
  const stabilityMode: StabilityMode = stabilityModeRaw;

  // "inotify" reads CLOSE_WRITE and MOVED_TO from the kernel, which say a
  // writer closed the file or that it arrived by atomic rename -- both
  // definitive, where a stability window only infers completion from a lack of
  // change. See docs/file-arrival-detection.md for the measurements behind it.
  const eventSource = (opts.eventSource ?? process.env["EVENT_SOURCE"]?.trim() ?? "chokidar");
  if (eventSource !== "chokidar" && eventSource !== "inotify") {
    logError(LABEL, `EVENT_SOURCE must be 'chokidar' or 'inotify' — got '${eventSource}'`);
    process.exit(1);
  }

  // Belt and braces for the inotify path: inotify can drop events when its
  // queue overflows, and with no polling behind it a dropped event is lost
  // rather than late. Re-announcing what is still sitting in a drain-to-empty
  // folder costs nothing, because the consumer refuses a path already in the
  // chain and skips one that has left.
  const sweepFolders = parseCommaSeparated(
    opts.sweepFolders ?? process.env["SWEEP_FOLDERS"] ?? "", []);
  const sweepIntervalSeconds = Number(
    opts.sweepIntervalSeconds ?? process.env["SWEEP_INTERVAL_SECONDS"] ?? "300");
  // Ten minutes covers a restart with room to spare, and is short enough that a
  // catch-up after a long outage does not replay a day of work. Set 0 to disable.
  const catchUpIntervalSeconds = Number(process.env["CATCHUP_INTERVAL_SECONDS"] ?? "300");
  // Twice the interval, so consecutive runs overlap and nothing can fall
  // between them. Set either to 0 to disable.
  const catchUpWindowSeconds = Number(
    process.env["CATCHUP_WINDOW_SECONDS"] ?? String(catchUpIntervalSeconds * 2));

  const usePollingRaw = opts.usePolling
    ?? process.env["USE_POLLING"]?.trim()
    ?? "true";
  const usePolling = usePollingRaw !== "false";

  const mqttTopic = opts.mqttTopic
    ?? process.env["MQTT_TOPIC"]?.trim()
    ?? "file-watcher/change";

  const mqttUsername = opts.mqttUsername ?? process.env["MQTT_USERNAME"]?.trim() ?? undefined;
  const mqttPassword = opts.mqttPassword ?? process.env["MQTT_PASSWORD"] ?? undefined;

  const logFile = opts.logFile ?? process.env["LOG_FILE"]?.trim() ?? undefined;

  return {
    watchFolders,
    watchExtensions,
    ignorePatterns,
    watchEvents,
    pollIntervalSeconds,
    stabilityThresholdSeconds,
    stabilityMode,
    eventSource,
    sweepFolders,
    sweepIntervalSeconds,
    catchUpWindowSeconds,
    catchUpIntervalSeconds,
    usePolling,
    mqttBrokerUrl,
    mqttTopic,
    mqttUsername,
    mqttPassword,
    logFile,
  };
}

// ---------------------------------------------------------------------------
// Extension matching
// ---------------------------------------------------------------------------

function matchesExtension(filePath: string, extensions: Set<string>): boolean {
  if (extensions.size === 0) return true;
  const ext = path.extname(filePath).slice(1).toLowerCase();
  return extensions.has(ext);
}

// Glob → basename regex: `*` → `.*`, `?` → `.`, everything else literal.
function globToBasenameRegex(glob: string): RegExp {
  const escaped = glob.replace(/[.+^${}()|[\]\\]/g, "\\$&");
  const pattern = escaped.replace(/\*/g, ".*").replace(/\?/g, ".");
  return new RegExp(`^${pattern}$`);
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------


/// Read arrivals straight from the kernel.
///
/// CLOSE_WRITE means the writer closed the file; MOVED_TO means it arrived by
/// rename and was complete before it had that name. Between them they cover
/// every way a file lands here -- cp, scp, mv either way, rsync, Resilio --
/// which is measured in docs/file-arrival-detection.md.
///
/// CREATE is watched for one reason: to tell a *new* file from a *rewritten*
/// one. CLOSE_WRITE alone cannot -- the kernel reports the same event for both
/// -- so every in-place tag edit was announced as `add`, and consumers routing
/// on the event type lost the distinction silently. The rule is just whether
/// this path's CLOSE_WRITE was preceded by a CREATE.
///
/// Directories are watched, never files: a watch on a file follows the inode,
/// and a rename-into-place replaces it, leaving a watch on nothing.
function startInotify(
  config: PublisherConfig,
  ignoreRegexes: RegExp[],
  handleEvent: (event: string, filePath: string, stats?: { size?: number; mtimeMs?: number }) => void,
): void {
  const dirs = config.watchFolders.filter((f: string) => {
    try { return statSync(f).isDirectory(); } catch { return false; }
  });
  if (dirs.length === 0) {
    logError(LABEL, "EVENT_SOURCE=inotify needs at least one watchable directory");
    process.exit(1);
  }

  const spawnWatcher = (): void => {
    const child = spawn("inotifywait", [
      "-m", "-q", "-r",
      "-e", "close_write", "-e", "create", "-e", "moved_to", "-e", "delete", "-e", "moved_from",
      "--format", "%e|%w%f",
      ...dirs,
    ], { stdio: ["ignore", "pipe", "pipe"] });

    // Paths seen as CREATE and not yet closed -- read and pruned by
    // classifyInotify, which owns the rule and is tested on it.
    const created = new Map<string, number>();
    const CREATE_WINDOW_MS = 10 * 60 * 1000;

    let buffered = "";
    child.stdout.on("data", (chunk: Buffer) => {
      buffered += chunk.toString();
      const lines = buffered.split("\n");
      buffered = lines.pop() ?? "";
      for (const line of lines) {
        if (!line) continue;
        const sep = line.indexOf("|");
        if (sep === -1) continue;
        const events = line.slice(0, sep);
        const filePath = line.slice(sep + 1);
        const base = path.basename(filePath);

        // Temp files, ours and other tools'. rsync writes .name.XXXXXX and
        // Resilio writes .!sync, and both CLOSE_WRITE the temp a moment before
        // renaming it into place -- acting on that ingests a partial file under
        // a name nobody meant.
        if (base.startsWith(".")) continue;
        if (ignoreRegexes.some((r) => r.test(base))) continue;

        // What the line means is decided in events.ts, where it is unit
        // tested. Both rules it encodes were bugs first.
        const kind = classifyInotify(events, filePath, created, Date.now(), CREATE_WINDOW_MS);
        if (kind) handleEvent(kind, filePath);
      }
    });
    // No overflow branch here, deliberately. Measured 2026-08-25: with the
    // reader stopped and `max_queued_events` at 1, 1599 of 1600 events were
    // dropped and inotifywait said **nothing** -- not on stderr, not as an
    // event on stdout, with or without -q. A queue overflow is invisible from
    // this side, so it cannot be reacted to; it can only be reconciled against,
    // which is what the periodic catch-up is for.
    child.stderr.on("data", (c: Buffer) => logError(LABEL, `inotifywait: ${c.toString().trim()}`));
    child.on("exit", (code) => {
      // Going deaf silently is the failure that matters here, so say so and
      // come back rather than sitting there looking healthy.
      logError(LABEL, `inotifywait exited (${code}); restarting in 5s`);
      setTimeout(spawnWatcher, 5000);
    });
  };

  spawnWatcher();
  log(LABEL, `inotify watching ${dirs.length} folder(s) for close_write, create, moved_to`);
  // After the watches exist, not before: a file landing during establishment is
  // exactly what this is for, and running it first would leave that window open.
  catchUp(config, handleEvent, "startup");
  startCatchUp(config, handleEvent);
}

/// Announce whatever changed while nobody was listening.
///
/// Two gaps this closes, both silent and both the same shape -- an event that
/// is never delivered and never reported:
///
///   * **The startup gap.** Between this process starting and inotify watches
///     being established, anything that lands is missed. Every restart is a
///     small window of exactly that.
///   * **Queue overflow.** The kernel queue is finite (16384 events here). Past
///     it, inotify drops events and says so once on stderr. `SWEEP_FOLDERS`
///     recovers a lost arrival in Ingress; nothing recovered a lost *edit* to a
///     library file, and since an edit only reaches Plex through its event,
///     losing one loses the edit.
///
/// Bounded by mtime rather than re-announcing everything: the library is
/// thousands of files and a blanket re-announce would put every one of them
/// through the chain. Only what changed inside the window is announced.
///
/// Announced as `add` even though most of these are edits, because from here
/// the two are indistinguishable -- and the consumer resolves it anyway by
/// asking whether Plex already holds the file.
///
/// Each (path, mtime) is announced at most once. Repeats are safe by
/// construction -- the consumer refuses a path already in the chain, and a
/// re-read Plex agrees with costs nothing -- but a run every ten minutes over
/// an overlapping window would otherwise re-announce the same edit three times
/// for no new information.
const caughtUp = new Map<string, number>();

function catchUp(
  config: PublisherConfig,
  handleEvent: (event: string, filePath: string, stats?: { size?: number; mtimeMs?: number },
                origin?: PendingOrigin) => void,
  reason: string,
): number {
  if (config.catchUpWindowSeconds <= 0) return 0;
  const cutoff = Date.now() - config.catchUpWindowSeconds * 1000;
  let announced = 0;

  const consider = (filePath: string, name: string): void => {
    if (name.startsWith(".")) return;
    if (!matchesExtension(name, config.watchExtensions)) return;
    let st;
    try { st = statSync(filePath); } catch { return; }
    if (st.mtimeMs < cutoff) return;
    if (caughtUp.get(filePath) === st.mtimeMs) return;
    caughtUp.set(filePath, st.mtimeMs);
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

  for (const target of config.watchFolders) {
    try {
      // WATCH_FOLDERS can name a file directly -- master.db is watched that way.
      if (statSync(target).isDirectory()) walk(target);
      else consider(target, path.basename(target));
    } catch { /* gone or unreadable; the mount check elsewhere owns that */ }
  }
  // Forget what has aged out of the window, so this cannot grow without bound
  // on a long-running process.
  for (const [seen, at] of caughtUp) if (at < cutoff) caughtUp.delete(seen);
  if (announced) {
    log(LABEL, `catch-up (${reason}): re-announced ${announced} file(s) `
      + `modified in the last ${config.catchUpWindowSeconds}s`);
  }
  return announced;
}


/// Run the catch-up on a timer, because a dropped event is never reported.
///
/// The window is twice the interval so consecutive runs overlap -- a file
/// modified between two sweeps must be inside at least one of them, and the
/// per-(path, mtime) memory makes the overlap free.
function startCatchUp(
  config: PublisherConfig,
  handleEvent: (event: string, filePath: string, stats?: { size?: number; mtimeMs?: number }) => void,
): void {
  if (config.catchUpIntervalSeconds <= 0 || config.catchUpWindowSeconds <= 0) return;
  setInterval(() => catchUp(config, handleEvent, "periodic"),
              config.catchUpIntervalSeconds * 1000).unref?.();
  log(LABEL, `catch-up every ${config.catchUpIntervalSeconds}s `
    + `over files modified in the last ${config.catchUpWindowSeconds}s`);
}


/// Re-announce whatever is still sitting in a folder that should drain.
///
/// Only for folders that empty by design: re-announcing a library folder would
/// be thousands of messages. The consumer refuses a path already in the chain
/// and skips one that has left, so a repeat here is cheap by construction.
function startSweep(
  config: PublisherConfig,
  handleEvent: (event: string, filePath: string, stats?: { size?: number; mtimeMs?: number }) => void,
): void {
  if (config.sweepFolders.length === 0 || config.sweepIntervalSeconds <= 0) return;
  setInterval(() => {
    for (const dir of config.sweepFolders) {
      let names: string[];
      try { names = readdirSync(dir); } catch { continue; }
      for (const name of names) {
        if (name.startsWith(".")) continue;
        if (!matchesExtension(name, config.watchExtensions)) continue;
        log(LABEL, `sweep: re-announcing ${name}`);
        handleEvent("add", path.join(dir, name));
      }
    }
  }, config.sweepIntervalSeconds * 1000).unref?.();
  log(LABEL, `sweep every ${config.sweepIntervalSeconds}s over ${config.sweepFolders.join(", ")}`);
}

async function main(): Promise<void> {
  const config = loadConfig();
  configureFileLogger(config.logFile, LABEL);

  log(LABEL, "Starting with config:");
  log(LABEL, `  WATCH_FOLDERS:         ${config.watchFolders.join(", ")}`);
  log(LABEL, `  WATCH_EXTENSIONS:      ${[...config.watchExtensions].join(", ") || "(all)"}`);
  log(LABEL, `  WATCH_IGNORE_PATTERNS: ${config.ignorePatterns.join(", ") || "(none)"}`);
  log(LABEL, `  WATCH_EVENTS:          ${config.watchEvents.join(", ")}`);
  log(LABEL, `  USE_POLLING:           ${config.usePolling}`);
  log(LABEL, `  POLL_INTERVAL_SECONDS: ${config.pollIntervalSeconds}`);
  log(LABEL, `  STABILITY_THRESHOLD_S: ${config.stabilityThresholdSeconds}`);
  log(LABEL, `  STABILITY_MODE:        ${config.stabilityMode}`);
  log(LABEL, `  EVENT_SOURCE:          ${config.eventSource}`);
  log(LABEL, `  SWEEP_FOLDERS:         ${config.sweepFolders.join(", ") || "(none)"}`);
  log(LABEL, `  MQTT_BROKER_URL:       ${config.mqttBrokerUrl}`);
  log(LABEL, `  MQTT_TOPIC:            ${config.mqttTopic}`);
  log(LABEL, `  MQTT_USERNAME:         ${config.mqttUsername ?? "(none)"}`);
  log(LABEL, `  LOG_FILE:              ${config.logFile ?? "(disabled)"}`);

  // Connect to MQTT broker (auto-reconnects every reconnectPeriod ms on failure)
  const client: MqttClient = await mqtt.connectAsync(config.mqttBrokerUrl, {
    username: config.mqttUsername,
    password: config.mqttPassword,
    reconnectPeriod: 2000,
  });
  log(LABEL, "Connected to MQTT broker");

  client.on("connect", () => log(LABEL, "Reconnected to MQTT broker"));
  client.on("offline", () => log(LABEL, "Offline — broker unreachable, will retry"));
  client.on("error", (err) => {
    // ECONNREFUSED is expected when broker is restarting; suppress the spam
    if ((err as NodeJS.ErrnoException).code !== "ECONNREFUSED") {
      logError(LABEL, `MQTT error: ${err.message}`);
    }
  });

  // Paths listed verbatim in WATCH_FOLDERS bypass the extension filter — if the
  // user pointed at a specific file (e.g. /mnt/music/rekordbox/master.db), they
  // want events for it regardless of its extension.
  const explicitFiles = new Set(config.watchFolders);

  function publish(event: string, filePath: string): void {
    const payload: MqttChangePayload = {
      event,
      path: filePath,
      timestamp: new Date().toISOString(),
    };
    log(LABEL, `${event}: ${filePath}`);
    client.publish(config.mqttTopic, JSON.stringify(payload), { qos: 1 });
  }

  // add-only mode: track files that have just appeared but haven't proven stable yet.
  // Change events during this window are suppressed (it's still the same write).
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
    setTimeout(() => { void verifyStability(filePath); }, config.stabilityThresholdSeconds * 1000);
  }

  function handleEvent(
    event: string,
    filePath: string,
    stats?: { size?: number; mtimeMs?: number },
    origin: PendingOrigin = "live",
  ): void {
    if (!explicitFiles.has(filePath) && !matchesExtension(filePath, config.watchExtensions)) return;

    if (config.stabilityMode === "add-only") {
      if (event === "add") {
        if (pendingAdds.has(filePath)) return;
        if (stats?.size === undefined || stats?.mtimeMs === undefined) {
          // chokidar didn't give us stats — publish immediately rather than guess
          publish(event, filePath);
          return;
        }
        pendingAdds.set(filePath, { size: stats.size, mtimeMs: stats.mtimeMs, origin });
        setTimeout(() => { void verifyStability(filePath); }, config.stabilityThresholdSeconds * 1000);
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

  const ignoreRegexes = config.ignorePatterns.map(globToBasenameRegex);

    if (config.eventSource === "inotify") {
      startInotify(config, ignoreRegexes, handleEvent);
      startSweep(config, handleEvent);
      return;
    }


  // Start watching
  const watcher: FSWatcher = watch(config.watchFolders, {
    usePolling: config.usePolling,
    interval: config.usePolling ? config.pollIntervalSeconds * 1000 : undefined,
    ignoreInitial: true,
    ignored: ignoreRegexes.length > 0
      ? (filePath: string) => ignoreRegexes.some((r) => r.test(path.basename(filePath)))
      : undefined,
    // In add-only mode we do stability checks ourselves so change events fire immediately.
    awaitWriteFinish: config.stabilityMode === "await-write-finish"
      ? { stabilityThreshold: config.stabilityThresholdSeconds * 1000, pollInterval: 1000 }
      : false,
  });

  for (const event of config.watchEvents) {
    const eventName = event as EventName;
    watcher.on(eventName, (filePath: string, stats?: { size?: number; mtimeMs?: number }) =>
      handleEvent(event, filePath, stats),
    );
  }

  watcher.on("error", (error: unknown) => {
    logError(LABEL, `Watcher error: ${error instanceof Error ? error.message : String(error)}`);
  });

  watcher.on("ready", () => {
    log(LABEL, "Watcher is ready and scanning for changes");
  });

  // Graceful shutdown
  function shutdown(signal: string): void {
    log(LABEL, `Received ${signal} — shutting down`);
    client.end(true, () => {
      log(LABEL, "MQTT client disconnected");
      watcher.close().then(() => {
        log(LABEL, "Watcher closed — exiting");
        process.exit(0);
      });
    });
  }

  process.on("SIGINT", () => shutdown("SIGINT"));
  process.on("SIGTERM", () => shutdown("SIGTERM"));
}

main().catch((err) => {
  logError(LABEL, `Fatal: ${err instanceof Error ? err.message : String(err)}`);
  process.exit(1);
});
