/// Configuration for both binaries: CLI flags first, then the environment.
///
/// Pure -- it takes the parsed flags and an env object and returns a config or
/// an error -- so the rules are tested without spawning anything. The binaries
/// print the error and exit.

import { parseCommaSeparated } from "./shared.js";
import type { StabilityMode } from "./pipeline.js";

export interface PublisherOpts {
  watchPaths: string[];
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
}

export interface PublisherConfig {
  /// As configured: folders, files, or paths that do not exist yet. A trailing
  /// slash says "this will be a folder".
  watchPaths: string[];
  watchExtensions: Set<string>;
  ignorePatterns: string[];
  watchEvents: string[];
  pollIntervalSeconds: number;
  stabilityThresholdSeconds: number;
  stabilityMode: StabilityMode;
  eventSource: "chokidar" | "inotify";
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

export type Loaded<T> = { ok: true; config: T; warnings: string[] } | { ok: false; error: string };

type Env = Record<string, string | undefined>;

const list = (value: string | undefined): string[] =>
  (value ?? "").split(",").map((s) => s.trim()).filter(Boolean);

/// The watch list, from wherever it was given.
///
/// WATCH_PATHS is the name; WATCH_FOLDERS is what it was called while it could
/// name a file and quietly did not watch one. The old name is still read, with
/// a warning, so a host can take the new code before its .env changes -- and
/// is refused alongside the new one, because two lists that disagree would
/// leave the reader guessing which one is live.
export function resolveWatchPaths(opts: Pick<PublisherOpts, "watchPaths" | "watchFolders">, env: Env):
    { ok: true; paths: string[]; warnings: string[] } | { ok: false; error: string } {
  const warnings: string[] = [];
  if (opts.watchPaths.length > 0 && opts.watchFolders.length > 0) {
    return { ok: false, error: "pass --watch-paths or --watch-folders, not both (--watch-folders is the old name)" };
  }
  if (opts.watchPaths.length > 0) return { ok: true, paths: opts.watchPaths, warnings };
  if (opts.watchFolders.length > 0) {
    warnings.push("--watch-folders is deprecated; use --watch-paths (it takes folders and files)");
    return { ok: true, paths: opts.watchFolders, warnings };
  }
  const fromPaths = list(env["WATCH_PATHS"]);
  const fromFolders = list(env["WATCH_FOLDERS"]);
  if (fromPaths.length > 0 && fromFolders.length > 0) {
    return { ok: false, error: "set WATCH_PATHS or WATCH_FOLDERS, not both (WATCH_FOLDERS is the old name)" };
  }
  if (fromPaths.length > 0) return { ok: true, paths: fromPaths, warnings };
  if (fromFolders.length > 0) {
    warnings.push("WATCH_FOLDERS is deprecated; rename it to WATCH_PATHS (it takes folders and files)");
    return { ok: true, paths: fromFolders, warnings };
  }
  return { ok: false, error: "WATCH_PATHS is required — pass --watch-paths or set WATCH_PATHS in env" };
}

export function loadPublisherConfig(opts: PublisherOpts, env: Env): Loaded<PublisherConfig> {
  const watch = resolveWatchPaths(opts, env);
  if (!watch.ok) return watch;
  const warnings = [...watch.warnings];

  const mqttBrokerUrl = opts.mqttBrokerUrl ?? env["MQTT_BROKER_URL"]?.trim() ?? "";
  if (!mqttBrokerUrl) {
    return { ok: false, error: "MQTT_BROKER_URL is required — pass --mqtt-broker-url or set MQTT_BROKER_URL in env" };
  }

  const watchExtensions = new Set(
    (opts.watchExtensions.length > 0 ? opts.watchExtensions : list(env["WATCH_EXTENSIONS"]))
      .map((e) => e.toLowerCase()));

  const ignorePatterns = opts.ignorePatterns.length > 0
    ? opts.ignorePatterns
    : list(env["WATCH_IGNORE_PATTERNS"] ?? "._*");

  const watchEvents = opts.watchEvents.length > 0
    ? opts.watchEvents
    : list(env["WATCH_EVENTS"] ?? "add,unlink,change");

  const pollIntervalSeconds = opts.pollIntervalSeconds ?? Number(env["POLL_INTERVAL_SECONDS"] ?? "10");
  const stabilityThresholdSeconds = opts.stabilityThresholdSeconds
    ?? Number(env["STABILITY_THRESHOLD_SECONDS"] ?? "60");

  const stabilityMode = opts.stabilityMode ?? env["STABILITY_MODE"]?.trim() ?? "add-only";
  if (stabilityMode !== "await-write-finish" && stabilityMode !== "add-only") {
    return { ok: false, error: `STABILITY_MODE must be 'await-write-finish' or 'add-only' — got '${stabilityMode}'` };
  }

  // "inotify" reads CLOSE_WRITE and MOVED_TO from the kernel, which say a
  // writer closed the file or that it arrived by atomic rename -- both
  // definitive, where a stability window only infers completion from a lack of
  // change. See docs/file-arrival-detection.md for the measurements behind it.
  const eventSource = opts.eventSource ?? env["EVENT_SOURCE"]?.trim() ?? "chokidar";
  if (eventSource !== "chokidar" && eventSource !== "inotify") {
    return { ok: false, error: `EVENT_SOURCE must be 'chokidar' or 'inotify' — got '${eventSource}'` };
  }

  // Belt and braces for the inotify path: re-announcing what is still sitting
  // in a drain-to-empty folder costs nothing, because the consumer refuses a
  // path already in the chain and skips one that has left.
  const sweepFolders = parseCommaSeparated(opts.sweepFolders ?? env["SWEEP_FOLDERS"] ?? "", []);
  const sweepIntervalSeconds = Number(opts.sweepIntervalSeconds ?? env["SWEEP_INTERVAL_SECONDS"] ?? "300");
  // Ten minutes covers a restart with room to spare, and is short enough that a
  // catch-up after a long outage does not replay a day of work. 0 disables.
  const catchUpIntervalSeconds = Number(env["CATCHUP_INTERVAL_SECONDS"] ?? "300");
  // Twice the interval, so consecutive runs overlap and nothing can fall
  // between them. 0 disables.
  const catchUpWindowSeconds = Number(env["CATCHUP_WINDOW_SECONDS"] ?? String(catchUpIntervalSeconds * 2));

  const usePolling = (opts.usePolling ?? env["USE_POLLING"]?.trim() ?? "true") !== "false";

  return {
    ok: true,
    warnings,
    config: {
      watchPaths: watch.paths,
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
      mqttTopic: opts.mqttTopic ?? env["MQTT_TOPIC"]?.trim() ?? "file-watcher/change",
      mqttUsername: opts.mqttUsername ?? env["MQTT_USERNAME"]?.trim() ?? undefined,
      mqttPassword: opts.mqttPassword ?? env["MQTT_PASSWORD"] ?? undefined,
      logFile: opts.logFile ?? env["LOG_FILE"]?.trim() ?? undefined,
    },
  };
}

export interface SubscriberOpts {
  mqttBrokerUrl?: string;
  mqttTopic?: string;
  pathPrefixes: string[];
  debounceSeconds?: number;
  onChangeCommand?: string;
  mqttUsername?: string;
  mqttPassword?: string;
  logFile?: string;
}

export interface SubscriberConfig {
  mqttBrokerUrl: string;
  mqttTopic: string;
  pathPrefixes: string[];
  debounceSeconds: number;
  onChangeCommand: string;
  mqttUsername?: string;
  mqttPassword?: string;
  logFile?: string;
}

export function loadSubscriberConfig(opts: SubscriberOpts, env: Env): Loaded<SubscriberConfig> {
  const mqttBrokerUrl = opts.mqttBrokerUrl ?? env["MQTT_BROKER_URL"]?.trim() ?? "";
  if (!mqttBrokerUrl) {
    return { ok: false, error: "MQTT_BROKER_URL is required — pass --mqtt-broker-url or set MQTT_BROKER_URL in env" };
  }
  const onChangeCommand = opts.onChangeCommand ?? env["ON_CHANGE_COMMAND"]?.trim() ?? "";
  if (!onChangeCommand) {
    return { ok: false, error: "ON_CHANGE_COMMAND is required — pass --on-change-command or set ON_CHANGE_COMMAND in env" };
  }
  return {
    ok: true,
    warnings: [],
    config: {
      mqttBrokerUrl,
      mqttTopic: opts.mqttTopic ?? env["MQTT_TOPIC"]?.trim() ?? "file-watcher/change",
      pathPrefixes: opts.pathPrefixes.length > 0 ? opts.pathPrefixes : list(env["PATH_PREFIXES"]),
      debounceSeconds: opts.debounceSeconds ?? Number(env["DEBOUNCE_SECONDS"] ?? "30"),
      onChangeCommand,
      mqttUsername: opts.mqttUsername ?? env["MQTT_USERNAME"]?.trim() ?? undefined,
      mqttPassword: opts.mqttPassword ?? env["MQTT_PASSWORD"] ?? undefined,
      logFile: opts.logFile ?? env["LOG_FILE"]?.trim() ?? undefined,
    },
  };
}
