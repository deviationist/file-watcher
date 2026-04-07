import "dotenv/config";
import { exec } from "node:child_process";
import path from "node:path";
import { watch, type FSWatcher } from "chokidar";
import type { EventName } from "chokidar/handler.js";
import { program } from "commander";

// ---------------------------------------------------------------------------
// Logging
// ---------------------------------------------------------------------------

function log(message: string): void {
  console.log(`${new Date().toISOString()} [file-watcher] ${message}`);
}

function logError(message: string): void {
  console.error(`${new Date().toISOString()} [file-watcher] ERROR: ${message}`);
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

function parseCommaSeparated(value: string, prev: string[]): string[] {
  return prev.concat(value.split(",").map((s) => s.trim()).filter(Boolean));
}

program
  .name("file-watcher")
  .description("Watch folders for file changes and run a configurable command")
  .option("-w, --watch-folders <paths>", "comma-separated paths to watch", parseCommaSeparated, [])
  .option("-e, --watch-extensions <exts>", "comma-separated extensions (no dots)", parseCommaSeparated, [])
  .option("--watch-events <events>", "comma-separated chokidar events", parseCommaSeparated, [])
  .option("-d, --debounce-seconds <n>", "quiet period before firing command", parseFloat)
  .option("-p, --poll-interval-seconds <n>", "chokidar polling interval in seconds", parseFloat)
  .option("-c, --on-change-command <cmd>", "shell command to execute on change")
  .parse();

const opts = program.opts<{
  watchFolders: string[];
  watchExtensions: string[];
  watchEvents: string[];
  debounceSeconds?: number;
  pollIntervalSeconds?: number;
  onChangeCommand?: string;
}>();

// ---------------------------------------------------------------------------
// Config — CLI args override env vars
// ---------------------------------------------------------------------------

interface Config {
  watchFolders: string[];
  watchExtensions: Set<string>;
  watchEvents: string[];
  debounceSeconds: number;
  pollIntervalSeconds: number;
  onChangeCommand: string;
}

function loadConfig(): Config {
  const watchFolders = opts.watchFolders.length > 0
    ? opts.watchFolders
    : (process.env["WATCH_FOLDERS"] ?? "").split(",").map((f) => f.trim()).filter(Boolean);

  const onChangeCommand = opts.onChangeCommand
    ?? process.env["ON_CHANGE_COMMAND"]?.trim()
    ?? "";

  if (watchFolders.length === 0) {
    logError("WATCH_FOLDERS is required — pass --watch-folders or set WATCH_FOLDERS in env");
    process.exit(1);
  }

  if (!onChangeCommand) {
    logError("ON_CHANGE_COMMAND is required — pass --on-change-command or set ON_CHANGE_COMMAND in env");
    process.exit(1);
  }

  const extensionsList = opts.watchExtensions.length > 0
    ? opts.watchExtensions
    : (process.env["WATCH_EXTENSIONS"] ?? "").split(",").map((e) => e.trim().toLowerCase()).filter(Boolean);
  const watchExtensions = new Set(extensionsList);

  const watchEvents = opts.watchEvents.length > 0
    ? opts.watchEvents
    : (process.env["WATCH_EVENTS"] ?? "add,unlink,change").split(",").map((e) => e.trim()).filter(Boolean);

  const debounceSeconds = opts.debounceSeconds
    ?? Number(process.env["DEBOUNCE_SECONDS"] ?? "30");

  const pollIntervalSeconds = opts.pollIntervalSeconds
    ?? Number(process.env["POLL_INTERVAL_SECONDS"] ?? "10");

  return {
    watchFolders,
    watchExtensions,
    watchEvents,
    debounceSeconds,
    pollIntervalSeconds,
    onChangeCommand,
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

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

const config = loadConfig();

log("Starting with config:");
log(`  WATCH_FOLDERS:        ${config.watchFolders.join(", ")}`);
log(`  WATCH_EXTENSIONS:     ${[...config.watchExtensions].join(", ") || "(all)"}`);
log(`  WATCH_EVENTS:         ${config.watchEvents.join(", ")}`);
log(`  DEBOUNCE_SECONDS:     ${config.debounceSeconds}`);
log(`  POLL_INTERVAL_SECONDS: ${config.pollIntervalSeconds}`);
log(`  ON_CHANGE_COMMAND:    ${config.onChangeCommand}`);

let debounceTimer: ReturnType<typeof setTimeout> | null = null;
let pendingEventCount = 0;
let changedPaths: Set<string> = new Set();

function scheduleCommand(): void {
  if (debounceTimer !== null) {
    clearTimeout(debounceTimer);
  }

  debounceTimer = setTimeout(() => {
    debounceTimer = null;
    const count = pendingEventCount;
    const paths = changedPaths;
    pendingEventCount = 0;
    changedPaths = new Set();
    const pathsList = [...paths].join("\n");
    log(`Debounce expired after ${count} event(s) — executing command`);
    log(`Changed paths:\n${pathsList}`);
    exec(config.onChangeCommand, {
      env: { ...process.env, CHANGED_PATHS: pathsList },
    }, (error, stdout, stderr) => {
      if (error) {
        logError(`Command failed (exit ${error.code}): ${error.message}`);
        if (stderr) logError(`stderr: ${stderr.trim()}`);
      } else {
        if (stdout.trim()) log(`Command stdout: ${stdout.trim()}`);
        log("Command completed successfully");
      }
    });
  }, config.debounceSeconds * 1000);
}

function handleEvent(event: string, filePath: string): void {
  if (!matchesExtension(filePath, config.watchExtensions)) return;
  pendingEventCount++;
  changedPaths.add(filePath);
  log(`${event}: ${filePath} (${pendingEventCount} pending, debounce reset)`);
  scheduleCommand();
}

const watcher: FSWatcher = watch(config.watchFolders, {
  usePolling: true,
  interval: config.pollIntervalSeconds * 1000,
  ignoreInitial: true,
  awaitWriteFinish: {
    stabilityThreshold: 2000,
  },
});

for (const event of config.watchEvents) {
  const eventName = event as EventName;
  watcher.on(eventName, (filePath: string) => handleEvent(event, filePath));
}

watcher.on("error", (error: unknown) => {
  logError(`Watcher error: ${error instanceof Error ? error.message : String(error)}`);
});

watcher.on("ready", () => {
  log("Watcher is ready and scanning for changes");
});

// ---------------------------------------------------------------------------
// Graceful shutdown
// ---------------------------------------------------------------------------

function shutdown(signal: string): void {
  log(`Received ${signal} — shutting down`);
  if (debounceTimer !== null) {
    clearTimeout(debounceTimer);
    debounceTimer = null;
    log(`Cleared pending debounce timer (${pendingEventCount} event(s), ${changedPaths.size} path(s) will NOT trigger command)`);
  }
  watcher.close().then(() => {
    log("Watcher closed — exiting");
    process.exit(0);
  });
}

process.on("SIGINT", () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));
