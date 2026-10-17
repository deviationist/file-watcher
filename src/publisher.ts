import "dotenv/config";
import path from "node:path";
import { watch, type FSWatcher } from "chokidar";
import type { EventName } from "chokidar/handler.js";
import { program } from "commander";
import mqtt, { type MqttClient } from "mqtt";
import { loadPublisherConfig, type PublisherOpts } from "./config.js";
import { InotifySource, probeFs } from "./inotify.js";
import { createPipeline, globToBasenameRegex } from "./pipeline.js";
import { configureFileLogger, log, logError, parseCommaSeparated } from "./shared.js";
import { describe, parseTargets, resolveTarget } from "./watchplan.js";

const LABEL = "publisher";

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

program
  .name("file-watcher-publisher")
  .description("Watch folders and files for changes and publish events to MQTT")
  .option("-w, --watch-paths <paths>", "comma-separated folders and/or files to watch; "
    + "a path that does not exist yet is watched for (trailing slash: it will be a folder)",
    parseCommaSeparated, [])
  .option("--watch-folders <paths>", "deprecated name for --watch-paths", parseCommaSeparated, [])
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

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
  const loaded = loadPublisherConfig(program.opts<PublisherOpts>(), process.env);
  if (!loaded.ok) {
    logError(LABEL, loaded.error);
    process.exit(1);
  }
  const config = loaded.config;
  configureFileLogger(config.logFile, LABEL);
  for (const w of loaded.warnings) logError(LABEL, w);

  const parsed = parseTargets(config.watchPaths);
  if (parsed.errors.length > 0) {
    for (const e of parsed.errors) logError(LABEL, e);
    process.exit(1);
  }
  const targets = parsed.targets;

  log(LABEL, "Starting with config:");
  log(LABEL, `  WATCH_PATHS:           ${targets.map((t) => describe(resolveTarget(t, probeFs))).join(", ")}`);
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

  const pipeline = createPipeline(
    { ...config, watchPaths: targets.map((t) => t.path) },
    {
      publish: (payload) => { client.publish(config.mqttTopic, JSON.stringify(payload), { qos: 1 }); },
      log: (m) => log(LABEL, m),
    },
  );
  const ignoreRegexes = config.ignorePatterns.map(globToBasenameRegex);

  let closeSource: () => Promise<void>;

  if (config.eventSource === "inotify") {
    const source = new InotifySource({
      targets,
      ignoreRegexes,
      onEvent: (event, filePath) => pipeline.handleKernelEvent(event, filePath),
      // A path that appeared after startup was unwatched from the moment it was
      // created until its watch was set up. Reconcile just that path.
      onSettled: (state) => { pipeline.catchUp(`${state.path} appeared`, [state.path]); },
      log: (m) => log(LABEL, m),
      logError: (m) => logError(LABEL, m),
    });
    await source.start();
    // After the watches exist, not before: a file landing during establishment
    // is exactly what this is for, and running it first would leave that open.
    pipeline.catchUp("startup");
    pipeline.startTimers();
    closeSource = () => source.stop();
  } else {
    const watcher: FSWatcher = watch(targets.map((t) => t.path), {
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
      watcher.on(event as EventName, (filePath: string, stats?: { size?: number; mtimeMs?: number }) =>
        pipeline.handleEvent(event, filePath, stats));
    }
    watcher.on("error", (error: unknown) => {
      logError(LABEL, `Watcher error: ${error instanceof Error ? error.message : String(error)}`);
    });
    watcher.on("ready", () => log(LABEL, "Watcher is ready and scanning for changes"));
    closeSource = () => watcher.close();
  }

  function shutdown(signal: string): void {
    log(LABEL, `Received ${signal} — shutting down`);
    pipeline.stop();
    client.end(true, () => {
      log(LABEL, "MQTT client disconnected");
      void closeSource().then(() => {
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
