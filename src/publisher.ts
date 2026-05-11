import "dotenv/config";
import path from "node:path";
import { watch, type FSWatcher } from "chokidar";
import type { EventName } from "chokidar/handler.js";
import { program } from "commander";
import mqtt, { type MqttClient } from "mqtt";
import { log, logError, parseCommaSeparated, type MqttChangePayload } from "./shared.js";

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
  .option("-s, --stability-threshold-seconds <n>", "seconds file must be stable before emitting event", parseFloat)
  .option("--use-polling <bool>", "use polling mode (required for CIFS/NAS, default: true)")
  .option("-b, --mqtt-broker-url <url>", "MQTT broker URL (e.g. mqtt://localhost:1883)")
  .option("-t, --mqtt-topic <topic>", "MQTT topic to publish to")
  .option("-u, --mqtt-username <user>", "MQTT username (optional)")
  .option("--mqtt-password <pass>", "MQTT password (optional)")
  .parse();

const opts = program.opts<{
  watchFolders: string[];
  watchExtensions: string[];
  ignorePatterns: string[];
  watchEvents: string[];
  pollIntervalSeconds?: number;
  stabilityThresholdSeconds?: number;
  usePolling?: string;
  mqttBrokerUrl?: string;
  mqttTopic?: string;
  mqttUsername?: string;
  mqttPassword?: string;
}>();

// ---------------------------------------------------------------------------
// Config — CLI args override env vars
// ---------------------------------------------------------------------------

interface PublisherConfig {
  watchFolders: string[];
  watchExtensions: Set<string>;
  ignorePatterns: string[];
  watchEvents: string[];
  pollIntervalSeconds: number;
  stabilityThresholdSeconds: number;
  usePolling: boolean;
  mqttBrokerUrl: string;
  mqttTopic: string;
  mqttUsername?: string;
  mqttPassword?: string;
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

  const usePollingRaw = opts.usePolling
    ?? process.env["USE_POLLING"]?.trim()
    ?? "true";
  const usePolling = usePollingRaw !== "false";

  const mqttTopic = opts.mqttTopic
    ?? process.env["MQTT_TOPIC"]?.trim()
    ?? "file-watcher/change";

  const mqttUsername = opts.mqttUsername ?? process.env["MQTT_USERNAME"]?.trim() ?? undefined;
  const mqttPassword = opts.mqttPassword ?? process.env["MQTT_PASSWORD"] ?? undefined;

  return {
    watchFolders,
    watchExtensions,
    ignorePatterns,
    watchEvents,
    pollIntervalSeconds,
    stabilityThresholdSeconds,
    usePolling,
    mqttBrokerUrl,
    mqttTopic,
    mqttUsername,
    mqttPassword,
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

async function main(): Promise<void> {
  const config = loadConfig();

  log(LABEL, "Starting with config:");
  log(LABEL, `  WATCH_FOLDERS:         ${config.watchFolders.join(", ")}`);
  log(LABEL, `  WATCH_EXTENSIONS:      ${[...config.watchExtensions].join(", ") || "(all)"}`);
  log(LABEL, `  WATCH_IGNORE_PATTERNS: ${config.ignorePatterns.join(", ") || "(none)"}`);
  log(LABEL, `  WATCH_EVENTS:          ${config.watchEvents.join(", ")}`);
  log(LABEL, `  USE_POLLING:           ${config.usePolling}`);
  log(LABEL, `  POLL_INTERVAL_SECONDS: ${config.pollIntervalSeconds}`);
  log(LABEL, `  STABILITY_THRESHOLD_S: ${config.stabilityThresholdSeconds}`);
  log(LABEL, `  MQTT_BROKER_URL:       ${config.mqttBrokerUrl}`);
  log(LABEL, `  MQTT_TOPIC:            ${config.mqttTopic}`);
  log(LABEL, `  MQTT_USERNAME:         ${config.mqttUsername ?? "(none)"}`);

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

  // Handle file events
  function handleEvent(event: string, filePath: string): void {
    if (!matchesExtension(filePath, config.watchExtensions)) return;

    const payload: MqttChangePayload = {
      event,
      path: filePath,
      timestamp: new Date().toISOString(),
    };

    log(LABEL, `${event}: ${filePath}`);
    client.publish(config.mqttTopic, JSON.stringify(payload), { qos: 1 });
  }

  const ignoreRegexes = config.ignorePatterns.map(globToBasenameRegex);

  // Start watching
  const watcher: FSWatcher = watch(config.watchFolders, {
    usePolling: config.usePolling,
    interval: config.usePolling ? config.pollIntervalSeconds * 1000 : undefined,
    ignoreInitial: true,
    ignored: ignoreRegexes.length > 0
      ? (filePath: string) => ignoreRegexes.some((r) => r.test(path.basename(filePath)))
      : undefined,
    awaitWriteFinish: {
      stabilityThreshold: config.stabilityThresholdSeconds * 1000,
      pollInterval: 1000,
    },
  });

  for (const event of config.watchEvents) {
    const eventName = event as EventName;
    watcher.on(eventName, (filePath: string) => handleEvent(event, filePath));
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
