import "dotenv/config";
import { exec } from "node:child_process";
import { program } from "commander";
import mqtt, { type MqttClient } from "mqtt";
import { log, logError, parseCommaSeparated, type MqttChangePayload } from "./shared.js";

const LABEL = "subscriber";

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

program
  .name("file-watcher-subscriber")
  .description("Subscribe to MQTT file-change events, debounce, and run a command")
  .option("-b, --mqtt-broker-url <url>", "MQTT broker URL (e.g. mqtt://localhost:1883)")
  .option("-t, --mqtt-topic <topic>", "MQTT topic to subscribe to")
  .option("-f, --path-prefixes <paths>", "comma-separated path prefixes to filter on", parseCommaSeparated, [])
  .option("-d, --debounce-seconds <n>", "quiet period before firing command", parseFloat)
  .option("-c, --on-change-command <cmd>", "shell command to execute on change")
  .option("-u, --mqtt-username <user>", "MQTT username (optional)")
  .option("--mqtt-password <pass>", "MQTT password (optional)")
  .parse();

const opts = program.opts<{
  mqttBrokerUrl?: string;
  mqttTopic?: string;
  pathPrefixes: string[];
  debounceSeconds?: number;
  onChangeCommand?: string;
  mqttUsername?: string;
  mqttPassword?: string;
}>();

// ---------------------------------------------------------------------------
// Config — CLI args override env vars
// ---------------------------------------------------------------------------

interface SubscriberConfig {
  mqttBrokerUrl: string;
  mqttTopic: string;
  pathPrefixes: string[];
  debounceSeconds: number;
  onChangeCommand: string;
  mqttUsername?: string;
  mqttPassword?: string;
}

function loadConfig(): SubscriberConfig {
  const mqttBrokerUrl = opts.mqttBrokerUrl
    ?? process.env["MQTT_BROKER_URL"]?.trim()
    ?? "";

  const onChangeCommand = opts.onChangeCommand
    ?? process.env["ON_CHANGE_COMMAND"]?.trim()
    ?? "";

  if (!mqttBrokerUrl) {
    logError(LABEL, "MQTT_BROKER_URL is required — pass --mqtt-broker-url or set MQTT_BROKER_URL in env");
    process.exit(1);
  }

  if (!onChangeCommand) {
    logError(LABEL, "ON_CHANGE_COMMAND is required — pass --on-change-command or set ON_CHANGE_COMMAND in env");
    process.exit(1);
  }

  const mqttTopic = opts.mqttTopic
    ?? process.env["MQTT_TOPIC"]?.trim()
    ?? "file-watcher/change";

  const pathPrefixes = opts.pathPrefixes.length > 0
    ? opts.pathPrefixes
    : (process.env["PATH_PREFIXES"] ?? "").split(",").map((p) => p.trim()).filter(Boolean);

  const debounceSeconds = opts.debounceSeconds
    ?? Number(process.env["DEBOUNCE_SECONDS"] ?? "30");

  const mqttUsername = opts.mqttUsername ?? process.env["MQTT_USERNAME"]?.trim() ?? undefined;
  const mqttPassword = opts.mqttPassword ?? process.env["MQTT_PASSWORD"] ?? undefined;

  return {
    mqttBrokerUrl,
    mqttTopic,
    pathPrefixes,
    debounceSeconds,
    onChangeCommand,
    mqttUsername,
    mqttPassword,
  };
}

// ---------------------------------------------------------------------------
// Path matching
// ---------------------------------------------------------------------------

function matchesPrefix(filePath: string, prefixes: string[]): boolean {
  if (prefixes.length === 0) return true;
  return prefixes.some((prefix) => filePath.startsWith(prefix));
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
  const config = loadConfig();

  log(LABEL, "Starting with config:");
  log(LABEL, `  MQTT_BROKER_URL:   ${config.mqttBrokerUrl}`);
  log(LABEL, `  MQTT_TOPIC:        ${config.mqttTopic}`);
  log(LABEL, `  PATH_PREFIXES:     ${config.pathPrefixes.join(", ") || "(all)"}`);
  log(LABEL, `  DEBOUNCE_SECONDS:  ${config.debounceSeconds}`);
  log(LABEL, `  ON_CHANGE_COMMAND: ${config.onChangeCommand}`);
  log(LABEL, `  MQTT_USERNAME:     ${config.mqttUsername ?? "(none)"}`);

  // Debounce state
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
      log(LABEL, `Debounce expired after ${count} event(s) — executing command`);
      log(LABEL, `Changed paths:\n${pathsList}`);
      exec(config.onChangeCommand, {
        env: { ...process.env, CHANGED_PATHS: pathsList },
      }, (error, stdout, stderr) => {
        if (error) {
          logError(LABEL, `Command failed (exit ${error.code}): ${error.message}`);
          if (stderr) logError(LABEL, `stderr: ${stderr.trim()}`);
        } else {
          if (stdout.trim()) log(LABEL, `Command stdout: ${stdout.trim()}`);
          log(LABEL, "Command completed successfully");
        }
      });
    }, config.debounceSeconds * 1000);
  }

  // Connect to MQTT broker (auto-reconnects every reconnectPeriod ms on failure)
  const client: MqttClient = await mqtt.connectAsync(config.mqttBrokerUrl, {
    username: config.mqttUsername,
    password: config.mqttPassword,
    reconnectPeriod: 2000,
  });
  log(LABEL, "Connected to MQTT broker");

  async function subscribe(): Promise<void> {
    try {
      await client.subscribeAsync(config.mqttTopic, { qos: 1 });
      log(LABEL, `Subscribed to topic: ${config.mqttTopic}`);
    } catch (err) {
      logError(LABEL, `Failed to subscribe: ${err instanceof Error ? err.message : String(err)}`);
    }
  }

  await subscribe();

  // Re-subscribe on every reconnect (default `clean: true` clears server-side state)
  client.on("connect", () => {
    log(LABEL, "Reconnected to MQTT broker");
    void subscribe();
  });

  client.on("offline", () => log(LABEL, "Offline — broker unreachable, will retry"));
  client.on("error", (err) => {
    // ECONNREFUSED is expected when broker is restarting; suppress the spam
    if ((err as NodeJS.ErrnoException).code !== "ECONNREFUSED") {
      logError(LABEL, `MQTT error: ${err.message}`);
    }
  });

  // Handle incoming messages
  client.on("message", (_topic, payload) => {
    let parsed: MqttChangePayload;
    try {
      parsed = JSON.parse(payload.toString()) as MqttChangePayload;
    } catch {
      logError(LABEL, `Malformed message, skipping: ${payload.toString().slice(0, 200)}`);
      return;
    }

    if (!parsed.path || !parsed.event) {
      logError(LABEL, `Message missing required fields, skipping`);
      return;
    }

    if (!matchesPrefix(parsed.path, config.pathPrefixes)) return;

    pendingEventCount++;
    changedPaths.add(parsed.path);
    log(LABEL, `${parsed.event}: ${parsed.path} (${pendingEventCount} pending, debounce reset)`);
    scheduleCommand();
  });

  // Graceful shutdown
  function shutdown(signal: string): void {
    log(LABEL, `Received ${signal} — shutting down`);
    if (debounceTimer !== null) {
      clearTimeout(debounceTimer);
      debounceTimer = null;
      log(LABEL, `Cleared pending debounce timer (${pendingEventCount} event(s), ${changedPaths.size} path(s) will NOT trigger command)`);
    }
    client.end(true, () => {
      log(LABEL, "MQTT client disconnected — exiting");
      process.exit(0);
    });
  }

  process.on("SIGINT", () => shutdown("SIGINT"));
  process.on("SIGTERM", () => shutdown("SIGTERM"));
}

main().catch((err) => {
  logError(LABEL, `Fatal: ${err instanceof Error ? err.message : String(err)}`);
  process.exit(1);
});
