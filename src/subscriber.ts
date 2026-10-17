import "dotenv/config";
import { exec } from "node:child_process";
import { program } from "commander";
import mqtt, { type MqttClient } from "mqtt";
import { commandEnv, Debouncer, matchesPrefix, parseMessage, summarise } from "./batch.js";
import { loadSubscriberConfig, type SubscriberOpts } from "./config.js";
import { configureFileLogger, log, logError, parseCommaSeparated } from "./shared.js";

const LABEL = "subscriber";

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

program
  .name("file-watcher-subscriber")
  .description("Subscribe to MQTT file-change events, debounce, and run a command")
  .option("-b, --mqtt-broker-url <url>", "MQTT broker URL (e.g. mqtt://broker.example.com:1883)")
  .option("-t, --mqtt-topic <topic>", "MQTT topic to subscribe to")
  .option("-f, --path-prefixes <paths>", "comma-separated path prefixes to filter on", parseCommaSeparated, [])
  .option("-d, --debounce-seconds <n>", "quiet period before firing command", parseFloat)
  .option("-c, --on-change-command <cmd>", "shell command to execute on change")
  .option("-u, --mqtt-username <user>", "MQTT username (optional)")
  .option("--mqtt-password <pass>", "MQTT password (optional)")
  .option("--log-file <path>", "also append logs to this file (parent dir auto-created)")
  .parse();

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
  const loaded = loadSubscriberConfig(program.opts<SubscriberOpts>(), process.env);
  if (!loaded.ok) {
    logError(LABEL, loaded.error);
    process.exit(1);
  }
  const config = loaded.config;
  configureFileLogger(config.logFile, LABEL);

  log(LABEL, "Starting with config:");
  log(LABEL, `  MQTT_BROKER_URL:   ${config.mqttBrokerUrl}`);
  log(LABEL, `  MQTT_TOPIC:        ${config.mqttTopic}`);
  log(LABEL, `  PATH_PREFIXES:     ${config.pathPrefixes.join(", ") || "(all)"}`);
  log(LABEL, `  DEBOUNCE_SECONDS:  ${config.debounceSeconds}`);
  log(LABEL, `  ON_CHANGE_COMMAND: ${config.onChangeCommand}`);
  log(LABEL, `  MQTT_USERNAME:     ${config.mqttUsername ?? "(none)"}`);
  log(LABEL, `  LOG_FILE:          ${config.logFile ?? "(disabled)"}`);

  const debouncer = new Debouncer(config.debounceSeconds * 1000, (batch) => {
    log(LABEL, `Debounce expired: ${batch.events.length} event(s) across ${batch.paths.length} path(s) `
      + `(${summarise(batch)}) — executing command`);
    log(LABEL, `Changed paths:\n${batch.paths.join("\n")}`);
    exec(config.onChangeCommand, { env: { ...process.env, ...commandEnv(batch) } }, (error, stdout, stderr) => {
      if (error) {
        logError(LABEL, `Command failed (exit ${error.code}): ${error.message}`);
        if (stderr) logError(LABEL, `stderr: ${stderr.trim()}`);
      } else {
        if (stdout.trim()) log(LABEL, `Command stdout: ${stdout.trim()}`);
        log(LABEL, "Command completed successfully");
      }
    });
  });

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

  client.on("message", (_topic, payload) => {
    const parsed = parseMessage(payload.toString());
    if (!parsed.ok) {
      logError(LABEL, parsed.error);
      return;
    }
    if (!matchesPrefix(parsed.payload.path, config.pathPrefixes)) return;
    const pending = debouncer.add(parsed.payload);
    log(LABEL, `${parsed.payload.event}: ${parsed.payload.path} (${pending} pending, debounce reset)`);
  });

  function shutdown(signal: string): void {
    log(LABEL, `Received ${signal} — shutting down`);
    const dropped = debouncer.cancel();
    if (dropped.events > 0) {
      log(LABEL, `Cleared pending debounce timer (${dropped.events} event(s), ${dropped.paths} path(s) `
        + "will NOT trigger command)");
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
