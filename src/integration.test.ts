/// Against the *live* broker, on request only.
///
/// This used to run on every `npm test` wherever a .env with a broker URL sat
/// in the working directory -- which on xavi meant every test run connected to
/// production with production credentials. The chain itself is now tested
/// against a broker the test owns (e2e.test.ts); this remains as a smoke test
/// of the real broker's reachability and auth, run deliberately:
///
///   FILE_WATCHER_LIVE_BROKER=1 npm test -- src/integration.test.ts

import { describe, it, expect, beforeAll, afterAll } from "vitest";
import mqtt, { type MqttClient } from "mqtt";
import { config as loadDotenv } from "dotenv";
import type { MqttChangePayload } from "./shared.js";

const LIVE = process.env["FILE_WATCHER_LIVE_BROKER"] === "1";
if (LIVE) loadDotenv();
const BROKER_URL = process.env["MQTT_BROKER_URL"];
const TOPIC = "file-watcher/test";

describe.skipIf(!LIVE || !BROKER_URL)("the live MQTT broker", () => {
  let publisher: MqttClient;
  let subscriber: MqttClient;

  beforeAll(async () => {
    const auth = { username: process.env["MQTT_USERNAME"], password: process.env["MQTT_PASSWORD"] };
    publisher = await mqtt.connectAsync(BROKER_URL!, auth);
    subscriber = await mqtt.connectAsync(BROKER_URL!, auth);
    await subscriber.subscribeAsync(TOPIC, { qos: 1 });
  });

  afterAll(async () => {
    await publisher?.endAsync();
    await subscriber?.endAsync();
  });

  it("carries a change payload from one client to another, on a topic no subscriber acts on", async () => {
    const payload: MqttChangePayload = {
      event: "add",
      path: "/mnt/music/Artist/track.flac",
      timestamp: new Date().toISOString(),
    };
    const received = new Promise<MqttChangePayload>((resolve) => {
      subscriber.once("message", (_topic, msg) => resolve(JSON.parse(msg.toString())));
    });
    await publisher.publishAsync(TOPIC, JSON.stringify(payload), { qos: 1 });
    expect(await received).toEqual(payload);
  });
});
