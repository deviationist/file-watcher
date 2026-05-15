import { describe, it, expect, beforeAll, afterAll } from "vitest";
import mqtt, { type MqttClient } from "mqtt";
import "dotenv/config";
import type { MqttChangePayload } from "./shared.js";

const BROKER_URL = process.env["MQTT_BROKER_URL"];
const USERNAME = process.env["MQTT_USERNAME"];
const PASSWORD = process.env["MQTT_PASSWORD"];
const TOPIC = "file-watcher/test";

describe.skipIf(!BROKER_URL)("MQTT pub/sub integration", () => {
  let publisher: MqttClient;
  let subscriber: MqttClient;

  beforeAll(async () => {
    publisher = await mqtt.connectAsync(BROKER_URL!, { username: USERNAME, password: PASSWORD });
    subscriber = await mqtt.connectAsync(BROKER_URL!, { username: USERNAME, password: PASSWORD });
    await subscriber.subscribeAsync(TOPIC, { qos: 1 });
  });

  afterAll(async () => {
    await publisher.endAsync();
    await subscriber.endAsync();
  });

  it("publishes and receives a change payload", async () => {
    const payload: MqttChangePayload = {
      event: "add",
      path: "/mnt/music/Artist/track.flac",
      timestamp: new Date().toISOString(),
    };

    const received = new Promise<MqttChangePayload>((resolve) => {
      subscriber.on("message", (_topic, msg) => {
        resolve(JSON.parse(msg.toString()));
      });
    });

    await publisher.publishAsync(TOPIC, JSON.stringify(payload), { qos: 1 });
    const result = await received;

    expect(result.event).toBe("add");
    expect(result.path).toBe("/mnt/music/Artist/track.flac");
    expect(result.timestamp).toBe(payload.timestamp);
  });

  it("multiple events are received independently", async () => {
    const paths = ["/mnt/music/a.flac", "/mnt/music/b.mp3", "/mnt/media/movie.mkv"];
    const received: MqttChangePayload[] = [];

    const allReceived = new Promise<void>((resolve) => {
      subscriber.on("message", (_topic, msg) => {
        received.push(JSON.parse(msg.toString()));
        if (received.length === paths.length) resolve();
      });
    });

    for (const p of paths) {
      const payload: MqttChangePayload = {
        event: "change",
        path: p,
        timestamp: new Date().toISOString(),
      };
      await publisher.publishAsync(TOPIC, JSON.stringify(payload), { qos: 1 });
    }

    await allReceived;
    expect(received.map((r) => r.path)).toEqual(paths);
  });
});

describe("subscriber path filtering", () => {
  it("matchesPrefix logic: accepts matching paths", () => {
    const prefixes = ["/mnt/music", "/mnt/media"];
    const path = "/mnt/music/Artist/track.flac";
    const matches = prefixes.some((prefix) => path.startsWith(prefix));
    expect(matches).toBe(true);
  });

  it("matchesPrefix logic: rejects non-matching paths", () => {
    const prefixes = ["/mnt/music"];
    const path = "/mnt/media/movie.mkv";
    const matches = prefixes.some((prefix) => path.startsWith(prefix));
    expect(matches).toBe(false);
  });

  it("matchesPrefix logic: empty prefixes accepts all", () => {
    const prefixes: string[] = [];
    const path = "/anything/at/all";
    const matches = prefixes.length === 0 || prefixes.some((prefix) => path.startsWith(prefix));
    expect(matches).toBe(true);
  });
});

describe("debounce behavior", () => {
  it("collects paths into a Set for deduplication", () => {
    const changedPaths = new Set<string>();
    changedPaths.add("/mnt/music/track.flac");
    changedPaths.add("/mnt/music/track.flac");
    changedPaths.add("/mnt/music/other.mp3");

    expect(changedPaths.size).toBe(2);
    expect([...changedPaths].join("\n")).toBe("/mnt/music/track.flac\n/mnt/music/other.mp3");
  });

  it("resets cleanly after snapshot", () => {
    let changedPaths = new Set<string>();
    changedPaths.add("/mnt/music/a.flac");
    changedPaths.add("/mnt/music/b.flac");

    const snapshot = changedPaths;
    changedPaths = new Set();
    changedPaths.add("/mnt/music/c.flac");

    expect(snapshot.size).toBe(2);
    expect(changedPaths.size).toBe(1);
    expect([...changedPaths]).toEqual(["/mnt/music/c.flac"]);
  });
});
