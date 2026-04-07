import { describe, it, expect } from "vitest";
import { parseCommaSeparated, type MqttChangePayload } from "./shared.js";

describe("parseCommaSeparated", () => {
  it("splits a comma-separated string and trims whitespace", () => {
    const result = parseCommaSeparated("foo, bar , baz", []);
    expect(result).toEqual(["foo", "bar", "baz"]);
  });

  it("appends to the previous array", () => {
    const result = parseCommaSeparated("c,d", ["a", "b"]);
    expect(result).toEqual(["a", "b", "c", "d"]);
  });

  it("filters out empty strings", () => {
    const result = parseCommaSeparated("a,,b, ,c", []);
    expect(result).toEqual(["a", "b", "c"]);
  });

  it("handles a single value", () => {
    const result = parseCommaSeparated("solo", []);
    expect(result).toEqual(["solo"]);
  });
});

describe("MqttChangePayload", () => {
  it("satisfies the interface shape", () => {
    const payload: MqttChangePayload = {
      event: "add",
      path: "/mnt/music/track.flac",
      timestamp: new Date().toISOString(),
    };
    expect(payload.event).toBe("add");
    expect(payload.path).toBe("/mnt/music/track.flac");
    expect(typeof payload.timestamp).toBe("string");
  });
});
