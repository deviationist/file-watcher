import { describe, it, expect, vi, afterEach } from "vitest";
import { commandEnv, Debouncer, matchesPrefix, parseMessage, summarise, type Batch } from "./batch.js";
import type { MqttChangePayload } from "./shared.js";

const ev = (event: string, path: string): MqttChangePayload => ({ event, path, timestamp: "t" });

describe("matchesPrefix — which messages a subscriber takes", () => {
  it("takes a path under one of its prefixes", () => {
    expect(matchesPrefix("/mnt/resilio/data/rekordbox/master.db", ["/mnt/music/main", "/mnt/resilio/data/rekordbox"])).toBe(true);
  });

  it("refuses a path under none", () => {
    expect(matchesPrefix("/mnt/music/on-hold/Ingress/a.mp3", ["/mnt/music/main"])).toBe(false);
  });

  it("takes everything when it has no prefixes", () => {
    expect(matchesPrefix("/anything", [])).toBe(true);
  });

  // Pinned, not endorsed: a prefix is a string prefix, so /mnt/music/main also
  // takes /mnt/music/mainline. No live prefix has a sibling like that today.
  it("matches by string, not by path segment", () => {
    expect(matchesPrefix("/mnt/music/mainline/a.mp3", ["/mnt/music/main"])).toBe(true);
  });
});

describe("parseMessage", () => {
  it("reads a well-formed payload", () => {
    expect(parseMessage(JSON.stringify(ev("add", "/a")))).toEqual({ ok: true, payload: ev("add", "/a") });
  });

  it("refuses something that is not JSON, quoting a bounded piece of it", () => {
    const r = parseMessage("x".repeat(500));
    expect(r.ok).toBe(false);
    expect(!r.ok && r.error.length).toBeLessThan(260);
  });

  it("refuses a payload without a path or an event", () => {
    expect(parseMessage(JSON.stringify({ event: "add" })).ok).toBe(false);
    expect(parseMessage(JSON.stringify({ path: "/a" })).ok).toBe(false);
    expect(parseMessage("null").ok).toBe(false);
  });
});

describe("Debouncer — one command run per burst", () => {
  afterEach(() => vi.useRealTimers());

  it("runs once, after the quiet period, with every event in the burst", () => {
    vi.useFakeTimers();
    const fired: Batch[] = [];
    const d = new Debouncer(30_000, (b) => fired.push(b));
    d.add(ev("change", "/rb/master.db"));
    vi.advanceTimersByTime(20_000);
    d.add(ev("add", "/rb/master.db-wal"));
    vi.advanceTimersByTime(29_999);
    expect(fired).toHaveLength(0);
    vi.advanceTimersByTime(1);
    expect(fired).toHaveLength(1);
    expect(fired[0]!.paths).toEqual(["/rb/master.db", "/rb/master.db-wal"]);
  });

  it("dedupes paths but keeps every event, in order", () => {
    vi.useFakeTimers();
    const fired: Batch[] = [];
    const d = new Debouncer(10, (b) => fired.push(b));
    d.add(ev("unlink", "/a"));
    d.add(ev("add", "/a"));
    d.add(ev("change", "/b"));
    vi.advanceTimersByTime(10);
    expect(fired[0]!.paths).toEqual(["/a", "/b"]);
    expect(fired[0]!.events.map((e) => e.event)).toEqual(["unlink", "add", "change"]);
  });

  it("starts the next burst empty", () => {
    vi.useFakeTimers();
    const fired: Batch[] = [];
    const d = new Debouncer(10, (b) => fired.push(b));
    d.add(ev("add", "/a"));
    vi.advanceTimersByTime(10);
    d.add(ev("add", "/c"));
    vi.advanceTimersByTime(10);
    expect(fired.map((b) => b.paths)).toEqual([["/a"], ["/c"]]);
  });

  it("runs nothing for what was cancelled, and says how much that was", () => {
    vi.useFakeTimers();
    const fired: Batch[] = [];
    const d = new Debouncer(10, (b) => fired.push(b));
    d.add(ev("add", "/a"));
    d.add(ev("add", "/a"));
    expect(d.cancel()).toEqual({ events: 2, paths: 1 });
    vi.advanceTimersByTime(100);
    expect(fired).toHaveLength(0);
  });
});

describe("commandEnv — the contract with ON_CHANGE_COMMAND", () => {
  it("gives newline-separated paths and the events as JSON", () => {
    const batch = { events: [ev("add", "/a"), ev("unlink", "/b")], paths: ["/a", "/b"] };
    const env = commandEnv(batch);
    expect(env.CHANGED_PATHS).toBe("/a\n/b");
    expect(JSON.parse(env.CHANGED_EVENTS)).toEqual(batch.events);
    expect(summarise(batch)).toBe("1 add, 1 unlink");
  });
});
