import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { mkdtempSync, mkdirSync, rmSync, utimesSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { createPipeline, globToBasenameRegex, matchesExtension, type Pipeline, type PipelineConfig } from "./pipeline.js";
import type { MqttChangePayload } from "./shared.js";
import { waitFor } from "./testing.js";

let dir: string;
let sent: MqttChangePayload[];
let pipeline: Pipeline | undefined;

function make(overrides: Partial<PipelineConfig> = {}): Pipeline {
  pipeline = createPipeline({
    watchPaths: [path.join(dir, "lib"), path.join(dir, "rb", "master.db")],
    watchExtensions: new Set(["aiff", "mp3"]),
    stabilityThresholdSeconds: 0.05,
    stabilityMode: "add-only",
    sweepFolders: [],
    sweepIntervalSeconds: 0,
    catchUpWindowSeconds: 600,
    catchUpIntervalSeconds: 0,
    ...overrides,
  }, { publish: (p) => sent.push(p), log: () => {} });
  return pipeline;
}
const write = (rel: string, body = "x"): string => {
  const p = path.join(dir, rel);
  mkdirSync(path.dirname(p), { recursive: true });
  writeFileSync(p, body);
  return p;
};
const sentAs = () => sent.map((s) => `${s.event} ${path.relative(dir, s.path)}`);

beforeEach(() => {
  dir = mkdtempSync(path.join(tmpdir(), "fw-pipeline-"));
  sent = [];
});
afterEach(() => {
  pipeline?.stop();
  pipeline = undefined;
  rmSync(dir, { recursive: true, force: true });
});

describe("the name filters", () => {
  it("publishes a named file whatever its extension — master.db is not audio", () => {
    const p = make();
    p.handleEvent("change", write("rb/master.db"));
    expect(sentAs()).toEqual(["change rb/master.db"]);
  });

  it("drops a file in a watched folder whose extension is not listed", () => {
    const p = make();
    p.handleEvent("add", write("lib/cover.jpg"));
    expect(sent).toEqual([]);
  });

  it("matches extensions without regard to case", () => {
    expect(matchesExtension("/a/B.AIFF", new Set(["aiff"]))).toBe(true);
    expect(matchesExtension("/a/b", new Set())).toBe(true);
  });

  it("turns an ignore glob into a whole-basename match", () => {
    const r = globToBasenameRegex("._*");
    expect(r.test("._track.aiff")).toBe(true);
    expect(r.test("a._track.aiff")).toBe(false);
    expect(globToBasenameRegex("a?.mp3").test("ab.mp3")).toBe(true);
  });
});

describe("arrivals", () => {
  it("publishes an inotify arrival at once — CLOSE_WRITE already proved the write finished", () => {
    const p = make();
    p.handleEvent("add", write("lib/a.aiff"));
    expect(sentAs()).toEqual(["add lib/a.aiff"]);
  });

  it("holds an arrival that came with stats until it has stopped changing", async () => {
    const p = make();
    const f = write("lib/a.aiff");
    p.handleEvent("add", f, { size: 1, mtimeMs: 1 });   // not what is on disk
    await new Promise((r) => setTimeout(r, 70));
    expect(sent).toEqual([]);                             // it changed, so wait again
    await waitFor(() => sent.length === 1);
    expect(sentAs()).toEqual(["add lib/a.aiff"]);
  });

  it("drops the removal of a file nobody downstream heard of", () => {
    const p = make();
    const f = write("lib/a.aiff");
    p.handleEvent("add", f, { size: 1, mtimeMs: 1 });
    p.handleEvent("unlink", f);
    expect(sent).toEqual([]);
  });

  it("publishes the removal of a re-announced file that has been in the library all along", () => {
    const p = make();
    const f = write("lib/a.aiff");
    p.handleEvent("add", f, { size: 1, mtimeMs: 1 }, "catchup");
    p.handleEvent("unlink", f);
    expect(sentAs()).toEqual(["unlink lib/a.aiff"]);
  });
});

describe("a catch-up and the kernel seeing the same arrival", () => {
  it("publishes at once when the kernel's event comes after the catch-up parked the file", () => {
    // The race the end-to-end suite found: a file lands as the startup
    // catch-up runs, the catch-up sees it first and holds it for the
    // stability wait, and the kernel's CLOSE_WRITE used to be dropped.
    const p = make({ stabilityThresholdSeconds: 60 });
    const f = write("rb/master.db");
    expect(p.catchUp("startup")).toBe(1);
    expect(sent).toEqual([]);
    p.handleKernelEvent("add", f);
    expect(sentAs()).toEqual(["add rb/master.db"]);
  });

  it("publishes once, not again when the stability wait would have ended", async () => {
    const p = make({ stabilityThresholdSeconds: 0.05 });
    const f = write("rb/master.db");
    p.catchUp("startup");
    p.handleKernelEvent("change", f);
    await new Promise((r) => setTimeout(r, 120));
    expect(sentAs()).toEqual(["add rb/master.db"]);
  });

  it("passes an ordinary kernel event straight through", () => {
    const p = make();
    p.handleKernelEvent("add", write("lib/a.aiff"));
    expect(sentAs()).toEqual(["add lib/a.aiff"]);
  });
});

describe("changes must be real", () => {
  it("does not announce a close that wrote nothing", () => {
    const p = make();
    const f = write("rb/master.db");
    p.handleEvent("change", f);
    p.handleEvent("change", f);
    expect(sent).toHaveLength(1);
  });

  it("announces the next close that did write", () => {
    const p = make();
    const f = write("rb/master.db");
    p.handleEvent("change", f);
    writeFileSync(f, "longer");
    p.handleEvent("change", f);
    expect(sent).toHaveLength(2);
  });

  it("says nothing about a change to a file that is already gone", () => {
    const p = make();
    p.handleEvent("change", path.join(dir, "rb", "master.db"));
    expect(sent).toEqual([]);
  });
});

describe("catch-up — what changed while nobody was listening", () => {
  it("announces recent files in folders and named files, and nothing else", async () => {
    write("lib/a.aiff");
    write("lib/sub/b.mp3");
    write("lib/notes.txt");
    write("lib/.hidden.aiff");
    write("rb/master.db");
    write("rb/master.db-shm");
    const p = make();
    expect(p.catchUp("test")).toBe(3);
    await waitFor(() => sent.length === 3);
    expect(sentAs().sort()).toEqual(["add lib/a.aiff", "add lib/sub/b.mp3", "add rb/master.db"]);
  });

  it("skips what the live path already announced", async () => {
    const p = make();
    p.handleEvent("change", write("rb/master.db"));
    expect(p.catchUp("test")).toBe(0);
  });

  it("skips files older than the window", () => {
    const f = write("lib/old.aiff");
    const old = (Date.now() - 3600_000) / 1000;
    utimesSync(f, old, old);
    expect(make().catchUp("test")).toBe(0);
  });

  it("can be pointed at one path — the one that just appeared", async () => {
    write("lib/a.aiff");
    write("rb/master.db");
    const p = make();
    expect(p.catchUp("appeared", [path.join(dir, "rb", "master.db")])).toBe(1);
  });

  it("is quiet about a watch path that does not exist", () => {
    expect(make({ watchPaths: [path.join(dir, "nope")] }).catchUp("test")).toBe(0);
  });
});
