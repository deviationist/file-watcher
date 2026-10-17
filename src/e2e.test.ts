/// The whole chain, as deployed: the real publisher and subscriber binaries as
/// child processes, a real MQTT broker (aedes, in this process), and a command
/// that writes down what it was given.
///
/// This is the level the August regression lived at. The publisher logged
/// healthy, the subscriber logged healthy, the broker was up -- and a write to
/// master.db reached nobody. Only a test that writes the file and waits for the
/// command could have said so.

import { describe, it, expect, beforeAll, afterAll, beforeEach, afterEach } from "vitest";
import { spawn, execFileSync, type ChildProcess } from "node:child_process";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, renameSync, rmSync, utimesSync, writeFileSync } from "node:fs";
import { createServer, type Server, type AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { Aedes } from "aedes";
import { settle, waitFor } from "./testing.js";

const REPO = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const TSX = path.join(REPO, "node_modules", ".bin", "tsx");

const hasInotifywait = (() => {
  try { execFileSync("inotifywait", ["--help"], { stdio: "ignore" }); return true; }
  catch (e) { return (e as { status?: number }).status !== undefined; }
})();

let broker: Aedes;
let server: Server;
let brokerUrl: string;

beforeAll(async () => {
  broker = await Aedes.createBroker();
  server = createServer(broker.handle);
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  brokerUrl = `mqtt://127.0.0.1:${(server.address() as AddressInfo).port}`;
});
afterAll(async () => {
  await new Promise<void>((resolve) => broker.close(() => resolve()));
  await new Promise<void>((resolve) => server.close(() => resolve()));
});

interface Proc { child: ChildProcess; out: () => string; }
let root: string;
let procs: Proc[];

/// Run a binary from src/ the way its unit does, but with only the environment
/// given here: the working directory is the temp folder, so dotenv finds no
/// .env, and nothing of the host's own configuration leaks in.
function run(bin: "publisher" | "subscriber", args: string[], env: Record<string, string> = {}): Proc {
  let output = "";
  const child = spawn(TSX, [path.join(REPO, "src", `${bin}.ts`), ...args], {
    cwd: root,
    env: { PATH: process.env["PATH"]!, HOME: root, MQTT_BROKER_URL: brokerUrl, MQTT_TOPIC: `t/${path.basename(root)}`, ...env },
    stdio: ["ignore", "pipe", "pipe"],
  });
  child.stdout!.on("data", (c: Buffer) => { output += c.toString(); });
  child.stderr!.on("data", (c: Buffer) => { output += c.toString(); });
  const proc = { child, out: () => output };
  procs.push(proc);
  return proc;
}

const p = (rel: string): string => path.join(root, rel);
const write = (rel: string, body = "x"): void => {
  mkdirSync(path.dirname(p(rel)), { recursive: true });
  writeFileSync(p(rel), body);
};
const runs = (): Array<Array<{ event: string; path: string }>> =>
  existsSync(p("runs.jsonl"))
    ? readFileSync(p("runs.jsonl"), "utf8").trim().split("\n").filter(Boolean).map((l) => JSON.parse(l))
    : [];
const rel = (run: Array<{ event: string; path: string }>): string[] =>
  run.map((e) => `${e.event} ${path.relative(root, e.path)}`);

/// The rekordbox unit, with a short debounce and a command that records what
/// it was handed instead of running rekordbox2plex.
async function rekordboxSubscriber(): Promise<Proc> {
  const sub = run("subscriber", [
    "--path-prefixes", p("rb"),
    "--debounce-seconds", "0.3",
    "--on-change-command", `printf '%s\\n' "$CHANGED_EVENTS" >> '${p("runs.jsonl")}'`,
  ]);
  await waitFor(() => sub.out().includes("Subscribed to topic"), 8000, "subscriber to subscribe");
  return sub;
}

beforeEach(() => {
  root = mkdtempSync(path.join(tmpdir(), "fw-e2e-"));
  procs = [];
});
afterEach(async (ctx) => {
  // A timeout here says only that the command never ran. The two processes'
  // own logs say where the event stopped, so print them when it matters.
  if (ctx.task.result?.state === "fail") {
    for (const { child, out } of procs) {
      console.error(`---- ${child.spawnargs.slice(1, 2).join(" ")} (pid ${child.pid})\n${out()}`);
    }
  }
  for (const { child } of procs) {
    if (child.exitCode === null) {
      const exited = new Promise((r) => child.once("exit", r));
      child.kill("SIGTERM");
      await exited;
    }
  }
  rmSync(root, { recursive: true, force: true });
});

// Each test starts two Node processes through tsx; 5 s is not enough on a busy host.
describe.skipIf(!hasInotifywait)("publisher → broker → subscriber → command, on inotify", { timeout: 20_000 }, () => {
  beforeEach(() => {
    write("rb/master.db", "v1");
    // An hour old, as master.db is when the publisher starts on a real day.
    // A file written in the catch-up window before startup is re-announced and
    // held for the stability wait (60 s by default), and a live event for it
    // during that wait folds into the one pending announcement -- correct, but
    // it would make every test here about that wait instead.
    const hourAgo = (Date.now() - 3600_000) / 1000;
    utimesSync(p("rb/master.db"), hourAgo, hourAgo);
    mkdirSync(p("lib/House"), { recursive: true });
  });

  async function publisher(env: Record<string, string> = {}): Promise<Proc> {
    const pub = run("publisher", [], {
      EVENT_SOURCE: "inotify",
      WATCH_PATHS: [p("lib"), p("rb/master.db"), p("rb/master.db-wal")].join(","),
      WATCH_EXTENSIONS: "aiff,mp3",
      ...env,
    });
    await waitFor(() => pub.out().includes("inotify: "), 8000, "publisher watches");
    return pub;
  }

  it("runs the rekordbox command once when Resilio replaces master.db, and hands it only master.db", async () => {
    await rekordboxSubscriber();
    const pub = await publisher();
    expect(pub.out()).toContain(`${p("rb/master.db")} (file)`);
    expect(pub.out()).toContain(`${p("rb/master.db-wal")} (file, not there yet)`);

    // A Rekordbox session's worth of noise beside it, then the real change.
    write("rb/master.db-shm");
    write("rb/networkAnalyze6.db");
    write("rb/master.backup.20261017.db");
    writeFileSync(p("rb/master.db.!sync"), "v2");
    renameSync(p("rb/master.db.!sync"), p("rb/master.db"));

    await waitFor(() => runs().length === 1, 5000, "the command to run");
    await settle(600);    // two debounce periods: no second run trails in
    expect(runs().map(rel)).toEqual([["add rb/master.db"]]);
  });

  it("runs it when only master.db's mtime is bumped — what rbnudge does", async () => {
    await rekordboxSubscriber();
    await publisher();
    const now = new Date();
    utimesSync(p("rb/master.db"), now, now);
    await waitFor(() => runs().length === 1, 5000, "the command to run");
    expect(runs().map(rel)).toEqual([["change rb/master.db"]]);
  });

  it("runs it for a WAL that appears while Rekordbox is open", async () => {
    await rekordboxSubscriber();
    await publisher();
    write("rb/master.db-wal", "frames");
    await waitFor(() => runs().length === 1, 5000, "the command to run");
    expect(runs().map(rel)).toEqual([["add rb/master.db-wal"]]);
  });

  it("does not run it for a track landing in the library — other subscribers own those", async () => {
    await rekordboxSubscriber();
    const pub = await publisher();
    write("lib/House/a.aiff");
    await waitFor(() => pub.out().includes("add: "), 5000, "the publisher to announce the track");
    await settle(600);
    expect(runs()).toEqual([]);
  });

  it("still reads the old WATCH_FOLDERS name, and says it is deprecated", async () => {
    await rekordboxSubscriber();
    const pub = await publisher({ WATCH_PATHS: "", WATCH_FOLDERS: [p("lib"), p("rb/master.db")].join(",") });
    expect(pub.out()).toMatch(/WATCH_FOLDERS is deprecated/);
    writeFileSync(p("rb/master.db"), "v2");
    await waitFor(() => runs().length === 1, 5000, "the command to run");
  });

  it("picks up master.db when the rekordbox folder only appears after startup", async () => {
    rmSync(p("rb"), { recursive: true });
    await rekordboxSubscriber();
    const pub = await publisher();
    expect(pub.out()).toContain(`${p("rb/master.db")} (waiting for ${p("rb")})`);
    mkdirSync(p("rb"));
    await waitFor(() => pub.out().includes(`${p("rb/master.db")} (file`), 5000, "the plan to change");
    await settle(200);
    write("rb/master.db", "first");
    await waitFor(() => runs().length === 1, 5000, "the command to run");
  });

  it("announces at startup a master.db written while the publisher was down, after the stability wait", async () => {
    await rekordboxSubscriber();
    writeFileSync(p("rb/master.db"), "written during a restart");
    const pub = await publisher({ STABILITY_THRESHOLD_SECONDS: "0.2" });
    await waitFor(() => runs().length === 1, 5000, "the command to run");
    expect(pub.out()).toMatch(/catch-up \(startup\): re-announced 1 file/);
    expect(runs().map(rel)).toEqual([["add rb/master.db"]]);
  });

  it("refuses to start on a relative path, and says which", async () => {
    const pub = run("publisher", [], { EVENT_SOURCE: "inotify", WATCH_PATHS: "rb/master.db" });
    const code = await new Promise((r) => pub.child.once("exit", r));
    expect(code).toBe(1);
    expect(pub.out()).toContain("watch path must be absolute: 'rb/master.db'");
  });

  it("leaves no inotifywait behind when the unit is stopped", async () => {
    const pub = await publisher();
    const exited = new Promise((r) => pub.child.once("exit", r));
    pub.child.kill("SIGTERM");
    await exited;
    await settle(200);
    let left = "";
    try { left = execFileSync("pgrep", ["-f", `inotifywait.*${root}`]).toString(); } catch { /* none */ }
    expect(left).toBe("");
  });
});

describe("publisher → broker → subscriber → command, on chokidar polling", { timeout: 20_000 }, () => {
  // Polling is the mode for mounts that emit no inotify events (CIFS). It
  // takes paths as given -- files included -- and never needed the parent
  // folder treatment; this pins that it still delivers a named file.
  it("delivers a change to a named file", async () => {
    write("rb/master.db", "v1");
    await rekordboxSubscriber();
    const pub = run("publisher", [], {
      WATCH_PATHS: p("rb/master.db"),
      POLL_INTERVAL_SECONDS: "0.1",
    });
    await waitFor(() => pub.out().includes("Watcher is ready"), 8000, "chokidar to be ready");
    writeFileSync(p("rb/master.db"), "v2 longer");
    await waitFor(() => runs().length === 1, 5000, "the command to run");
    expect(runs().map(rel)).toEqual([["change rb/master.db"]]);
  });
});
