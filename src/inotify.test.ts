/// Against the real kernel: a real inotifywait on real temporary folders.
///
/// The bug these exist for was not in any one function -- each piece did what
/// it was written to do, and master.db still went unwatched for two months. So
/// the tests here go from "a file is written" to "an event comes out", through
/// everything in between.

import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { execFileSync } from "node:child_process";
import { mkdirSync, mkdtempSync, renameSync, rmSync, unlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { InotifySource } from "./inotify.js";
import { globToBasenameRegex } from "./pipeline.js";
import { settle, waitFor } from "./testing.js";
import { parseTargets } from "./watchplan.js";

const hasInotifywait = (() => {
  try { execFileSync("inotifywait", ["--help"], { stdio: "ignore" }); return true; }
  catch (e) { return (e as { status?: number }).status !== undefined; }   // --help exits non-zero
})();

let root: string;
let source: InotifySource | undefined;
let events: string[];
let settled: string[];
let logs: string[];
let errors: string[];

const p = (rel: string): string => path.join(root, rel);
const write = (rel: string, body = "x"): void => {
  mkdirSync(path.dirname(p(rel)), { recursive: true });
  writeFileSync(p(rel), body);
};
/// How Resilio, rsync and SQLite's own tools deliver a file: write a partial
/// under another name, then rename it over the real one.
const renameIntoPlace = (rel: string, partial: string): void => {
  writeFileSync(p(partial), "new");
  renameSync(p(partial), p(rel));
};
const has = (e: string): boolean => events.includes(e);

async function start(paths: string[], extra: { ignore?: string[] } = {}): Promise<InotifySource> {
  const { targets, errors: bad } = parseTargets(paths.map((x) => (x.startsWith("/") ? x : p(x)) + (x.endsWith("/") ? "/" : "")));
  expect(bad).toEqual([]);
  source = new InotifySource({
    targets,
    ignoreRegexes: (extra.ignore ?? ["._*"]).map(globToBasenameRegex),
    onEvent: (event, filePath) => events.push(`${event} ${path.relative(root, filePath)}`),
    onSettled: (s) => settled.push(`${s.kind} ${path.relative(root, s.path)}`),
    log: (m) => logs.push(m),
    logError: (m) => errors.push(m),
    recheckMs: 0,
    restartDelayMs: 50,
  });
  await source.start();
  return source;
}

beforeEach(() => {
  root = mkdtempSync(path.join(tmpdir(), "fw-inotify-"));
  events = [];
  settled = [];
  logs = [];
  errors = [];
});
afterEach(async () => {
  await source?.stop();
  source = undefined;
  rmSync(root, { recursive: true, force: true });
});

describe.skipIf(!hasInotifywait)("a named file — the master.db case", () => {
  beforeEach(() => {
    write("rb/master.db", "v1");
    mkdirSync(p("rb/share/PIONEER/USBANLZ"), { recursive: true });
  });

  it("reports master.db replaced by rename, the way Resilio delivers it", async () => {
    await start(["rb/master.db"]);
    renameIntoPlace("rb/master.db", "rb/master.db.!sync");
    await waitFor(() => has("add rb/master.db"));
    // The partial's name never surfaces.
    expect(events.filter((e) => e.includes("!sync"))).toEqual([]);
  });

  it("reports master.db written in place", async () => {
    await start(["rb/master.db"]);
    writeFileSync(p("rb/master.db"), "v2");
    await waitFor(() => has("change rb/master.db"));
  });

  it("keeps reporting after the first replacement — the watch is on the folder, not the old inode", async () => {
    await start(["rb/master.db"]);
    renameIntoPlace("rb/master.db", "rb/master.db.!sync");
    await waitFor(() => events.length === 1);
    writeFileSync(p("rb/master.db"), "v3");
    await waitFor(() => has("change rb/master.db"));
  });

  it("says nothing about the other files in the folder", async () => {
    await start(["rb/master.db"]);
    for (const name of ["master.db-shm", "networkAnalyze6.db", "master.backup.20261017.db", "masterPlaylists6.xml"]) {
      write(`rb/${name}`);
    }
    write("rb/share/PIONEER/USBANLZ/ANLZ0000.DAT");
    writeFileSync(p("rb/master.db"), "v2");    // a marker, so we know the others had their chance
    await waitFor(() => has("change rb/master.db"));
    await settle();
    expect(events).toEqual(["change rb/master.db"]);
  });

  it("does not watch the folder's subtree for the sake of one file", async () => {
    await start(["rb/master.db"]);
    expect(source!.summary()).toBe("0 folder(s) recursively, 1 file(s) via 1 parent folder(s)");
  });

  it("reports a WAL that was not there at startup when it appears, is written and goes away", async () => {
    await start(["rb/master.db", "rb/master.db-wal"]);
    expect(logs).toContain(`watch: ${p("rb/master.db-wal")} (file, not there yet)`);
    write("rb/master.db-wal", "frames");
    await waitFor(() => has("add rb/master.db-wal"));
    writeFileSync(p("rb/master.db-wal"), "more frames");
    await waitFor(() => has("change rb/master.db-wal"));
    unlinkSync(p("rb/master.db-wal"));
    await waitFor(() => has("unlink rb/master.db-wal"));
  });

  it("reports a named file even if it starts with a dot or matches an ignore pattern — naming it is asking", async () => {
    write("rb/._odd.db");
    await start(["rb/._odd.db"]);
    writeFileSync(p("rb/._odd.db"), "y");
    await waitFor(() => has("change rb/._odd.db"));
  });
});

describe.skipIf(!hasInotifywait)("a folder", () => {
  it("reports a new file, an edit and a removal, at any depth", async () => {
    mkdirSync(p("lib/House"), { recursive: true });
    await start(["lib"]);
    write("lib/House/a.aiff");
    await waitFor(() => has("add lib/House/a.aiff"));
    writeFileSync(p("lib/House/a.aiff"), "tagged");
    await waitFor(() => has("change lib/House/a.aiff"));
    unlinkSync(p("lib/House/a.aiff"));
    await waitFor(() => has("unlink lib/House/a.aiff"));
  });

  it("follows a subfolder created after startup", async () => {
    mkdirSync(p("lib"));
    await start(["lib"]);
    mkdirSync(p("lib/New Genre"));
    await settle(100);    // inotifywait adds the watch on the new folder
    write("lib/New Genre/b.mp3");
    await waitFor(() => has("add lib/New Genre/b.mp3"));
  });

  it("drops dotfile partials and ignored names", async () => {
    mkdirSync(p("lib"));
    await start(["lib"]);
    write("lib/.a.aiff.part");
    write("lib/._a.aiff");
    write("lib/a.aiff");
    await waitFor(() => has("add lib/a.aiff"));
    await settle();
    expect(events).toEqual(["add lib/a.aiff"]);
  });

  it("does not report a folder's own creation as a file arriving", async () => {
    mkdirSync(p("lib"));
    await start(["lib"]);
    mkdirSync(p("lib/Empty"));
    await settle();
    expect(events).toEqual([]);
  });
});

describe.skipIf(!hasInotifywait)("paths that do not exist yet", () => {
  it("starts watching a folder that appears after startup, and reconciles what landed first", async () => {
    await start(["later/"]);
    expect(errors).toContain(`watch: ${p("later")} (waiting for ${p("later")})`);
    mkdirSync(p("later"));
    write("later/early.aiff");            // may land before the new watch exists
    await waitFor(() => settled.includes("folder later"), 3000, "folder to settle");
    write("later/late.aiff");
    await waitFor(() => has("add later/late.aiff"));
  });

  it("follows a folder that appears several levels down, one level at a time", async () => {
    await start(["a/b/c/"]);
    mkdirSync(p("a/b/c"), { recursive: true });
    await waitFor(() => settled.includes("folder a/b/c"), 3000, "deep folder to settle");
    write("a/b/c/x.aiff");
    await waitFor(() => has("add a/b/c/x.aiff"));
  });

  it("watches a named file whose folder does not exist yet", async () => {
    await start(["rb/master.db"]);
    mkdirSync(p("rb"));
    await waitFor(() => settled.includes("file rb/master.db"), 3000, "file target to settle");
    write("rb/master.db");
    await waitFor(() => has("add rb/master.db"));
  });

  it("finds an awaited path that appeared without any event, on the periodic recheck", async () => {
    // The race the recheck exists for: the path appears between being probed
    // and its parent's watch being set up, so no event ever says so.
    await start(["late/"]);
    mkdirSync(p("late"));
    await source!.replan("periodic recheck");
    expect(source!.states()[0]!.kind).toBe("folder");
  });
});

describe.skipIf(!hasInotifywait)("a watched folder that goes away", () => {
  it("waits for it again when it is deleted, and resumes when it is back", async () => {
    mkdirSync(p("lib"));
    await start(["lib"]);
    rmSync(p("lib"), { recursive: true });
    await waitFor(() => source!.states()[0]!.kind === "awaiting", 3000, "folder to be awaited");
    mkdirSync(p("lib"));
    // onSettled, not states(): it fires once the new watch is established.
    // A file written before that is the reconcile's to find, by design.
    await waitFor(() => settled.includes("folder lib"), 3000, "folder to be watched again");
    write("lib/back.aiff");
    await waitFor(() => has("add lib/back.aiff"), 3000, "back.aiff");
  });

  it("re-watches a folder that was moved away and replaced by a new one of the same name", async () => {
    mkdirSync(p("lib"));
    await start(["lib"]);
    renameSync(p("lib"), p("lib.old"));
    mkdirSync(p("lib"));
    // The old watch followed the moved folder. Without a restart, this file
    // would be missed and the old folder's would be reported under lib/.
    await settle(300);
    write("lib/new.aiff");
    write("lib.old/stale.aiff");
    await waitFor(() => has("add lib/new.aiff"));
    await settle();
    expect(events.filter((e) => e.includes("stale"))).toEqual([]);
  });

  it("re-watches a named file's folder after it is deleted and recreated", async () => {
    write("rb/master.db");
    await start(["rb/master.db"]);
    rmSync(p("rb"), { recursive: true });
    await waitFor(() => source!.states()[0]!.kind === "awaiting", 3000, "file to be awaited");
    write("rb/master.db");
    await waitFor(() => settled.includes("file rb/master.db"), 3000, "file to be watched again");
    writeFileSync(p("rb/master.db"), "after");
    await waitFor(() => has("change rb/master.db"), 3000, "change after recreate");
  });
});

describe.skipIf(!hasInotifywait)("the watcher process itself", () => {
  it("puts a watch back when inotifywait dies, and says so", async () => {
    mkdirSync(p("lib"));
    await start(["lib"]);
    execFileSync("pkill", ["-f", `inotifywait.*${root}`]);
    await waitFor(() => errors.some((e) => e.includes("exited")), 3000, "exit to be logged");
    await settle(400);
    write("lib/after-restart.aiff");
    await waitFor(() => has("add lib/after-restart.aiff"));
  });

  it("leaves no inotifywait running after stop()", async () => {
    mkdirSync(p("lib"));
    write("rb/master.db");
    await start(["lib", "rb/master.db"]);
    await source!.stop();
    source = undefined;
    let left = "";
    try { left = execFileSync("pgrep", ["-f", `inotifywait.*${root}`]).toString(); } catch { /* none */ }
    expect(left).toBe("");
  });

  it("does not restart the library's watch when an unrelated path appears", async () => {
    mkdirSync(p("lib"));
    await start(["lib", "later/"]);
    const pidsBefore = execFileSync("pgrep", ["-f", `inotifywait.*-r.*${p("lib")}`]).toString().trim();
    mkdirSync(p("later"));
    await waitFor(() => settled.includes("folder later"));
    const pidsAfter = execFileSync("pgrep", ["-f", `inotifywait.*-r.*${p("lib")}`]).toString().trim();
    expect(pidsAfter).toBe(pidsBefore);
  });
});
