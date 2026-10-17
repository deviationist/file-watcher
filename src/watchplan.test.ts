import { describe, it, expect } from "vitest";
import {
  buildPlan, describe as describeState, parseTargets, planFor, resolveTarget, route, sameWatches,
  type Probe,
} from "./watchplan.js";

/// A filesystem made of a list: directories end in "/", everything else is a
/// file, and every ancestor of a listed path is a directory.
function fakeFs(entries: string[]): Probe {
  const dirs = new Set<string>(["/"]);
  const files = new Set<string>();
  for (const e of entries) {
    const p = e.endsWith("/") ? e.slice(0, -1) : e;
    (e.endsWith("/") ? dirs : files).add(p);
    let d = p;
    while ((d = d.slice(0, d.lastIndexOf("/")) || "/") !== "/") dirs.add(d);
  }
  return (p) => (dirs.has(p) ? "dir" : files.has(p) ? "file" : "missing");
}

const RB = "/mnt/resilio/data/rekordbox";
const rekordbox = fakeFs([`${RB}/master.db`, `${RB}/master.db-shm`, "/mnt/music/main/", "/mnt/music/on-hold/Ingress/"]);

describe("parseTargets — reading the watch list", () => {
  it("keeps a trailing slash as 'this will be a folder' and strips it from the path", () => {
    const { targets } = parseTargets(["/a/b/", "/a/c"]);
    expect(targets).toEqual([{ path: "/a/b", wantDir: true }, { path: "/a/c", wantDir: false }]);
  });

  it("refuses a relative path rather than resolving it against the unit's working directory", () => {
    const { targets, errors } = parseTargets(["music/main", "/ok"]);
    expect(targets.map((t) => t.path)).toEqual(["/ok"]);
    expect(errors).toEqual(["watch path must be absolute: 'music/main'"]);
  });

  it("drops duplicates, including ones that differ only by a trailing slash or a dot segment", () => {
    const { targets } = parseTargets(["/a/b", "/a/b/", "/a/./b", " /a/b "]);
    expect(targets).toHaveLength(1);
  });

  it("treats / as a folder, not as an empty path with a trailing slash", () => {
    expect(parseTargets(["/"]).targets).toEqual([{ path: "/", wantDir: false }]);
  });
});

describe("resolveTarget — what a configured path is right now", () => {
  it("watches an existing directory as a folder", () => {
    expect(resolveTarget({ path: "/mnt/music/main", wantDir: false }, rekordbox))
      .toEqual({ kind: "folder", path: "/mnt/music/main" });
  });

  // The regression this whole module exists for: master.db was dropped
  // because it was not a directory, and rekordbox2plex stopped for two months.
  it("watches an existing file through its parent folder, by name", () => {
    expect(resolveTarget({ path: `${RB}/master.db`, wantDir: false }, rekordbox))
      .toEqual({ kind: "file", path: `${RB}/master.db`, dir: RB, name: "master.db", present: true });
  });

  it("watches a file that does not exist yet, when its folder does — the -wal comes and goes", () => {
    expect(resolveTarget({ path: `${RB}/master.db-wal`, wantDir: false }, rekordbox))
      .toEqual({ kind: "file", path: `${RB}/master.db-wal`, dir: RB, name: "master.db-wal", present: false });
  });

  it("waits on the nearest existing ancestor when the parent is missing too", () => {
    expect(resolveTarget({ path: "/mnt/new/deep/file.db", wantDir: false }, rekordbox))
      .toEqual({ kind: "awaiting", path: "/mnt/new/deep/file.db", dir: "/mnt", next: "new" });
  });

  it("waits for a trailing-slash path to appear as a folder rather than watching it as a file", () => {
    expect(resolveTarget({ path: "/mnt/music/Vurderes", wantDir: true }, rekordbox))
      .toEqual({ kind: "awaiting", path: "/mnt/music/Vurderes", dir: "/mnt/music", next: "Vurderes" });
  });

  it("waits, rather than watching a file, when a trailing-slash path exists as a file", () => {
    expect(resolveTarget({ path: `${RB}/master.db`, wantDir: true }, rekordbox).kind).toBe("awaiting");
  });

  it("walks past an ancestor that exists as a file, since only a directory can be watched", () => {
    const fs = fakeFs(["/a/blocker"]);
    expect(resolveTarget({ path: "/a/blocker/x/y", wantDir: false }, fs))
      .toEqual({ kind: "awaiting", path: "/a/blocker/x/y", dir: "/a", next: "blocker" });
  });

  it("ends at / when nothing on the path exists", () => {
    expect(resolveTarget({ path: "/nowhere/at/all", wantDir: false }, fakeFs([])))
      .toEqual({ kind: "awaiting", path: "/nowhere/at/all", dir: "/", next: "nowhere" });
  });
});

describe("buildPlan — which inotifywait processes to run", () => {
  it("puts folders on recursive watches and files on a flat watch of their parent", () => {
    const plan = planFor(parseTargets([
      "/mnt/music/main", "/mnt/music/on-hold/Ingress", `${RB}/master.db`, `${RB}/master.db-wal`,
    ]).targets, rekordbox);
    expect(plan.recursive).toEqual(["/mnt/music/main", "/mnt/music/on-hold/Ingress"]);
    expect([...plan.flat.keys()]).toEqual([RB]);
    expect([...plan.flat.get(RB)!].sort()).toEqual(["master.db", "master.db-wal"]);
  });

  it("never watches the rekordbox folder recursively for the sake of one file", () => {
    // 23,797 subdirectories under it on xavi. A recursive watch would spend
    // that many inotify watches to observe master.db.
    const plan = planFor(parseTargets([`${RB}/master.db`]).targets, rekordbox);
    expect(plan.recursive).toEqual([]);
  });

  it("drops a folder nested inside another, which the outer watch already covers", () => {
    const fs = fakeFs(["/m/a/b/"]);
    expect(planFor(parseTargets(["/m/a", "/m/a/b"]).targets, fs).recursive).toEqual(["/m/a"]);
  });

  it("does not add a flat watch for a file inside a watched folder — it would report each event twice", () => {
    const fs = fakeFs(["/m/a/", "/m/a/x.db"]);
    const plan = planFor(parseTargets(["/m/a", "/m/a/x.db"]).targets, fs);
    expect(plan.flat.size).toBe(0);
    expect(plan.files.has("/m/a/x.db")).toBe(true);
  });

  it("does not take /m/ab for a folder under /m/a", () => {
    const fs = fakeFs(["/m/a/", "/m/ab/x.db"]);
    expect([...planFor(parseTargets(["/m/a", "/m/ab/x.db"]).targets, fs).flat.keys()]).toEqual(["/m/ab"]);
  });

  it("makes an awaited path's next component a trigger", () => {
    const plan = planFor(parseTargets(["/mnt/new/deep/"]).targets, rekordbox);
    expect([...plan.triggers]).toEqual(["/mnt/new"]);
    expect([...plan.flat.get("/mnt")!]).toEqual(["new"]);
  });

  it("makes a file target a trigger, so turning into a folder redraws the plan", () => {
    const plan = planFor(parseTargets([`${RB}/master.db`]).targets, rekordbox);
    expect(plan.triggers.has(`${RB}/master.db`)).toBe(true);
  });
});

describe("route — what one inotifywait line means for the plan", () => {
  const plan = planFor(parseTargets(["/mnt/music/main", `${RB}/master.db`, `${RB}/master.db-wal`]).targets, rekordbox);
  const flat = { kind: "flat", dir: RB } as const;
  const rec = { kind: "recursive", root: "/mnt/music/main" } as const;

  it("passes a rename-into-place of master.db — how Resilio delivers it", () => {
    expect(route(plan, flat, "MOVED_TO", `${RB}/master.db`)).toEqual({ pass: true, replan: true });
  });

  it("passes the WAL appearing, being written and going away", () => {
    for (const e of ["CREATE", "CLOSE_WRITE,CLOSE", "DELETE"]) {
      expect(route(plan, flat, e, `${RB}/master.db-wal`).pass).toBe(true);
    }
  });

  it("passes master.db's timestamp being set — what rbnudge does, with no new bytes", () => {
    expect(route(plan, flat, "ATTRIB", `${RB}/master.db`)).toEqual({ pass: true, replan: false });
  });

  it("drops ATTRIB for anything else in the folder, and from a library watch", () => {
    expect(route(plan, flat, "ATTRIB", `${RB}/master.db-shm`).pass).toBe(false);
    expect(route(plan, rec, "ATTRIB", "/mnt/music/main/House/a.aiff").pass).toBe(false);
  });

  it("drops everything else in the rekordbox folder", () => {
    for (const name of ["master.db-shm", "networkAnalyze6.db", "master.backup.20261017.db",
                        "master.db.!sync", "automixPlaylist6.xml"]) {
      expect(route(plan, flat, "CLOSE_WRITE,CLOSE", `${RB}/${name}`)).toEqual({ pass: false, replan: false });
    }
  });

  it("passes everything under a recursive root, leaving the name filters to the caller", () => {
    expect(route(plan, rec, "CLOSE_WRITE,CLOSE", "/mnt/music/main/House/a.aiff"))
      .toEqual({ pass: true, replan: false });
  });

  it("redraws when the watched folder itself is deleted — inotifywait keeps running, deaf", () => {
    expect(route(plan, flat, "DELETE_SELF", `${RB}/`)).toEqual({ pass: false, replan: true });
    expect(route(plan, rec, "DELETE_SELF", "/mnt/music/main/")).toEqual({ pass: false, replan: true });
  });

  it("redraws when the watched folder is moved — the watch would follow it and report the old name", () => {
    expect(route(plan, rec, "MOVE_SELF", "/mnt/music/main/")).toEqual({ pass: false, replan: true });
  });

  it("ignores self events for subdirectories, which inotifywait handles itself under -r", () => {
    expect(route(plan, rec, "DELETE_SELF", "/mnt/music/main/House/"))
      .toEqual({ pass: false, replan: false });
  });

  it("redraws when an awaited folder is created, from whichever watch saw it", () => {
    const waiting = planFor(parseTargets(["/mnt/music/main/New/", "/mnt/music/Later/"]).targets, rekordbox);
    expect(route(waiting, rec, "CREATE,ISDIR", "/mnt/music/main/New").replan).toBe(true);
    expect(route(waiting, { kind: "flat", dir: "/mnt/music" }, "CREATE,ISDIR", "/mnt/music/Later"))
      .toEqual({ pass: false, replan: true });
  });
});

describe("sameWatches — whether a new plan needs any watch restarted", () => {
  it("is true when only a file target's presence changed", () => {
    const before = planFor(parseTargets([`${RB}/master.db-wal`]).targets, rekordbox);
    const after = planFor(parseTargets([`${RB}/master.db-wal`]).targets, fakeFs([`${RB}/master.db-wal`]));
    expect(sameWatches(before, after)).toBe(true);
  });

  it("is false when an awaited folder appeared", () => {
    const t = parseTargets(["/mnt/x/"]).targets;
    expect(sameWatches(planFor(t, rekordbox), planFor(t, fakeFs(["/mnt/x/"])))).toBe(false);
  });
});

describe("describe — the startup line", () => {
  it("says what was decided for each kind, so a dropped path shows", () => {
    const plan = buildPlan(parseTargets(["/mnt/music/main", `${RB}/master.db`, `${RB}/master.db-wal`, "/mnt/new/x"])
      .targets.map((t) => resolveTarget(t, rekordbox)));
    expect(plan.states.map(describeState)).toEqual([
      "/mnt/music/main (folder)",
      `${RB}/master.db (file)`,
      `${RB}/master.db-wal (file, not there yet)`,
      "/mnt/new/x (waiting for /mnt/new)",
    ]);
  });
});
