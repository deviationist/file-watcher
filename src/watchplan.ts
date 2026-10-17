/// What to ask the kernel to watch, given a list of paths that may be folders,
/// files, or nothing yet.
///
/// This exists because the switch to inotify (2026-08-24) kept only the
/// entries of WATCH_FOLDERS that were directories. master.db was a file, so it
/// was dropped without a word, and rekordbox2plex -- which runs on master.db
/// changes -- did not run again for two months. Every rule below is pure and
/// tested, so the next such loss is a failing test rather than a quiet log.
///
/// The three shapes a configured path can be in:
///
///   * **folder** -- it exists and is a directory. Watched recursively, as
///     before.
///   * **file** -- it exists and is not a directory, *or* it does not exist yet
///     but its parent does. Its parent is watched without -r and only events
///     for its own name pass. Never a watch on the file itself: that follows
///     the inode, and a rename-into-place (Resilio, rsync, SQLite) replaces the
///     inode, leaving a watch on nothing.
///   * **awaiting** -- neither it nor its parent exists. The nearest ancestor
///     that does exist is watched without -r for the next path component to
///     appear, and the plan is redrawn when it does.
///
/// A missing path counts as a file unless it is written with a trailing slash.
/// That is the useful default: the paths that come and go are files --
/// `master.db-wal` exists only while SQLite has something to checkpoint -- and
/// a folder that does not exist yet is rare enough to be spelled out.

import path from "node:path";

export type Probe = (p: string) => "dir" | "file" | "missing";

/// One configured path, normalised. `wantDir` is the trailing slash.
export interface Target {
  path: string;
  wantDir: boolean;
}

export type TargetState =
  | { kind: "folder"; path: string }
  | { kind: "file"; path: string; dir: string; name: string; present: boolean }
  | { kind: "awaiting"; path: string; dir: string; next: string };

/// Turn configured strings into targets. Relative paths are refused rather
/// than resolved: a unit's working directory is not something a watch list
/// should silently depend on.
export function parseTargets(paths: string[]): { targets: Target[]; errors: string[] } {
  const targets: Target[] = [];
  const errors: string[] = [];
  const seen = new Set<string>();
  for (const raw of paths) {
    const trimmed = raw.trim();
    if (!trimmed) continue;
    if (!path.isAbsolute(trimmed)) {
      errors.push(`watch path must be absolute: '${trimmed}'`);
      continue;
    }
    const wantDir = trimmed.length > 1 && trimmed.endsWith("/");
    const normalised = path.resolve(trimmed);
    if (seen.has(normalised)) continue;
    seen.add(normalised);
    targets.push({ path: normalised, wantDir });
  }
  return { targets, errors };
}

/// Where one target stands right now.
export function resolveTarget(target: Target, probe: Probe): TargetState {
  const kind = probe(target.path);
  if (kind === "dir") return { kind: "folder", path: target.path };

  const parent = path.dirname(target.path);
  const name = path.basename(target.path);
  if (!target.wantDir && probe(parent) === "dir") {
    return { kind: "file", path: target.path, dir: parent, name, present: kind === "file" };
  }

  // Walk up to the nearest directory that exists. `/` always does, so this
  // ends. A component that exists as a *file* is walked past too: the path
  // cannot exist until it is replaced, and that replacement is an event in its
  // parent like any other.
  let child = target.path;
  let dir = parent;
  while (dir !== child && probe(dir) !== "dir") {
    child = dir;
    dir = path.dirname(dir);
  }
  return { kind: "awaiting", path: target.path, dir, next: path.basename(child) };
}

/// What to run: one recursive inotifywait per folder, one flat one per
/// directory that holds a file target or an awaited name.
export interface Plan {
  states: TargetState[];
  /// Folder roots, watched with -r. Nested roots are dropped: the outer watch
  /// already sees everything under them.
  recursive: string[];
  /// Directories watched without -r, each with the names whose events pass.
  /// A directory already under a recursive root is not listed -- that watch
  /// sees its events, and a second one would report each of them twice.
  flat: Map<string, Set<string>>;
  /// Full paths whose appearance, disappearance or change of type means the
  /// plan must be redrawn. Matched against every event, from any watch.
  triggers: Set<string>;
  /// Full paths of file targets. Their events pass whichever watch saw them,
  /// and they bypass the name filters -- naming a file is asking for it.
  files: Set<string>;
}

export function isUnder(p: string, root: string): boolean {
  return p === root || p.startsWith(root.endsWith("/") ? root : `${root}/`);
}

export function buildPlan(states: TargetState[]): Plan {
  const folderRoots = states.filter((s) => s.kind === "folder").map((s) => s.path);
  const recursive = folderRoots
    .filter((r) => !folderRoots.some((other) => other !== r && isUnder(r, other)))
    .sort();

  const flat = new Map<string, Set<string>>();
  const triggers = new Set<string>();
  const files = new Set<string>();
  const covered = (dir: string): boolean => recursive.some((r) => isUnder(dir, r));
  const watchName = (dir: string, name: string): void => {
    if (covered(dir)) return;
    let names = flat.get(dir);
    if (!names) flat.set(dir, (names = new Set()));
    names.add(name);
  };

  for (const s of states) {
    if (s.kind === "file") {
      files.add(s.path);
      // A file that turns into a directory becomes a folder target.
      triggers.add(s.path);
      watchName(s.dir, s.name);
    } else if (s.kind === "awaiting") {
      const next = path.join(s.dir, s.next);
      triggers.add(next);
      watchName(s.dir, s.next);
    }
  }
  return { states, recursive, flat, triggers, files };
}

export function planFor(targets: Target[], probe: Probe): Plan {
  return buildPlan(targets.map((t) => resolveTarget(t, probe)));
}

/// Whether two plans would run the same watches. States can differ without
/// that -- a file target going from absent to present changes nothing to
/// watch -- and a restart for nothing would open a window of deafness.
export function sameWatches(a: Plan, b: Plan): boolean {
  if (a.recursive.join("\0") !== b.recursive.join("\0")) return false;
  if (a.flat.size !== b.flat.size) return false;
  for (const dir of a.flat.keys()) if (!b.flat.has(dir)) return false;
  return true;
}

/// Which watch a target's events arrive through.
export type Origin = { kind: "recursive"; root: string } | { kind: "flat"; dir: string };

export interface Routed {
  /// Hand the event on to the classifier.
  pass: boolean;
  /// Redraw the plan: something a target depends on appeared or went away.
  replan: boolean;
}

/// What to do with one line of inotifywait output.
///
/// `events` is inotifywait's `%e`, `filePath` its `%w%f` -- which for an event
/// about the watched directory itself is the directory with a trailing slash.
export function route(plan: Plan, origin: Origin, events: string, filePath: string): Routed {
  const p = filePath.length > 1 && filePath.endsWith("/") ? filePath.slice(0, -1) : filePath;
  const self = events.includes("DELETE_SELF") || events.includes("MOVE_SELF");

  // The watched directory went away or moved. inotifywait does not exit on
  // this -- measured: after `rm -r` of its root it keeps running, watching
  // nothing -- and a moved root keeps reporting under its *old* name. Either
  // way the watch is wrong now, so redraw. Self events below a recursive root
  // are inotifywait's own bookkeeping for subdirectories and mean nothing here.
  if (self) {
    const watched = origin.kind === "recursive" ? origin.root : origin.dir;
    return { pass: false, replan: p === watched };
  }

  const replan = plan.triggers.has(p);
  // ATTRIB is only subscribed to on flat watches, for named files. Should one
  // ever arrive from a recursive watch, it is not for the library.
  if (events.includes("ATTRIB")) {
    return { pass: plan.files.has(p) && origin.kind === "flat", replan: false };
  }
  if (origin.kind === "recursive") return { pass: true, replan };

  // A flat watch exists for particular names; everything else in that
  // directory -- master.db-shm, Rekordbox's caches, Resilio's partials -- is
  // somebody else's business.
  const names = plan.flat.get(origin.dir);
  const pass = plan.files.has(p) && !!names?.has(path.basename(p));
  return { pass, replan };
}

/// One line per target for the startup log, saying what was decided. A path
/// that is missing from this line is a path nothing is listening to.
export function describe(state: TargetState): string {
  switch (state.kind) {
    case "folder": return `${state.path} (folder)`;
    case "file": return `${state.path} (file${state.present ? "" : ", not there yet"})`;
    case "awaiting": return `${state.path} (waiting for ${path.join(state.dir, state.next)})`;
  }
}
