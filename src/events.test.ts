import { describe, it, expect } from "vitest";
import { catchUpVerdict, classifyInotify, unlinkWhilePending } from "./events.js";

const WINDOW = 600_000;

describe("classifyInotify — what a raw inotify line means", () => {
  it("treats a create followed by a close as an arrival", () => {
    const created = new Map<string, number>();
    expect(classifyInotify("CREATE", "/m/a.mp3", created, 0, WINDOW)).toBeNull();
    expect(classifyInotify("CLOSE_WRITE,CLOSE", "/m/a.mp3", created, 1, WINDOW)).toBe("add");
  });

  // The regression that broke edits for a day: CLOSE_WRITE alone is a rewrite,
  // not an arrival, and calling it `add` loses the distinction every consumer
  // routes on.
  it("treats a close with no create before it as an edit", () => {
    const created = new Map<string, number>();
    expect(classifyInotify("CLOSE_WRITE,CLOSE", "/m/a.mp3", created, 1, WINDOW)).toBe("change");
  });

  it("does not let one create excuse two closes", () => {
    const created = new Map<string, number>();
    classifyInotify("CREATE", "/m/a.mp3", created, 0, WINDOW);
    expect(classifyInotify("CLOSE_WRITE", "/m/a.mp3", created, 1, WINDOW)).toBe("add");
    expect(classifyInotify("CLOSE_WRITE", "/m/a.mp3", created, 2, WINDOW)).toBe("change");
  });

  it("forgets a create nothing ever closed, and reads the next close as an edit", () => {
    const created = new Map<string, number>();
    classifyInotify("CREATE", "/m/a.mp3", created, 0, WINDOW);
    // A later create elsewhere is what prunes the stale entry.
    classifyInotify("CREATE", "/m/b.mp3", created, WINDOW + 1, WINDOW);
    expect(classifyInotify("CLOSE_WRITE", "/m/a.mp3", created, WINDOW + 2, WINDOW)).toBe("change");
  });

  it("reads delete and moved_from as removals", () => {
    const created = new Map<string, number>();
    expect(classifyInotify("DELETE", "/m/a.mp3", created, 0, WINDOW)).toBe("unlink");
    expect(classifyInotify("MOVED_FROM", "/m/a.mp3", created, 0, WINDOW)).toBe("unlink");
  });

  it("reads moved_to as an arrival, because a replacement is indistinguishable", () => {
    const created = new Map<string, number>();
    expect(classifyInotify("MOVED_TO", "/m/a.mp3", created, 0, WINDOW)).toBe("add");
  });

  it("ignores directory events", () => {
    const created = new Map<string, number>();
    expect(classifyInotify("CREATE,ISDIR", "/m/genre", created, 0, WINDOW)).toBeNull();
    expect(classifyInotify("DELETE,ISDIR", "/m/genre", created, 0, WINDOW)).toBeNull();
  });

  it("drops a create it already knows about rather than resetting its age", () => {
    const created = new Map<string, number>();
    classifyInotify("CREATE", "/m/a.mp3", created, 0, WINDOW);
    classifyInotify("CREATE", "/m/a.mp3", created, 500, WINDOW);
    expect(created.get("/m/a.mp3")).toBe(0);
  });
});

describe("a removal arriving while an add is still pending", () => {
  // A real new file that vanishes before it was ever announced: the add was
  // never published, so there is nothing to retract.
  it("drops the removal when the pending add was live", () => {
    expect(unlinkWhilePending("live")).toBe("drop");
  });

  // The regression this test exists for: the catch-up re-announces files that
  // already exist and that consumers already know about. Treating a removal in
  // that window as "it never arrived" silently leaves an orphan in Plex —
  // observed 2026-08-25, a file deleted six seconds after a catch-up run.
  it("publishes the removal when the pending add came from the catch-up", () => {
    expect(unlinkWhilePending("catchup")).toBe("publish");
  });
});

describe("what the periodic reconcile should announce", () => {
  const CUTOFF = 1_000;

  it("announces a file nothing has announced", () => {
    // The case it exists for: inotify dropped the event, so nothing published,
    // so nothing recorded. Measured once at 1599 of 1600 lost, silently.
    expect(catchUpVerdict(2_000, undefined, CUTOFF)).toBe("announce");
  });

  it("skips one the live path already announced", () => {
    // This is the fix. The memory used to hold only the sweep's own
    // announcements, so every published track was announced a second time
    // minutes later -- a whole downstream run for a file nothing had touched.
    expect(catchUpVerdict(2_000, 2_000, CUTOFF)).toBe("skip");
  });

  it("announces one that was modified after it was announced", () => {
    // Keyed on the mtime, not the path. A file announced and then edited is
    // genuinely news again, and a path-only memory would swallow it.
    expect(catchUpVerdict(2_500, 2_000, CUTOFF)).toBe("announce");
  });

  it("skips anything older than the window", () => {
    expect(catchUpVerdict(999, undefined, CUTOFF)).toBe("skip");
  });

  it("skips an old file even if it was never announced", () => {
    // Not a gap: the window is twice the sweep interval, so a file this old was
    // inside at least two earlier sweeps. Announcing it now would mean
    // re-announcing the whole library on every restart.
    expect(catchUpVerdict(0, undefined, CUTOFF)).toBe("skip");
  });

  it("announces a file exactly on the cutoff", () => {
    // The boundary is inclusive on purpose: the overlap between sweeps is what
    // makes a file falling between two of them impossible, and excluding the
    // boundary would open a one-millisecond hole in it.
    expect(catchUpVerdict(1_000, undefined, CUTOFF)).toBe("announce");
  });
});
