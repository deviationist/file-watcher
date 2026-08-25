import { describe, it, expect } from "vitest";
import { classifyInotify, unlinkWhilePending } from "./events.js";

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
