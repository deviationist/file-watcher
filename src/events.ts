/// The two decisions the publisher gets wrong when nobody is looking.
///
/// Both were bugs before they were functions. `classifyInotify` exists because
/// CLOSE_WRITE was read as an arrival, which erased the difference between a
/// new file and an edited one and silently stopped tag edits reaching Plex for
/// a day. `unlinkWhilePending` exists because a removal arriving just after a
/// catch-up re-announcement was dropped as "it never really arrived", which
/// leaves Plex holding a track whose file is gone.
///
/// They live here, pure and exported, so a regression is a failing test rather
/// than a week of a pipeline looking healthy.

export type WatchEvent = "add" | "change" | "unlink";

/// What one line of `inotifywait` output means.
///
/// `created` is the caller's memory of paths seen as CREATE and not yet closed;
/// this both reads and prunes it. Returns null for lines that are not events in
/// their own right -- a CREATE, whose meaning arrives with its CLOSE_WRITE, and
/// anything about a directory.
export function classifyInotify(
  events: string,
  filePath: string,
  created: Map<string, number>,
  now: number,
  createWindowMs: number,
): WatchEvent | null {
  // A new genre folder is not a file arriving.
  if (events.includes("ISDIR")) return null;

  if (events.includes("DELETE") || events.includes("MOVED_FROM")) {
    created.delete(filePath);
    return "unlink";
  }

  if (events.includes("CREATE")) {
    // Pruned here rather than on a timer: a writer that creates a file and dies
    // without closing it would otherwise hold its entry forever, and the next
    // write to that path would be read as an arrival years later.
    for (const [seen, at] of created) {
      if (now - at > createWindowMs) created.delete(seen);
    }
    // Keep the *first* sighting. Re-stamping it would let a stream of creates
    // keep an entry alive past the window it is supposed to age out of.
    if (!created.has(filePath)) created.set(filePath, now);
    return null;
  }

  if (events.includes("CLOSE_WRITE")) {
    return created.delete(filePath) ? "add" : "change";
  }

  // MOVED_TO, and anything else that means the file is complete under this
  // name. Whether it replaced an existing file is not knowable from the kernel,
  // so it is an arrival and the consumer settles it against its own store.
  return "add";
}

export type PendingOrigin = "live" | "catchup";

/// What to do with a removal that lands while an add for the same path is still
/// waiting to be proven stable.
///
/// The answer turns on whether anything downstream has ever heard of the file.
/// A live add that has not been published yet describes a file that, as far as
/// every consumer knows, never existed -- so its removal is not news. A
/// catch-up add re-announces a file that has been there all along and is very
/// likely already in the library; dropping its removal is how an orphan is
/// made.
export function unlinkWhilePending(origin: PendingOrigin): "publish" | "drop" {
  return origin === "catchup" ? "publish" : "drop";
}
