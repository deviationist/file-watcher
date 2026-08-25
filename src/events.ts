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

/// Whether the periodic reconcile should announce a file it found on disk.
///
/// The sweep exists because a dropped inotify event is *silent* -- the kernel
/// queue overflows and nothing says so, on stderr or anywhere else. Measured:
/// 1599 of 1600 lost, with no output at all. So every file modified recently is
/// walked and compared against what has already been announced.
///
/// `announcedMtimeMs` is what the *whole publisher* has announced for this path,
/// not just what the sweep has. That distinction is the point: it used to record
/// only the sweep's own announcements, so a file the live inotify path had
/// already published was still news to the sweep, and every published track got
/// a second announcement minutes later -- a whole downstream run for a file
/// nothing had touched.
///
/// The safety net survives the change intact, and this is the property to hold
/// on to: if the event was dropped, nothing published, so nothing recorded, so
/// this returns "announce". The sweep only stops repeating work the fast path
/// already did.
export function catchUpVerdict(
  mtimeMs: number,
  announcedMtimeMs: number | undefined,
  cutoffMs: number,
): "announce" | "skip" {
  // Older than the window. Its announcement, if it ever needed one, was several
  // sweeps ago.
  if (mtimeMs < cutoffMs) return "skip";
  // Keyed on the mtime, not merely on the path: a file announced and *then*
  // modified is genuinely news again, and a path-only memory would swallow it.
  if (announcedMtimeMs === mtimeMs) return "skip";
  return "announce";
}

/// What the publisher last announced about a file: when it was written, and how
/// big it was. Both, because either alone can stay still across a real write.
export type Announced = { mtimeMs: number; size: number };

/// Whether a `CLOSE_WRITE` is worth calling a change.
///
/// `CLOSE_WRITE` says a file that was open for *writing* was closed. It does not
/// say anything was written, and two things exploit that gap constantly:
///
///  - **One save, reported twice.** A tag editor writes a file in more than one
///    open/write/close cycle. Measured while editing a track in OneTagger:
///    two `change` events, two milliseconds apart, for one save.
///  - **A close that wrote nothing at all.** Something opens a published file
///    about fifteen seconds after it lands and closes it again, with no
///    `MODIFY` and no change to mtime — verified against the file's own
///    timestamps. What opens it is still unknown.
///
/// Downstream, each of those cost a Plex album refresh, a round trip of
/// animation, and a count that went briefly wrong. So the question is not how
/// fast the events arrive -- a time window would be a guess at that -- but
/// whether the file is different from the one we last spoke about.
///
/// The hole, stated rather than pretended away: a writer that changes content
/// while preserving both size and mtime is invisible to this. That needs a tool
/// deliberately restoring mtime after an in-place overwrite of exactly equal
/// length. Nothing in this pipeline does it, and the alternative -- subscribing
/// to IN_MODIFY -- means thousands of events per large write against a 16384
/// queue, trading a real risk of silent overflow for an imagined one.
export function changeIsReal(
  current: Announced,
  lastAnnounced: Announced | undefined,
): boolean {
  // Never announced, so there is nothing to be the same as.
  if (!lastAnnounced) return true;
  return current.mtimeMs !== lastAnnounced.mtimeMs || current.size !== lastAnnounced.size;
}
