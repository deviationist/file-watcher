# Knowing when a file has finished arriving

Measured on xavi, 2026-08-24, against `/tank/music/on-hold` (local ZFS, also
exported over NFSv4.2 to quim).

## The question

Ingress must not be read while a file is still being written: a truncated file
that gets enriched and published looks exactly like a real one, and Plex will
not give it back. Until now the answer was a heuristic — `STABILITY_MODE=add-only`
records `(size, mtime)` on `add`, waits `STABILITY_THRESHOLD_SECONDS`, re-stats,
and publishes only if neither moved.

That works, but it infers completion from *absence of change*, so a writer that
stalls longer than the window fools it, and the window is pure latency on every
file that was never in doubt.

## Finding 1: inotify works here, despite what the internet says

The common claim is that inotify cannot be used with NFS. That is true for
watching an NFS **client mount** for changes made by other clients — the client
kernel never sees them.

It does not apply to this setup. The watcher runs on **xavi, the NFS server**,
watching a **local ZFS path**. `nfsd` performs client writes through the kernel
VFS, which is where the fsnotify hooks live, so the events fire normally.

Verified: a `cp` from quim over NFS produced, on xavi:

    OPEN → MODIFY ×6 → CLOSE_WRITE

and a 15 MB copy was detected at the instant of creation, with writes settling
~350 ms later.

## Finding 2: two events cover every way a file can arrive

Every arrival method was tested into a watched directory, from quim over NFS
unless noted:

| method             | events on the FINAL name          | why it is definitive     |
|--------------------|-----------------------------------|--------------------------|
| `cp`               | `CREATE` + `CLOSE_WRITE`          | the writer closed it     |
| `scp`              | `CREATE` + `CLOSE_WRITE`          | the writer closed it     |
| `mv` (cross-fs)    | `CREATE` + `CLOSE_WRITE`          | copy then unlink         |
| `rsync`            | `MOVED_TO`                        | atomic rename            |
| `mv` (same-fs)     | `MOVED_TO`                        | atomic rename            |
| Resilio (`.!sync`) | `MOVED_TO`                        | atomic rename            |

**The rule: act on `IN_CLOSE_WRITE` or `IN_MOVED_TO`, on a non-temp final name.**

This is not a heuristic. Either the kernel says the writer closed the file, or
the file arrived by rename and was therefore complete before it had that name.

### The trap

`rsync` and Resilio both `CLOSE_WRITE` a **temp** file and only then rename it:

    CREATE       .b-rsync.aiff.ZJb4UR
    CLOSE_WRITE  .b-rsync.aiff.ZJb4UR
    MOVED_FROM   .b-rsync.aiff.ZJb4UR
    MOVED_TO     b-rsync.aiff

A naive "any CLOSE_WRITE" rule ingests the temp file. Temp names must be
ignored — dotfiles covers rsync (`.name.XXXXXX`), Resilio (`.!sync`) and our own
staging convention.

## Finding 3: the threshold, not the polling, was the cost

With `add-only`, `STABILITY_THRESHOLD_SECONDS` dominates. Polling only cost the
discovery delay (`POLL_INTERVAL_SECONDS`, ~10 s). Measured pickup:

| configuration                                   | pickup   |
|-------------------------------------------------|----------|
| polling, threshold 60 s, debounce 30 s          | 60–105 s |
| inotify, threshold 15 s, debounce 10 s          | 37 s     |
| inotify, CLOSE_WRITE/MOVED_TO, debounce 10 s    | ~15 s    |

The last row is the prize, and it comes with a *stronger* guarantee than the
first, not a weaker one.

## Defence in depth

media-bridge re-checks stability itself immediately before consuming a file
(`settled()` in `main.py`: two stats a short gap apart). The watcher decides
from the other side of the network; the consumer asks again where the cost of
being wrong is paid. That check stays regardless of what this project does, and
it is what makes the threshold here a tuning knob rather than the only thing
between a half-copied file and the library.

media-bridge is also idempotent about repeats: a path already queued or in
flight is refused, and a path that has already left ingress is skipped rather
than failed. Duplicate announcements are therefore cheap, which matters because
an event-driven watcher emits more of them than a polling one.

## Caveat if polling is dropped entirely

inotify can drop events on queue overflow. With no polling at all, a dropped
event is lost rather than late. Keep a slow safety sweep (~5 min) as a backstop.

## Note on NFS versions

quim mounts NFSv4.2. NFSv4 is stateful and carries OPEN/CLOSE, which is what
lets the server translate a client close into a VFS close and therefore an
`IN_CLOSE_WRITE`. NFSv3 is stateless and this reasoning does not carry over.
