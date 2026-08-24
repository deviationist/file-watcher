#!/usr/bin/env bash
#
# A second opinion on when a file has finished arriving.
#
# Runs alongside the real publisher and announces nothing: it only records what
# a CLOSE_WRITE/MOVED_TO watcher *would* have said, and when, so the two can be
# compared on the same arrivals before anything is switched over.
#
# Why those two events, and why they are enough for every way a file lands
# here, is in docs/file-arrival-detection.md.
set -euo pipefail

FOLDERS="${WATCH_FOLDERS:-/mnt/music/on-hold/Ingress}"
EXTENSIONS="${WATCH_EXTENSIONS:-mp3,aiff,flac,wav,m4a}"
LOG="${PROBE_LOG:-/var/log/inotify-probe.log}"

# Only directories: inotifywait on a file watches the inode, which a
# rename-into-place replaces, so the watch would survive as a watch on nothing.
dirs=()
IFS=',' read -ra parts <<< "$FOLDERS"
for p in "${parts[@]}"; do
  [ -d "$p" ] && dirs+=("$p")
done
[ ${#dirs[@]} -eq 0 ] && { echo "no watchable directories in $FOLDERS" >&2; exit 1; }

# mp3|aiff|flac|wav|m4a
ext_re="\\.(${EXTENSIONS//,/|})\$"

printf '%s starting: %s\n' "$(date -Is)" "${dirs[*]}" >> "$LOG"

# --format gives us the event and the full path; %T with --timefmt is the
# kernel's timestamp rather than ours, which matters when comparing latency.
inotifywait -m -q -r \
  -e close_write -e moved_to \
  --timefmt '%FT%T%z' --format '%T|%e|%w%f' \
  "${dirs[@]}" |
while IFS='|' read -r ts ev path; do
  base="${path##*/}"

  # Temp files, from us and from others. rsync writes .name.XXXXXX, Resilio
  # writes .!sync, and both CLOSE_WRITE the temp before renaming it into place
  # -- acting on that would ingest a partial file under a name nobody meant.
  case "$base" in
    .*|*.tmp|*.partial|*.!sync|*.crdownload) continue ;;
  esac

  [[ "$base" =~ $ext_re ]] || continue

  printf '%s|%s|%s\n' "$ts" "$ev" "$path" >> "$LOG"
done
