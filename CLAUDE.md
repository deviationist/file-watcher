# file-watcher

Generic file-system watcher using MQTT pub/sub. A publisher watches folders and files for changes and emits events to an MQTT broker. Subscribers independently filter, debounce, and run commands in response. Reads inotify on local filesystems (`EVENT_SOURCE=inotify`, what xavi runs) and polls with chokidar on CIFS/NAS mounts.

## Architecture

```
chokidar (polling) → publisher → MQTT broker (Mosquitto)
                                       │
                          ┌─────────────┼─────────────┐
                          ▼             ▼             ▼
                    subscriber 1   subscriber 2   subscriber N
```

## Tech stack

- Node.js + TypeScript (strict, ESM)
- chokidar (poll mode for CIFS compatibility)
- MQTT via `mqtt` package (QoS 1)
- commander for CLI args
- dotenv for env fallback config
- tsx for dev, compiled JS for production

## Project structure

```
src/shared.ts             — shared logging, types, CLI helpers
src/config.ts             — both binaries' config: flags, then env. Pure
src/publisher.ts          — wiring only: config, MQTT, an event source, the pipeline
src/watchplan.ts          — which paths are folders, files or not there yet, and what to watch for each. Pure
src/inotify.ts            — runs the plan: one inotifywait per watch, redrawn as paths come and go
src/events.ts             — what an inotify line means; the reconcile's rules. Pure
src/pipeline.ts           — filters, stability wait, change-must-be-real, catch-up, sweep
src/subscriber.ts         — wiring only: config, MQTT, the debouncer, the command
src/batch.ts              — the subscriber's prefix filter, message parsing, debounce
src/testing.ts            — waitFor/settle for the tests
src/watcher.ts            — legacy standalone daemon (pre-MQTT)
.env                      — runtime config (gitignored)
.env.example              — documented config template
```

The MQTT broker (Mosquitto) and web client (MQTTX Web) live in `~/docker-root/mqtt/` on quim as shared infrastructure — not in this project.

## Commands

```bash
npm run dev:publisher    # run publisher with tsx watch
npm run dev:subscriber   # run subscriber with tsx watch
npm run check            # typecheck + the whole suite (needs inotifywait)
npm run build            # check, then compile to dist/ -- refuses on a failure
npm run start:publisher  # run compiled publisher
npm run start:subscriber # run compiled subscriber
```

## Configuration

All config is via CLI args with env var fallback (CLI takes precedence). See `.env.example`.

### MQTT broker (shared)
- `MQTT_BROKER_URL` — broker URL (required)
- `MQTT_TOPIC` — topic (default: `file-watcher/change`)
- `MQTT_USERNAME` / `MQTT_PASSWORD` — optional credentials

### Publisher
- `WATCH_PATHS` — comma-separated folders, files, or paths not there yet (required). `WATCH_FOLDERS` is the old name, still read with a warning; both set is refused
- `EVENT_SOURCE` — `chokidar` (default, polling) or `inotify`
- `WATCH_EXTENSIONS` — comma-separated extensions without dots
- `WATCH_IGNORE_PATTERNS` — comma-separated basename globs to ignore (default `._*` to drop macOS AppleDouble metadata files on CIFS)
- `WATCH_EVENTS` — chokidar events to listen for
- `USE_POLLING` — use polling mode (default: true, required for CIFS/NAS)
- `POLL_INTERVAL_SECONDS` — chokidar polling interval (>= 5 for CIFS)

### Subscriber
- `PATH_PREFIXES` — comma-separated path prefixes to filter on
- `DEBOUNCE_SECONDS` — quiet period before firing command
- `ON_CHANGE_COMMAND` — shell command to execute (required, receives `CHANGED_PATHS` env var)

### Optional file logger (both)
- `LOG_FILE` — if set, every log line is also appended to this file (parent dir auto-created). stdout/stderr still receive the line so journald keeps capturing service output. Set per service in its systemd unit; unset to disable.

### MQTT message format

```json
{ "event": "add|change|unlink", "path": "/absolute/path", "timestamp": "ISO-8601" }
```

## Tests -- read before changing the publisher

The publisher fails silently: a path it stops watching produces no error, only
an absence downstream. That happened (2026-08-24 → 2026-10-17: master.db
unwatched, rekordbox2plex never ran), so the suite is layered to catch it:

- `watchplan.test.ts`, `events.test.ts`, `config.test.ts`, `batch.test.ts` — pure rules
- `pipeline.test.ts` — the publisher's event handling on real temp files
- `inotify.test.ts` — a real `inotifywait` on real temp folders: renames into
  place, files and folders appearing late, watched folders deleted or moved,
  the watcher process dying
- `e2e.test.ts` — the real binaries (via tsx), an in-process aedes broker, and
  a command that records what it was handed
- `integration.test.ts` — the *live* broker, only with `FILE_WATCHER_LIVE_BROKER=1`

Reintroducing the August bug fails 17 tests across the three layers. A change
to what is watched needs a test at the inotify or e2e level, not only a unit
test -- each piece passed its own tests last time.

## Key design decisions

- Polling (`usePolling: true`) on CIFS, which emits no inotify events; inotify on local filesystems
- A file is watched through its parent folder, never directly: a watch follows the inode and a rename-into-place replaces it. Never through a *recursive* watch of the parent either -- the rekordbox folder has ~24k subfolders
- `awaitWriteFinish` with configurable `stabilityThreshold` (default 60s) so slow CIFS copies generate one event, not many
- Publisher emits immediately (no debounce) — subscribers own their debounce
- Subscriber passes two env vars to the command:
  - `CHANGED_PATHS` — newline-separated, deduped absolute host paths (backwards-compat for simple consumers)
  - `CHANGED_EVENTS` — JSON array of `{event, path, timestamp}` in arrival order, no dedupe. Same per-event shape as the MQTT payload above
- Path deduplication via Set — same file changing multiple times = one entry
- The system is fully agnostic — no knowledge of what subscribers do with events
- Publisher + subscriber auto-reconnect to the broker (2s retry) and the subscriber re-subscribes on each reconnect, so broker restarts are non-fatal

## Broker

Mosquitto + MQTTX Web live in `~/docker-root/mqtt/` on quim (not in this repo). Connection details are in `.env` (`MQTT_BROKER_URL`, `MQTT_USERNAME`, `MQTT_PASSWORD`). See the docker-root repo (branch `docker-quim`) for how the broker is configured.

## Deployment

The publisher and each subscriber run as separate systemd services. The MQTT broker (Mosquitto) runs as a Docker container.

## Downstream integrations

This repo stays domain-agnostic. Subscribers wire their `ON_CHANGE_COMMAND` to external scripts that own the domain logic. Current integrations:

- **Plex auto-rescan** — `/home/xavi/scripts/plex-sync/` houses `plex-orchestrator.ts` (host→container path mapping + Plex section discovery) and `plex-scan-trigger.ts` (the Plex API trigger via `@ctrl/plex`). The music subscriber's `ON_CHANGE_COMMAND` invokes the orchestrator, which consumes `CHANGED_EVENTS` to branch per event type (API-delete on `unlink`, scoped section scan on `add`/`change`).
