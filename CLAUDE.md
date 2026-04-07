# file-watcher

Generic file-system watcher using MQTT pub/sub. A publisher watches folders for changes and emits events to an MQTT broker. Subscribers independently filter, debounce, and run commands in response. Designed for CIFS/NAS mounts where inotify doesn't work.

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
src/shared.ts        — shared logging, types, CLI helpers
src/publisher.ts     — watches filesystem, publishes events to MQTT
src/subscriber.ts    — subscribes to MQTT, debounces, runs command
src/watcher.ts       — legacy standalone daemon (pre-MQTT)
.env                 — runtime config (gitignored)
.env.example         — documented config template
```

## Commands

```bash
npm run dev:publisher    # run publisher with tsx watch
npm run dev:subscriber   # run subscriber with tsx watch
npm run build            # compile to dist/
npm run start:publisher  # run compiled publisher
npm run start:subscriber # run compiled subscriber
```

## Configuration

All config is via CLI args with env var fallback (CLI takes precedence). See `.env.example`.

### Publisher
- `WATCH_FOLDERS` — comma-separated paths to watch (required)
- `WATCH_EXTENSIONS` — comma-separated extensions without dots
- `WATCH_EVENTS` — chokidar events to listen for
- `POLL_INTERVAL_SECONDS` — chokidar polling interval (>= 5 for CIFS)
- `MQTT_BROKER_URL` — MQTT broker connection URL (required)
- `MQTT_TOPIC` — topic to publish to (default: `file-watcher/change`)

### Subscriber
- `MQTT_BROKER_URL` — MQTT broker connection URL (required)
- `MQTT_TOPIC` — topic to subscribe to (default: `file-watcher/change`)
- `PATH_PREFIXES` — comma-separated path prefixes to filter on
- `DEBOUNCE_SECONDS` — quiet period before firing command
- `ON_CHANGE_COMMAND` — shell command to execute (required, receives `CHANGED_PATHS` env var)

### MQTT message format

```json
{ "event": "add|change|unlink", "path": "/absolute/path", "timestamp": "ISO-8601" }
```

## Key design decisions

- Uses polling (`usePolling: true`) because CIFS mounts don't support inotify
- `awaitWriteFinish` with 2s stability threshold to handle slow file copies
- Publisher emits immediately (no debounce) — subscribers own their debounce
- Subscriber passes `CHANGED_PATHS` (newline-separated) as env var to the command
- Path deduplication via Set — same file changing multiple times = one entry
- The system is fully agnostic — no knowledge of what subscribers do with events

## Deployment

The publisher and each subscriber run as separate systemd services. The MQTT broker (Mosquitto) runs as a Docker container.
