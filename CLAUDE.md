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
src/shared.ts             — shared logging, types, CLI helpers
src/publisher.ts          — watches filesystem, publishes events to MQTT
src/subscriber.ts         — subscribes to MQTT, debounces, runs command
src/watcher.ts            — legacy standalone daemon (pre-MQTT)
.env                      — runtime config (gitignored)
.env.example              — documented config template
```

The MQTT broker (Mosquitto) and web client (MQTTX Web) live in `~/docker-config/mqtt/` as shared infrastructure — not in this project.

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

### MQTT broker (shared)
- `MQTT_BROKER_URL` — broker URL (required)
- `MQTT_TOPIC` — topic (default: `file-watcher/change`)
- `MQTT_USERNAME` / `MQTT_PASSWORD` — optional credentials

### Publisher
- `WATCH_FOLDERS` — comma-separated paths to watch (required)
- `WATCH_EXTENSIONS` — comma-separated extensions without dots
- `WATCH_EVENTS` — chokidar events to listen for
- `USE_POLLING` — use polling mode (default: true, required for CIFS/NAS)
- `POLL_INTERVAL_SECONDS` — chokidar polling interval (>= 5 for CIFS)

### Subscriber
- `PATH_PREFIXES` — comma-separated path prefixes to filter on
- `DEBOUNCE_SECONDS` — quiet period before firing command
- `ON_CHANGE_COMMAND` — shell command to execute (required, receives `CHANGED_PATHS` env var)

### MQTT message format

```json
{ "event": "add|change|unlink", "path": "/absolute/path", "timestamp": "ISO-8601" }
```

## Key design decisions

- Uses polling (`usePolling: true`) because CIFS mounts don't support inotify
- `awaitWriteFinish` with configurable `stabilityThreshold` (default 60s) so slow CIFS copies generate one event, not many
- Publisher emits immediately (no debounce) — subscribers own their debounce
- Subscriber passes `CHANGED_PATHS` (newline-separated) as env var to the command
- Path deduplication via Set — same file changing multiple times = one entry
- The system is fully agnostic — no knowledge of what subscribers do with events
- Publisher + subscriber auto-reconnect to the broker (2s retry) and the subscriber re-subscribes on each reconnect, so broker restarts are non-fatal

## Broker

Mosquitto + MQTTX Web live in `~/docker-config/mqtt/` (not in this repo). Connection details are in `.env` (`MQTT_BROKER_URL`, `MQTT_USERNAME`, `MQTT_PASSWORD`). See the docker-config repo for how the broker is configured.

## Deployment

The publisher and each subscriber run as separate systemd services. The MQTT broker (Mosquitto) runs as a Docker container.

## Downstream integrations

This repo stays domain-agnostic. Subscribers wire their `ON_CHANGE_COMMAND` to external scripts that own the domain logic. Current integrations:

- **Plex auto-rescan** — `/home/xavi/scripts/plex-scanner/` houses `plex-orchestrator.ts` (host→container path mapping + Plex section discovery) and `plex-scan-trigger.ts` (the Plex API trigger via `@ctrl/plex`). The music subscriber's `ON_CHANGE_COMMAND` invokes the orchestrator with `CHANGED_PATHS` (newline-separated absolute host paths).
