# file-watcher

Generic filesystem change notifier built on MQTT pub/sub. A **publisher** watches one or more folders and emits change events to an MQTT broker. One or more independent **subscribers** filter, debounce, and run a shell command in response. Designed for CIFS/NAS mounts where inotify doesn't work.

```
chokidar (polling) → publisher → MQTT broker (Mosquitto)
                                       │
                          ┌─────────────┼─────────────┐
                          ▼             ▼             ▼
                    subscriber 1   subscriber 2   subscriber N
```

The system is fully agnostic — neither publisher nor subscriber knows anything about what events mean. Each subscriber wires its `ON_CHANGE_COMMAND` to an external script that owns the domain logic.

## Tech stack

- Node.js + TypeScript (strict, ESM)
- [chokidar](https://github.com/paulmillr/chokidar) (poll mode for CIFS compatibility)
- [mqtt](https://www.npmjs.com/package/mqtt) (QoS 1)
- commander for CLI args, dotenv for env-var fallback
- tsx for dev, compiled JS for production

## Quickstart

```bash
git clone <repo>
cd file-watcher
npm install
cp .env.example .env       # then fill in MQTT_BROKER_URL, WATCH_FOLDERS, etc.
npm run build
npm run start:publisher    # in one terminal
npm run start:subscriber   # in another
```

## Commands

| Command | Purpose |
|---|---|
| `npm run dev:publisher` | Run publisher with `tsx watch` |
| `npm run dev:subscriber` | Run subscriber with `tsx watch` |
| `npm run dev:all` | Run both concurrently (dev) |
| `npm run build` | Compile TypeScript to `dist/` |
| `npm run start:publisher` | Run compiled publisher |
| `npm run start:subscriber` | Run compiled subscriber |
| `npm test` | Run vitest test suite once |

## Configuration

All settings can be supplied via CLI args or env vars (CLI takes precedence). See `.env.example` for the full list with documentation.

### MQTT broker (shared by both binaries)

| Var | Default | Description |
|---|---|---|
| `MQTT_BROKER_URL` | — (required) | e.g. `mqtt://localhost:1883` |
| `MQTT_TOPIC` | `file-watcher/change` | Topic for publish/subscribe |
| `MQTT_USERNAME` / `MQTT_PASSWORD` | — | Optional credentials |

### Publisher

| Var | Default | Description |
|---|---|---|
| `WATCH_FOLDERS` | — (required) | Comma-separated absolute paths to watch |
| `WATCH_EXTENSIONS` | (all) | Comma-separated extensions without dots (e.g. `mp3,flac`) |
| `WATCH_IGNORE_PATTERNS` | `._*` | Comma-separated basename globs to ignore. Default drops macOS AppleDouble metadata files |
| `WATCH_EVENTS` | `add,unlink,change` | chokidar events to react to |
| `USE_POLLING` | `true` | Required for CIFS/SMB mounts |
| `POLL_INTERVAL_SECONDS` | `10` | Polling interval (≥5 for CIFS) |
| `STABILITY_THRESHOLD_SECONDS` | `60` | File stability window before emitting an event |

### Subscriber

| Var | Default | Description |
|---|---|---|
| `PATH_PREFIXES` | (all) | Comma-separated path prefixes to filter on |
| `DEBOUNCE_SECONDS` | `30` | Quiet period after last matching event before firing |
| `ON_CHANGE_COMMAND` | — (required) | Shell command to execute when debounce expires |

## MQTT message format

Each accepted file event becomes one MQTT message on `MQTT_TOPIC`:

```json
{ "event": "add|change|unlink", "path": "/absolute/host/path", "timestamp": "2026-05-11T07:50:03.180Z" }
```

## Subscriber → command contract

When debounce expires, the subscriber spawns `ON_CHANGE_COMMAND` with two env vars populated:

### `CHANGED_PATHS` (newline-separated)

Deduped absolute host paths from the debounce window. Simple consumers can use this alone.

```
/mnt/music/main/track1.mp3
/mnt/music/main/track2.mp3
```

### `CHANGED_EVENTS` (JSON)

Full event log in arrival order, no deduplication. Same per-event shape as the MQTT message.

```json
[
  {"event":"add",   "path":"/mnt/music/main/track1.mp3","timestamp":"2026-05-11T08:00:01.200Z"},
  {"event":"unlink","path":"/mnt/music/main/track2.mp3","timestamp":"2026-05-11T08:00:02.350Z"}
]
```

Consumer-side parse:

```ts
const events = JSON.parse(process.env.CHANGED_EVENTS ?? "[]") as Array<{
  event: "add" | "change" | "unlink";
  path: string;
  timestamp: string;
}>;
```

The array is guaranteed non-empty when the command runs. The same `path` may appear more than once with different events (e.g. `unlink` then `add` for a rename). Process in order, or collapse per-path with last-wins.

## Deployment

The publisher and each subscriber are designed to run as separate systemd units. The broker (Mosquitto) is expected to run separately — this repo doesn't include broker infrastructure.

Templates live in `systemd/`:

- `file-watcher-publisher.service.example`
- `file-watcher-subscriber.service.example`

Copy each, replace the placeholders (user, working directory, node binary path, watch config, etc.), drop the `.example` suffix, and install with `systemd/install.sh install`. The bare `.service` files are gitignored so each host can carry its own tailored copies.

## Key design decisions

- **Polling, not inotify.** CIFS mounts don't fire inotify events, so chokidar runs in poll mode by default.
- **`awaitWriteFinish` waits for the file to stabilize** for `STABILITY_THRESHOLD_SECONDS` before firing — slow NAS copies generate one event, not dozens.
- **Publisher emits immediately, subscribers own their debounce.** Each subscriber chooses how long to coalesce events for its workload.
- **Resilient to broker restarts.** Both binaries auto-reconnect every 2 s; the subscriber re-subscribes on each reconnect.
- **Fan-out is free.** Add a new reaction (e.g. "back up changed files to S3") by writing a new subscriber unit pointing at the same broker. No publisher change needed.
- **Agnostic by design.** Don't add domain knowledge (Plex, Rekordbox, S3, etc.) into the file-watcher itself. Wire `ON_CHANGE_COMMAND` to an external script that owns those decisions.
