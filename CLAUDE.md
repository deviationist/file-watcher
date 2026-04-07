# file-watcher

Generic file-system watcher daemon that monitors folders for changes and runs a configurable shell command after a debounce period. Designed for CIFS/NAS mounts where inotify doesn't work.

## Tech stack

- Node.js + TypeScript (strict, ESM)
- chokidar (poll mode for CIFS compatibility)
- dotenv for config
- tsx for dev, compiled JS for production

## Project structure

```
src/watcher.ts       — main daemon (single file)
.env                 — runtime config (gitignored)
.env.example         — documented config template
file-watcher.service — systemd unit file (references /home/trym/)
```

## Commands

```bash
npm run dev          # run with tsx watch (auto-reload)
npm run build        # compile to dist/
npm run start        # run compiled JS
```

## Configuration

All config is via environment variables (see `.env.example`):
- `WATCH_FOLDERS` — comma-separated paths to watch (required)
- `WATCH_EXTENSIONS` — comma-separated extensions without dots
- `WATCH_EVENTS` — chokidar events to listen for
- `DEBOUNCE_SECONDS` — quiet period before firing command
- `POLL_INTERVAL_SECONDS` — chokidar polling interval (>= 5 for CIFS)
- `ON_CHANGE_COMMAND` — shell command to execute (required)

## Key design decisions

- Uses polling (`usePolling: true`) because CIFS mounts don't support inotify
- `awaitWriteFinish` with 2s stability threshold to handle slow file copies
- Single debounce timer resets on every matching event — only fires once after activity settles
- The watcher is fully agnostic — it has no knowledge of what the command does

## Deployment

The systemd unit file targets the `trym` user and machine. Adjust `mnt-Resilio.mount` to match the actual CIFS mount unit name.

```bash
sudo cp file-watcher.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now file-watcher
```
