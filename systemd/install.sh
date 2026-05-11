#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SERVICES=(file-watcher-publisher file-watcher-plex file-watcher-rekordbox)

case "${1:-}" in
  install)
    echo "Installing systemd services..."
    for svc in "${SERVICES[@]}"; do
      sudo cp "$SCRIPT_DIR/$svc.service" /etc/systemd/system/
      echo "  Copied $svc.service"
    done
    sudo systemctl daemon-reload
    echo "Enabling and starting services..."
    sudo systemctl enable --now "${SERVICES[@]/%/.service}"
    echo "Done. Check status with: $0 status"
    ;;

  uninstall)
    echo "Stopping and disabling services..."
    sudo systemctl disable --now "${SERVICES[@]/%/.service}" 2>/dev/null || true
    for svc in "${SERVICES[@]}"; do
      sudo rm -f "/etc/systemd/system/$svc.service"
      echo "  Removed $svc.service"
    done
    sudo systemctl daemon-reload
    echo "Done."
    ;;

  status)
    for svc in "${SERVICES[@]}"; do
      echo "=== $svc ==="
      systemctl status "$svc" --no-pager 2>/dev/null || echo "  Not installed"
      echo ""
    done
    ;;

  logs)
    journalctl -u "${SERVICES[0]}" -u "${SERVICES[1]}" -u "${SERVICES[2]}" -f --no-pager
    ;;

  *)
    echo "Usage: $0 {install|uninstall|status|logs}"
    exit 1
    ;;
esac
