#!/usr/bin/env bash
# =====================================================================
# ais-relay — installer
#
#   sudo ./deploy/install.sh
#
# Installs (or updates) the service and creates the single configuration
# file (/etc/ais-relay/ais-relay.conf) if it does not exist. Then edit it
# and restart:  sudo systemctl restart ais-relay
# =====================================================================
set -euo pipefail

SRC="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
BIN=/usr/local/bin/ais-relay.py
UNIT=/etc/systemd/system/ais-relay.service
CONF_DIR=/etc/ais-relay
CONF="$CONF_DIR/ais-relay.conf"

# 1) Program
install -o root -g root -m 0755 "$SRC/../ais-relay.py" "$BIN"
echo "OK Program: $BIN"

# 2) systemd unit
install -o root -g root -m 0644 "$SRC/ais-relay.service" "$UNIT"
echo "OK Service: $UNIT"

# 3) Config (SINGLE PLACE) — only if it does not exist, to keep your settings
mkdir -p "$CONF_DIR"
if [ ! -f "$CONF" ]; then
  install -o root -g root -m 0600 "$SRC/ais-relay.conf.example" "$CONF"
  echo "OK Config created: $CONF  (edit it and restart the service)"
else
  echo "= Existing config kept (not overwritten): $CONF"
fi

# 4) Enable/start
systemctl daemon-reload
systemctl enable ais-relay.service >/dev/null 2>&1
systemctl restart ais-relay.service
echo "OK ais-relay enabled."
echo ""
echo "Next step: edit $CONF and, if changed, run:"
echo "  sudo systemctl restart ais-relay"
echo "Status: sudo systemctl status ais-relay — Test: nc <host> 10110"
