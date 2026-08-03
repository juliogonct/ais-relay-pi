#!/usr/bin/env bash
# =====================================================================
# ais-relay — uninstaller
#
#   sudo ./deploy/uninstall.sh
#
# Stops and removes the service and the program. KEEPS your configuration
# at /etc/ais-relay/ais-relay.conf in case you want to reinstall.
# =====================================================================
set -euo pipefail

UNIT=/etc/systemd/system/ais-relay.service
BIN=/usr/local/bin/ais-relay.py

systemctl disable --now ais-relay.service >/dev/null 2>&1 || true
rm -f "$UNIT"
rm -f "$BIN"
systemctl daemon-reload

echo "OK ais-relay uninstalled."
echo "  Config kept at /etc/ais-relay/ais-relay.conf"
echo "  To remove it too: sudo rm -rf /etc/ais-relay"
