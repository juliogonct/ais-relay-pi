#!/usr/bin/env bash
# =====================================================================
# ais-relay — desinstalador
#
#   sudo ./deploy/uninstall.sh
#
# Detiene y elimina el servicio y el programa. CONSERVA tu configuración
# en /etc/ais-relay/ais-relay.conf por si quisieras reinstalarlo.
# =====================================================================
set -euo pipefail

UNIT=/etc/systemd/system/ais-relay.service
BIN=/usr/local/bin/ais-relay.py

systemctl disable --now ais-relay.service >/dev/null 2>&1 || true
rm -f "$UNIT"
rm -f "$BIN"
systemctl daemon-reload

echo "✔ ais-relay desinstalado."
echo "  Config conservada en /etc/ais-relay/ais-relay.conf"
echo "  Para borrarla también: sudo rm -rf /etc/ais-relay"
