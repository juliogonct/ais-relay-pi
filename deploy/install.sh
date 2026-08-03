#!/usr/bin/env bash
# =====================================================================
# ais-relay — instalador
#
#   sudo ./deploy/install.sh
#
# Instala (o actualiza) el servicio y crea el fichero de configuración
# único (/etc/ais-relay/ais-relay.conf) si no existe. Luego edítalo y
# reinicia:  sudo systemctl restart ais-relay
# =====================================================================
set -euo pipefail

SRC="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
BIN=/usr/local/bin/ais-relay.py
UNIT=/etc/systemd/system/ais-relay.service
CONF_DIR=/etc/ais-relay
CONF="$CONF_DIR/ais-relay.conf"

# 1) Programa
install -o root -g root -m 0755 "$SRC/../ais-relay.py" "$BIN"
echo "✔ Programa: $BIN"

# 2) Unidad systemd
install -o root -g root -m 0644 "$SRC/ais-relay.service" "$UNIT"
echo "✔ Servicio: $UNIT"

# 3) Config (LUGAR ÚNICO) — solo si no existe, para no pisar tus ajustes
mkdir -p "$CONF_DIR"
if [ ! -f "$CONF" ]; then
  install -o root -g root -m 0600 "$SRC/ais-relay.conf.example" "$CONF"
  echo "✔ Config creada: $CONF  (edítala y reinicia el servicio)"
else
  echo "· Config existente (no se sobrescribe): $CONF"
fi

# 4) Arrancar/habilitar
systemctl daemon-reload
systemctl enable --now ais-relay.service >/dev/null 2>&1 || systemctl restart ais-relay.service
echo "✔ ais-relay activado."
echo ""
echo "Próximo paso: edita $CONF y, si lo cambias, haz:"
echo "  sudo systemctl restart ais-relay"
echo "Estado: sudo systemctl status ais-relay — Prueba: nc <host> 10110"
