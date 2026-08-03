#!/usr/bin/env python3
"""Cliente de prueba del ais-relay: conecta por TCP y muestra el stream NMEA.

Uso:
    python test_client.py [host] [port] [segundos]

Defaults: host=127.0.0.1, port=10110, segundos=10
"""
import socket, sys, time


def _arg(i, default):
    return sys.argv[i] if len(sys.argv) > i else default


HOST = _arg(1, "127.0.0.1")
PORT = int(_arg(2, "10110"))
DURATION = float(_arg(3, "10.0"))

s = socket.create_connection((HOST, PORT), timeout=10)
s.settimeout(2.0)
print(f"Conectado a {HOST}:{PORT}", flush=True)

buf = b""
t0 = time.time()
count = 0
aivdm = 0
while time.time() - t0 < DURATION:
    try:
        data = s.recv(4096)
    except socket.timeout:
        print("  (sin datos en 2s)", flush=True)
        continue
    except Exception as e:
        print(f"Conexion cerrada: {e}", flush=True)
        break
    if not data:
        print("Fin de stream (cliente cerró)", flush=True)
        break
    buf += data
    while b"\n" in buf:
        line, buf = buf.split(b"\n", 1)
        line = line.strip()
        if not line:
            continue
        count += 1
        if line.startswith(b"!AIVDM") or line.startswith(b"!AIVDO"):
            aivdm += 1
        print(line.decode("ascii", "ignore"), flush=True)

print(f"\nResumen: {count} lineas, {aivdm} AIVDM, en {time.time()-t0:.1f}s", flush=True)
s.close()

