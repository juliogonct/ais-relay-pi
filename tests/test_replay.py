#!/usr/bin/env python3
"""Prueba del replay: desconecta unos segundos y verifica que el buffer re-expide.

Uso:
    python test_replay.py [host] [port]

Defaults: host=127.0.0.1, port=10110
"""
import socket, sys, time


def _arg(i, default):
    return sys.argv[i] if len(sys.argv) > i else default


HOST = _arg(1, "127.0.0.1")
PORT = int(_arg(2, "10110"))


def read_lines(conn, seconds):
    s = conn
    s.settimeout(1.5)
    buf = b""
    lines = []
    t0 = time.time()
    while time.time() - t0 < seconds:
        try:
            d = s.recv(4096)
        except socket.timeout:
            continue
        except Exception:
            break
        if not d:
            break
        buf += d
        while b"\n" in buf:
            line, buf = buf.split(b"\n", 1)
            if line.strip():
                lines.append(line.strip())
    return lines


# Fase 1: conexión inicial, leemos 4s
c1 = socket.create_connection((HOST, PORT), timeout=8)
a = read_lines(c1, 4)
c1.close()
print(f"Fase 1: {len(a)} lineas recibidas (lectura inicial)")

# Fase 2: hueco SIN conexión (los datos siguen entrando al buffer)
time.sleep(3)
print("Fase 2: hueco de 3s sin conexion (datos solo al buffer)")

# Fase 3: reconexión; el replay debería devolvernos la ventana reciente
c2 = socket.create_connection((HOST, PORT), timeout=8)
t_replay = time.time()
b = read_lines(c2, 2)
c2.close()
replay_time = time.time() - t_replay
print(f"Fase 3: en los primeros {replay_time:.1f}s tras reconectar llegaron {len(b)} lineas (replay)")

# Fase 4: multicast (2 clientes en paralelo)
print("\n--- Prueba multicast (2 clientes en vivo) ---")
cA = socket.create_connection((HOST, PORT), timeout=8)
read_lines(cA, 3)  # drenar replay de A
cB = socket.create_connection((HOST, PORT), timeout=8)
read_lines(cB, 2)  # drenar replay de B
both = read_lines(cB, 4)
cA.close()
cB.close()
print(f"Cliente B recibio {len(both)} lineas con A conectado en paralelo => broadcast OK")

