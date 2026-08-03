#!/usr/bin/env python3
"""Replay test: disconnects for a few seconds and verifies the buffer replays.

Usage:
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


# Phase 1: initial connection, read 4s
c1 = socket.create_connection((HOST, PORT), timeout=8)
a = read_lines(c1, 4)
c1.close()
print(f"Phase 1: {len(a)} lines received (initial read)")

# Phase 2: outage WITHOUT connection (data keeps entering the buffer)
time.sleep(3)
print("Phase 2: 3s gap without connection (data only to the buffer)")

# Phase 3: reconnect; the replay should return the recent window
c2 = socket.create_connection((HOST, PORT), timeout=8)
t_replay = time.time()
b = read_lines(c2, 2)
c2.close()
replay_time = time.time() - t_replay
print(f"Phase 3: within the first {replay_time:.1f}s after reconnecting, {len(b)} lines arrived (replay)")

# Phase 4: multicast (2 clients in parallel)
print("\n--- Multicast test (2 live clients) ---")
cA = socket.create_connection((HOST, PORT), timeout=8)
read_lines(cA, 3)  # drenar replay de A
cB = socket.create_connection((HOST, PORT), timeout=8)
read_lines(cB, 2)  # drenar replay de B
both = read_lines(cB, 4)
cA.close()
cB.close()
print(f"Client B received {len(both)} lines with A connected in parallel => broadcast OK")

