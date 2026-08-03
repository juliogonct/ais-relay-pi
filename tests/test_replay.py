#!/usr/bin/env python3
"""Replay test: disconnects for a few seconds and verifies the buffer replays.

Usage:
    python test_replay.py [host] [port]

Defaults: host=127.0.0.1, port=10110
"""
import socket, sys, time
import os


def _arg(i, default):
    return sys.argv[i] if len(sys.argv) > i else default


HOST = _arg(1, "127.0.0.1")
PORT = int(_arg(2, "10110"))
UDP_PORT = int(os.environ.get("AIS_RELAY_UDP_PORT", str(PORT)))


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


def send_udp(lines, port):
    sender = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    for line in lines:
        sender.sendto(line, ("127.0.0.1", port))
    sender.close()


# Phase 1: initial connection, read 1s
c1 = socket.create_connection((HOST, PORT), timeout=8)
a = read_lines(c1, 1)
c1.close()
print(f"Phase 1: {len(a)} lines received (initial read)")

# Phase 2: inject identifiable messages while disconnected
gap = [f"!AIVDM,1,1,,A,TEST{i:02d},0*00\r\n".encode() for i in range(3)]
send_udp(gap, UDP_PORT)
print("Phase 2: injected 3 identifiable messages while disconnected")

# Phase 3: reconnect; the replay should return the recent window
c2 = socket.create_connection((HOST, PORT), timeout=8)
t_replay = time.time()
b = read_lines(c2, 1)
c2.close()
replay_time = time.time() - t_replay
received_gap = [line for line in b if b"TEST" in line]
assert all(line.rstrip() in b for line in gap), "replay did not return all injected messages"
print(f"Phase 3: replay returned {len(received_gap)}/3 injected messages in {replay_time:.1f}s")

# Phase 4: multicast (2 clients in parallel)
print("\n--- Multicast test (2 live clients) ---")
cA = socket.create_connection((HOST, PORT), timeout=8)
read_lines(cA, 1)  # drain replay for A
cB = socket.create_connection((HOST, PORT), timeout=8)
read_lines(cB, 1)  # drain replay for B
live = b"!AIVDM,1,1,,A,LIVE01,0*00\r\n"
send_udp([live], UDP_PORT)
both = read_lines(cB, 1)
cA.close()
cB.close()
assert live.rstrip() in both, "live broadcast did not reach client B"
print("Client B received a live message with A connected in parallel => broadcast OK")

