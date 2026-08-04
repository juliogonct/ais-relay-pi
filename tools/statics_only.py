#!/usr/bin/env python3
"""Live AIS static streaming tool for ais-relay-pi.

Connects to the relay's TCP output and prints static AIS messages — class-A
(type 5) and class-B (type 24) — as readable, indented JSON ordered by arrival.
It includes the vessel's 4 antenna-dimension offsets (bow/stern/port/
starboard), length and beam wherever the message carries them, and does not
filter any field out. Dynamic/position and other messages are ignored. It
streams until interrupted with Ctrl+C. The relay remains a pure NMEA
transport; filtering/decoding happens in this client.

Usage:
    python tools/statics_only.py [host] [port] [max_messages]

Defaults: host=127.0.0.1, port=10110, max_messages=0 (run until Ctrl+C)
"""
import json
import os
import socket
import sys

sys.path.insert(
    0,
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "tests"),
)
import aivdm


def lines_from(sock):
    buf = b""
    while True:
        data = sock.recv(4096)
        if not data:
            return
        buf += data
        while b"\n" in buf:
            raw, buf = buf.split(b"\n", 1)
            line = raw.strip()
            if line:
                yield line.decode("ascii", "ignore")


def is_static(decoded):
    return decoded is not None and decoded.get("type") in (5, 24)


def main():
    host = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 10110
    max_messages = int(sys.argv[3]) if len(sys.argv) > 3 else 0

    sock = socket.create_connection((host, port), timeout=10)
    sock.settimeout(None)  # stream until interrupted
    print(f"# static (5/24) stream from {host}:{port} (Ctrl+C to stop)", file=sys.stderr)

    assembler = aivdm.FragmentAssembly()
    errors = 0
    total = 0
    try:
        for line in lines_from(sock):
            item = aivdm.decode_aivdm(line)
            if item is None:
                errors += 1
                continue
            decoded = assembler.add(item)
            if decoded is None:
                continue
            if not is_static(decoded):
                continue
            total += 1
            print(json.dumps(decoded, indent=2, ensure_ascii=False), flush=True)
            print(flush=True)  # blank line between messages for readability
            if max_messages and total >= max_messages:
                break
    except KeyboardInterrupt:
        pass
    finally:
        sock.close()
    print(f"# {total} static (5/24) messages decoded, {errors} undecodable lines", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
