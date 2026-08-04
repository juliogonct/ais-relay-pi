#!/usr/bin/env python3
"""Real-time AIS NMEA -> JSON streaming decoder for ais-relay-pi.

Connects to the relay's TCP output, decodes every AIS message as it arrives
and prints it as readable, indented JSON (ordered by arrival), separated by
blank lines. It keeps streaming until interrupted with Ctrl+C. The relay
itself remains a pure NMEA transport; decoding happens only in this client.

Usage:
    python tools/live_ais_json.py [host] [port] [max_messages]

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


def main():
    host = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 10110
    max_messages = int(sys.argv[3]) if len(sys.argv) > 3 else 0

    sock = socket.create_connection((host, port), timeout=10)
    sock.settimeout(None)  # stream until interrupted
    print(f"# decoding live from {host}:{port} (Ctrl+C to stop)", file=sys.stderr)

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
            total += 1
            print(
                json.dumps(decoded, indent=2, ensure_ascii=False),
                flush=True,
            )
            print(flush=True)  # blank line between messages for readability
            if max_messages and total >= max_messages:
                break
    except KeyboardInterrupt:
        pass
    finally:
        sock.close()
    print(f"# {total} messages decoded, {errors} undecodable lines", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
