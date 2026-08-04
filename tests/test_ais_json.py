#!/usr/bin/env python3
"""Decode an AIS NMEA stream into JSON, preserving arrival order.

This is a verification test for ais-relay-pi: it decodes the NMEA output the
way a basic client decoder would, and asserts the JSON is valid and ordered by
arrival. The relay itself remains a pure NMEA transport.

Usage:
    python tests/test_ais_json.py                 # run fixture assertions
    python tests/test_ais_json.py --live HOST PORT [seconds]   # read from relay
"""
import json
import socket
import sys
import time

import aivdm

FIXTURE = [
    "!AIVDM,1,1,,B,13GQsw0P1DwsOVhEPPWVTgwJ2<1v,0*56",
    "!AIVDM,1,1,,A,4028j6ivb27QfOsa@0EPIH700hKF,0*50",
    "!AIVDM,1,1,,B,14hGj@4000wsNdpEPgFbOr7R0<1l,0*20",
    "!AIVDM,2,1,1,B,53Ebb?T24T9<D9@f000<4h4@E8u>0lDBr1A8E<0N0`C34uO306hhDU0A,0*3D",
    "!AIVDM,2,2,1,B,iCPH88888888880,2*14",
    "!AIVDM,1,1,,B,13GQsw0P1DwsOVhEPPWVTgwJ2<1v,0*56",
]


def read_lines(host, port, seconds):
    s = socket.create_connection((host, port), timeout=8)
    s.settimeout(2.0)
    buf = b""
    t0 = time.time()
    lines = []
    while time.time() - t0 < seconds:
        try:
            data = s.recv(4096)
        except socket.timeout:
            continue
        except Exception:
            break
        if not data:
            break
        buf += data
        while b"\n" in buf:
            raw, buf = buf.split(b"\n", 1)
            if raw.strip():
                lines.append(raw.decode("ascii", "ignore"))
    s.close()
    return lines


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "--live":
        host = sys.argv[2]
        port = int(sys.argv[3])
        seconds = float(sys.argv[4]) if len(sys.argv) > 4 else 10.0
        lines = read_lines(host, port, seconds)
        source = f"live {host}:{port}"
    else:
        lines = list(FIXTURE)
        source = "fixture"

    messages, errors = aivdm.decode_stream(lines)

    # Validation of a valid JSON, arrival-ordered list.
    data = json.dumps(messages, indent=2, ensure_ascii=False)
    parsed = json.loads(data)
    assert isinstance(parsed, list), "output must be a JSON array"
    assert all("mmsi" in m for m in parsed), "every record must have an mmsi"

    # Order by arrival: received_at must be monotonic.
    stamps = [m["received_at"] for m in parsed]
    assert stamps == sorted(stamps), "messages must be ordered by arrival"

    if len(parsed) > 0:
        assert "received_at" in parsed[0], "records must carry received_at"

    # The multi-fragment type-5 line only exists in the fixture; require the
    # fragment reassembly proof there (static type 5 -> name/callsign fields).
    if source == "fixture":
        statics = [m for m in parsed if "name" in m]
        assert statics, "expected a reassembled static (type 5) message"
        assert any(m["name"] for m in statics), "static message should contain a name"

    print(f"# decoded {len(parsed)} messages from {source} ({errors} undecodable lines)")
    print(data)
    return 0


if __name__ == "__main__":
    sys.exit(main())
