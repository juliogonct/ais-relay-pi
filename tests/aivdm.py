#!/usr/bin/env python3
"""Basic AIVDM decoder used by the ais-relay-pi tests.

This is a verification decoder only. AIS decoding is the responsibility of the
client/consumer; ais-relay-pi stays a pure NMEA transport.
"""
import re
from datetime import datetime, timezone

# Payload characters are ASCII 0x30..0x77 (48..119).
_SENTENCE = re.compile(
    r"^!(?P<talker>AIVDM|AIVDO)"
    r",(?P<count>\d+)"
    r",(?P<index>\d+)"
    r",(?P<seq>\d+|)"
    r",(?P<channel>[AB12]|)"
    r",(?P<payload>[\x30-\x77]+)"
    r",(?P<fill>[0-5])"
    r"(?:\*(?P<checksum>[0-9A-Fa-f]{2}))?$",
    re.IGNORECASE,
)


def decode_six(value):
    value = ord(value) - 48
    if value > 40:
        value -= 8
    return value


def payload_to_bits(payload, fill):
    bits = "".join(f"{decode_six(c):06b}" for c in payload)
    return bits[: len(bits) - fill]


def pfx(bits, start, length):
    return int(bits[start : start + length], 2)


def sig(value, length):
    if value & (1 << (length - 1)):
        value -= 1 << length
    return value


def signed_pfx(bits, start, length):
    return sig(pfx(bits, start, length), length)


def text_decode(bits, start, length):
    chars = []
    for i in range(start, start + length, 6):
        v = pfx(bits, i, 6)
        chars.append(chr(v + 64) if v < 32 else chr(v))
    return "".join(chars).rstrip("@ ")


def iso_now():
    return datetime.now(timezone.utc).isoformat()


def dims_from(bits, start):
    """Decode A/B/C/D: metres from the reference (antenna) point to bow, stern,
    port, starboard. Unknown values are reported as None."""
    a, b = pfx(bits, start, 9), pfx(bits, start + 9, 9)
    c, d = pfx(bits, start + 18, 6), pfx(bits, start + 24, 6)

    def conv(value, unknown):
        return None if value == unknown else value

    bow, stern = conv(a, 0), conv(b, 0)
    port, starboard = conv(c, 0), conv(d, 0)
    return {
        "bow": bow,
        "stern": stern,
        "port": port,
        "starboard": starboard,
        "length": (bow + stern) if (bow is not None and stern is not None) else None,
        "beam": (port + starboard) if (port is not None and starboard is not None) else None,
    }



def clean_position(rec):
    if rec.get("sog") == 102.3:
        rec["sog"] = None
    if rec.get("cog") == 360.0:
        rec["cog"] = None
    if rec.get("heading") == 511:
        rec["heading"] = None
    if rec.get("lat") is not None and (rec["lat"] >= 91 or rec["lat"] <= -91):
        rec["lat"] = None
    if rec.get("lon") is not None and (rec["lon"] >= 181 or rec["lon"] <= -181):
        rec["lon"] = None
    return rec


def decode_position(bits, msg_type):
    rec = {
        "type": msg_type,
        "mmsi": pfx(bits, 8, 30),
        "sog": pfx(bits, 50, 10) / 10.0,
        "lon": signed_pfx(bits, 61, 28) / 600000.0,
        "lat": signed_pfx(bits, 89, 27) / 600000.0,
        "cog": pfx(bits, 116, 12) / 10.0,
        "heading": pfx(bits, 128, 9),
    }
    return clean_position(rec)


def decode_static(bits, msg_type):
    rec = {
        "type": msg_type,
        "mmsi": pfx(bits, 8, 30),
        "imo": pfx(bits, 40, 30),
        "callsign": text_decode(bits, 70, 42),
        "name": text_decode(bits, 112, 120),
        "ship_type": pfx(bits, 232, 8),
        "draught": pfx(bits, 294, 8) / 10.0,
        "destination": text_decode(bits, 302, 120),
    }
    dims = dims_from(bits, 240)
    rec.update(dims)
    rec["dims"] = {k: dims[k] for k in ("bow", "stern", "port", "starboard", "length", "beam")}
    return rec


def decode_type24(bits):
    """Class-B static data: part A carries the name; part B carries callsign,
    vendor and the antenna-dimension offsets."""
    mmsi = pfx(bits, 8, 30)
    partno = pfx(bits, 38, 2)
    rec = {"type": 24, "mmsi": mmsi, "part": partno}
    if partno == 0:
        rec["name"] = text_decode(bits, 40, 120)
    else:
        rec["ship_type"] = pfx(bits, 40, 8)
        rec["vendor_company"] = pfx(bits, 48, 18)
        rec["vendor_model"] = pfx(bits, 66, 4)
        rec["vendor_serial"] = pfx(bits, 70, 20)
        rec["callsign"] = text_decode(bits, 90, 42)
        dims = dims_from(bits, 132)
        rec.update(dims)
        rec["dims"] = {k: dims[k] for k in ("bow", "stern", "port", "starboard", "length", "beam")}
    return rec


def decode_message(bits, msg_type):
    if msg_type in (1, 2, 3, 18):
        return decode_position(bits, msg_type)
    if msg_type == 5:
        return decode_static(bits, msg_type)
    if msg_type == 24:
        return decode_type24(bits)
    return {"type": msg_type, "mmsi": pfx(bits, 8, 30) if len(bits) >= 38 else None}


def decode_aivdm(line, received_at=None):
    match = _SENTENCE.match(line.strip())
    if not match:
        return None
    return {
        "count": int(match.group("count")),
        "index": int(match.group("index")),
        "seq": match.group("seq") or "0",
        "channel": match.group("channel") or "?",
        "bits": payload_to_bits(match.group("payload"), int(match.group("fill"))),
        "received_at": received_at or iso_now(),
    }


class FragmentAssembly:
    """Reassembles multi-fragment AIVDM messages in arrival order."""

    def __init__(self):
        self._partial = {}

    def add(self, sentence):
        if sentence["count"] == 1:
            return self._decode(sentence["bits"], sentence["received_at"])
        key = (sentence["seq"], sentence["channel"])
        parts = list(self._partial.get(key, []))
        parts.append(sentence)
        if len(parts) == sentence["count"]:
            del self._partial[key]
            bits = "".join(p["bits"] for p in parts)
            return self._decode(bits, sentence["received_at"])
        self._partial[key] = parts
        return None

    @staticmethod
    def _decode(bits, received_at):
        if len(bits) < 6:
            return None
        msg_type = pfx(bits, 0, 6)
        rec = decode_message(bits, msg_type)
        rec["received_at"] = received_at
        return rec


def decode_stream(lines):
    """Decode consecutive NMEA lines preserving arrival order.

    Returns (messages, errors): messages is an arrival-ordered list of decoded
    records; errors is the number of lines that could not be decoded.
    """
    assembler = FragmentAssembly()
    messages = []
    errors = 0
    for line in lines:
        item = decode_aivdm(line)
        if item is None:
            errors += 1
            continue
        decoded = assembler.add(item)
        if decoded is not None:
            messages.append(decoded)
    return messages, errors
