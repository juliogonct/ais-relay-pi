#!/usr/bin/env python3
"""ais-relay: NMEA UDP -> ring buffer -> TCP (network clients).

Receives an NMEA stream (e.g. from AIS-catcher) over UDP, keeps a circular
buffer in memory (and optionally on disk), and re-serves it over TCP to any
connecting network client. Designed for AIS receivers whose network link is
intermittent (WiFi, 4G/5G): data captured locally is not lost during an
outage.

Behavior:
  * Real-time pipe-through: every NMEA datagram is forwarded instantly to all
    connected clients (no rate limiting).
  * Replay on reconnect: on every new connection a small, cautious window of
    the buffer is re-sent (REPLAY_ON_CONNECT_SEC, default 30 s, ~ margin over
    the outage) and then the live stream continues. Data captured during a
    network outage (e.g. WiFi/5G) is recovered once connectivity returns.
  * The replay is ONLY the gap + a small margin; it is NOT exact. Duplicates
    are filtered on the CLIENT (idempotent: e.g. by MMSI in a consumer that
    keeps per-vessel state). For exact replay from a client's last marker, the
    client can send "REPLAY <seq>" and only messages with a higher sequence
    number are re-sent (no duplicates).

Access:
  * By default it relies on the network's access control (overlay ACL /
    firewall). The server listens on TCP_HOST/TCP_PORT.
  * Optional: if AIS_RELAY_TOKEN is set, the client must send
    "AUTH <token>\\n" as its first line before receiving data.
"""
import os
import socket
import threading
import time
import logging
import json
import hmac
import sys
from collections import deque

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("ais-relay")


def _env(name, default, cast=str):
    v = os.environ.get(name)
    return default if v is None else cast(v)


# Configuration (overridable via environment variables / systemd EnvironmentFile)
UDP_HOST = _env("AIS_RELAY_UDP_HOST", "127.0.0.1")
UDP_PORT = int(_env("AIS_RELAY_UDP_PORT", "10110"))
# Security: the UDP input must be loopback (only the local decoder). To allow a
# remote decoder, explicitly set 1 (insecure by default).
UDP_ALLOW_EXTERNAL = _env("AIS_RELAY_UDP_ALLOW_EXTERNAL", "0") == "1"
TCP_HOST = _env("AIS_RELAY_TCP_HOST", "0.0.0.0")
TCP_PORT = int(_env("AIS_RELAY_TCP_PORT", "10110"))
RETENTION_SEC = float(_env("AIS_RELAY_RETENTION_SEC", "3600"))   # in-memory ring
MAX_ENTRIES = int(_env("AIS_RELAY_MAX_ENTRIES", "200000"))
REPLAY_ON_CONNECT_SEC = float(_env("AIS_RELAY_REPLAY_ON_CONNECT_SEC", "30"))
TOKEN = _env("AIS_RELAY_TOKEN", "")
AUTH_TIMEOUT = float(_env("AIS_RELAY_AUTH_TIMEOUT", "5"))
LOG_FILE = _env("AIS_RELAY_LOG_FILE", "")   # "" = no disk persistence
# Limits (protect against slow/stuck clients and connection abuse):
MAX_CLIENTS = int(_env("AIS_RELAY_MAX_CLIENTS", "64"))
# Per-socket write timeout: if a client does not drain its buffer within this
# time it is disconnected (a frozen client must not stall the others).
SEND_TIMEOUT = float(_env("AIS_RELAY_SEND_TIMEOUT_SEC", "5"))
# Replay and disk-log ceilings (prevent CPU/memory abuse and unbounded disk):
MAX_REPLAY_ENTRIES = int(_env("AIS_RELAY_MAX_REPLAY_ENTRIES", "20000"))
# Max size (MB) of the disk JSONL before rotation. To disable disk logging,
# leave AIS_RELAY_LOG_FILE empty.
LOG_MAX_MB = float(_env("AIS_RELAY_LOG_MAX_MB", "64"))
LOG_BACKUPS = int(_env("AIS_RELAY_LOG_BACKUPS", "2"))

# ---- Global state ----
_SEQ = 0
_SEQ_LOCK = threading.Lock()
# Ring: deque of (seq, timestamp, line_bytes)
BUFFER = deque()
BUFFER_LOCK = threading.Lock()
CLIENTS = set()          # connected TCP sockets
CLIENTS_LOCK = threading.Lock()
ACTIVE = 0               # active connections (for the client limit)
ACTIVE_LOCK = threading.Lock()
LOG_FH = None            # optional JSONL file handle
_log_bytes = 0           # bytes written to the disk log (for rotation)


def next_seq():
    global _SEQ
    with _SEQ_LOCK:
        _SEQ += 1
        return _SEQ


def _open_log():
    global LOG_FH, _log_bytes
    if LOG_FILE:
        try:
            sz = os.path.getsize(LOG_FILE)
        except OSError:
            sz = 0
        _log_bytes = sz
        if sz >= LOG_MAX_MB * 1024 * 1024:
            _rotate_log()
        else:
            LOG_FH = open(LOG_FILE, "a", buffering=1)
            log.info("Disk log active: %s", LOG_FILE)


def _rotate_log():
    """Rotate the disk JSONL at max size, keeping LOG_BACKUPS copies."""
    global LOG_FH, _log_bytes
    try:
        if LOG_FH is not None:
            LOG_FH.close()
    except Exception:
        pass
    base = LOG_FILE
    if LOG_BACKUPS > 0:
        oldest = f"{base}.{LOG_BACKUPS}"
        if os.path.exists(oldest):
            try:
                os.remove(oldest)
            except OSError:
                pass
        for i in range(LOG_BACKUPS - 1, 0, -1):
            src = f"{base}.{i}"
            if os.path.exists(src):
                try:
                    os.replace(src, f"{base}.{i + 1}")
                except OSError:
                    pass
        if os.path.exists(base):
            try:
                os.replace(base, f"{base}.1")
            except OSError:
                pass
    LOG_FH = open(base, "a", buffering=1)
    _log_bytes = 0
    log.info("Disk log rotated (max %.0f MB, %d backups)", LOG_MAX_MB, LOG_BACKUPS)


def _append_disk(seq, ts, line):
    """Optionally persist each message to disk as JSONL, with size-based rotation."""
    global _log_bytes
    if LOG_FH is None:
        return
    try:
        rec = (
            json.dumps({"seq": seq, "ts": ts, "line": line.decode("ascii", "ignore")})
            + "\n"
        )
        LOG_FH.write(rec)
        _log_bytes += len(rec.encode("utf-8"))
        if _log_bytes >= LOG_MAX_MB * 1024 * 1024:
            _rotate_log()
    except Exception as exc:  # must not break the stream
        log.warning("failed to write disk log: %s", exc)


def add_to_buffer(line):
    now = time.time()
    seq = next_seq()
    with BUFFER_LOCK:
        BUFFER.append((seq, now, line))
        while BUFFER and (now - BUFFER[0][1] > RETENTION_SEC):
            BUFFER.popleft()
        while len(BUFFER) > MAX_ENTRIES:
            BUFFER.popleft()
    _append_disk(seq, now, line)


def snapshot_since(earliest_ts=None, seq_from=None, limit=None):
    """Return the N most recent lines (bytes) filtered by time/sequence.

    Iterate the ring in reverse with early exit, without copying the whole
    buffer, to avoid CPU/memory spikes on repeated replays.
    """
    out_rev = []
    with BUFFER_LOCK:
        for seq, ts, line in reversed(BUFFER):
            if earliest_ts is not None and ts < earliest_ts:
                break  # older behind; no longer applies
            if seq_from is not None and seq < seq_from:
                break
            out_rev.append(line)
            if limit is not None and len(out_rev) >= limit:
                break
    out_rev.reverse()
    return out_rev


# ---- UDP: ingest NMEA from the AIS decoder ----
def udp_receiver():
    udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    udp.bind((UDP_HOST, UDP_PORT))
    log.info("UDP listening on %s:%s", UDP_HOST, UDP_PORT)
    udp.settimeout(1.0)
    while True:
        try:
            data, _ = udp.recvfrom(4096)
        except socket.timeout:
            continue
        except OSError:
            continue
        if not data:
            continue
        add_to_buffer(data)
        with CLIENTS_LOCK:
            dead = set()
            for c in CLIENTS:
                try:
                    c.sendall(data)
                except Exception:
                    dead.add(c)
            for c in dead:
                CLIENTS.discard(c)
                try:
                    c.close()
                except Exception:
                    pass
                log.info("Slow/stuck client dropped, %d remaining", len(CLIENTS))


# ---- TCP: serve clients (network) ----
def handle_client(conn, addr):
    global ACTIVE
    with ACTIVE_LOCK:
        if ACTIVE >= MAX_CLIENTS:
            log.warning(
                "Rejected client %s: connection limit reached (%d)",
                addr, MAX_CLIENTS,
            )
            try:
                conn.close()
            except Exception:
                pass
            return
        ACTIVE += 1
    # Bounded write timeout for ALL operations of this client.
    conn.settimeout(SEND_TIMEOUT)
    try:
        if TOKEN:
            conn.settimeout(AUTH_TIMEOUT)
            first = conn.recv(4096)
            conn.settimeout(SEND_TIMEOUT)
            payload = first.strip()
            expected = b"AUTH " + TOKEN.encode()
            if not (payload.startswith(b"AUTH ") and hmac.compare_digest(payload, expected)):
                log.info("Rejected client %s (invalid/missing token)", addr)
                conn.close()
                return
            log.info("Client %s authenticated", addr)
        else:
            log.info("Client connected: %s", addr)

        # Replay the recent window (recovers gaps from network outages)
        earliest = time.time() - REPLAY_ON_CONNECT_SEC
        replayed = snapshot_since(earliest_ts=earliest, limit=MAX_REPLAY_ENTRIES)
        if replayed:
            try:
                conn.sendall(b"".join(replayed))
            except Exception:
                conn.close()
                return
            log.info("Replay sent to %s: %d messages", addr, len(replayed))

        # Join the live broadcast
        with CLIENTS_LOCK:
            CLIENTS.add(conn)
            log.info("Live streaming to %s (%d clients)", addr, len(CLIENTS))

        # Optional control: "REPLAY <seq>" re-sends from that sequence
        while True:
            try:
                chunk = conn.recv(4096)
            except socket.timeout:
                continue
            except Exception:
                break
            if not chunk:
                break
            if chunk.strip().startswith(b"REPLAY "):
                try:
                    seq_from = int(chunk.strip().split()[1])
                    for line in snapshot_since(seq_from=seq_from, limit=MAX_REPLAY_ENTRIES):
                        try:
                            conn.sendall(line)
                        except Exception:
                            raise
                except Exception:
                    pass
            # passive clients (nc, OpenCPN) send nothing: ignore
            time.sleep(0.05)
    except Exception as exc:
        log.info("End of connection %s: %s", addr, exc)
    finally:
        with CLIENTS_LOCK:
            CLIENTS.discard(conn)
        with ACTIVE_LOCK:
            ACTIVE = max(0, ACTIVE - 1)
        try:
            conn.close()
        except Exception:
            pass
        log.info("Client %s disconnected", addr)


def tcp_server():
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((TCP_HOST, TCP_PORT))
    srv.listen(16)
    log.info("TCP listening on %s:%s", TCP_HOST, TCP_PORT)
    while True:
        conn, addr = srv.accept()
        threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()


# ---- Startup ----
def load_disk_log():
    """If LOG_FILE is set, load the ring from entries within RETENTION_SEC."""
    if not LOG_FILE or not os.path.exists(LOG_FILE):
        return
    global _SEQ
    max_seq = 0
    loaded = 0
    try:
        size = os.path.getsize(LOG_FILE)
        tail = max(size - 8 * 1024 * 1024, 0)
        with open(LOG_FILE, "r", encoding="ascii", errors="ignore") as f:
            f.seek(tail)
            if tail:
                f.readline()  # discard partial line
            cutoff = time.time() - RETENTION_SEC
            for ln in f:
                try:
                    e = json.loads(ln)
                except Exception:
                    continue
                if e["ts"] < cutoff:
                    continue
                BUFFER.append((e["seq"], e["ts"], e["line"].encode("ascii")))
                max_seq = max(max_seq, e["seq"])
                loaded += 1
    except Exception as exc:
        log.warning("Failed to load disk log: %s", exc)
    _SEQ = max(max_seq, _SEQ)
    with BUFFER_LOCK:
        while len(BUFFER) > MAX_ENTRIES:
            BUFFER.popleft()
    if loaded:
        log.info("Buffer repopulated from disk: %d messages", loaded)


def _udp_is_loopback():
    """True if the UDP input is on loopback (only local sender)."""
    if UDP_HOST in ("127.0.0.1", "::1", "localhost"):
        return True
    try:
        infos = socket.getaddrinfo(UDP_HOST, UDP_PORT, 0, socket.SOCK_DGRAM)
    except OSError:
        return False
    for _, _, _, _, sockaddr in infos:
        ip = sockaddr[0]
        if ip.startswith("127.") or ip == "::1":
            return True
    return False


def main():
    if not _udp_is_loopback():
        if not UDP_ALLOW_EXTERNAL:
            log.error(
                "UDP input on %s is not loopback and AIS_RELAY_UDP_ALLOW_EXTERNAL is "
                "not enabled. Aborting for security: use 127.0.0.1 or set "
                "AIS_RELAY_UDP_ALLOW_EXTERNAL=1 if you have a remote decoder.",
                UDP_HOST,
            )
            sys.exit(1)
        log.warning(
            "External UDP input enabled on %s:%s — anyone with network access could "
            "inject NMEA. Ensure your firewall is configured.",
            UDP_HOST, UDP_PORT,
        )
    _open_log()
    load_disk_log()
    threading.Thread(target=udp_receiver, daemon=True).start()
    tcp_server()


if __name__ == "__main__":
    main()

