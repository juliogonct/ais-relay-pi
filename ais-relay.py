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


def _load_config_file(path="/etc/ais-relay/ais-relay.conf"):
    """Load simple KEY=value configuration without overriding the environment."""
    if not os.path.exists(path):
        return
    try:
        with open(path, "r", encoding="utf-8") as config:
            for raw in config:
                line = raw.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip()
                if key and key.startswith("AIS_RELAY_") and key not in os.environ:
                    os.environ[key] = value
    except PermissionError:
        # systemd may have loaded the root-owned EnvironmentFile before dropping
        # privileges; in that case the process itself need not read it again.
        return
    except OSError as exc:
        print(f"Warning: unable to read configuration file {path}: {exc}", file=sys.stderr)


def _env(name, default, cast=str):
    v = os.environ.get(name)
    return default if v is None else cast(v)


# Configuration (overridable via environment variables / systemd EnvironmentFile)
_load_config_file(os.environ.get("AIS_RELAY_CONFIG_FILE", "/etc/ais-relay/ais-relay.conf"))
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
MAX_BUFFER_BYTES = int(_env("AIS_RELAY_MAX_BUFFER_BYTES", str(64 * 1024 * 1024)))
MAX_REPLAY_BYTES = int(_env("AIS_RELAY_MAX_REPLAY_BYTES", str(2 * 1024 * 1024)))
MAX_CLIENT_QUEUE_BYTES = int(_env("AIS_RELAY_MAX_CLIENT_QUEUE_BYTES", str(2 * 1024 * 1024)))
MAX_AUTH_LINE_BYTES = int(_env("AIS_RELAY_MAX_AUTH_LINE_BYTES", "4096"))
MAX_REPLAY_REQUESTS_PER_MINUTE = int(_env("AIS_RELAY_MAX_REPLAY_REQUESTS_PER_MINUTE", "30"))
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
BUFFER_BYTES = 0
CLIENTS = set()          # connected TCP sockets
CLIENTS_LOCK = threading.Lock()
ACTIVE = 0               # active connections (for the client limit)
ACTIVE_LOCK = threading.Lock()
LOG_FH = None            # optional JSONL file handle
_log_bytes = 0           # bytes written to the disk log (for rotation)
UDP_FAILED = threading.Event()
UDP_READY = threading.Event()


class ClientState:
    """A bounded per-client queue and sender, keeping UDP ingestion non-blocking."""

    def __init__(self, conn, addr):
        self.conn = conn
        self.addr = addr
        self.queue = deque()
        self.queue_bytes = 0
        self.pending = deque()
        self.pending_bytes = 0
        self.condition = threading.Condition()
        self.closed = False
        self.ready = False
        self.replay_requests = deque()

    def enqueue(self, seq, data):
        with self.condition:
            if self.closed:
                return False
            if self.queue_bytes + len(data) > MAX_CLIENT_QUEUE_BYTES:
                self.closed = True
                self.condition.notify_all()
                return False
            self.queue.append((seq, data))
            self.queue_bytes += len(data)
            self.condition.notify()
            return True

    def enqueue_live(self, seq, data):
        with self.condition:
            if self.closed:
                return False
            if not self.ready:
                if self.pending_bytes + len(data) > MAX_CLIENT_QUEUE_BYTES:
                    self.closed = True
                    self.condition.notify_all()
                    return False
                self.pending.append((seq, data))
                self.pending_bytes += len(data)
                return True
            return self._enqueue_ready(seq, data)

    def _enqueue_ready(self, seq, data):
        if self.queue_bytes + len(data) > MAX_CLIENT_QUEUE_BYTES:
            self.closed = True
            self.condition.notify_all()
            return False
        self.queue.append((seq, data))
        self.queue_bytes += len(data)
        self.condition.notify()
        return True

    def activate(self):
        with self.condition:
            if self.closed:
                return False
            if self.queue_bytes + self.pending_bytes > MAX_CLIENT_QUEUE_BYTES:
                self.closed = True
                self.condition.notify_all()
                return False
            self.queue.extend(self.pending)
            self.queue_bytes += self.pending_bytes
            self.pending.clear()
            self.pending_bytes = 0
            self.ready = True
            self.condition.notify()
            return True

    def seed(self, items):
        """Place the replay before the client becomes visible to live broadcast."""
        with self.condition:
            if self.closed:
                return False
            replay_bytes = sum(len(data) for _, data in items)
            if replay_bytes + self.pending_bytes > MAX_CLIENT_QUEUE_BYTES:
                self.closed = True
                self.condition.notify_all()
                return False
            pending_live = self.pending
            self.pending = deque(items)
            self.pending.extend(pending_live)
            self.pending_bytes += replay_bytes
            return True

    def pop(self):
        with self.condition:
            while not self.queue and not self.closed:
                self.condition.wait(1.0)
            if not self.queue:
                return None
            item = self.queue.popleft()
            self.queue_bytes -= len(item[1])
            return item

    def close(self):
        with self.condition:
            self.closed = True
            self.condition.notify_all()
        try:
            self.conn.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            self.conn.close()
        except OSError:
            pass

    def send_direct(self, data):
        self.conn.settimeout(SEND_TIMEOUT)
        self.conn.sendall(data)


def next_seq():
    global _SEQ
    with _SEQ_LOCK:
        _SEQ += 1
        return _SEQ


def validate_config():
    """Reject unsafe or nonsensical limits before opening network sockets."""
    positive = {
        "AIS_RELAY_UDP_PORT": UDP_PORT,
        "AIS_RELAY_TCP_PORT": TCP_PORT,
        "AIS_RELAY_RETENTION_SEC": RETENTION_SEC,
        "AIS_RELAY_MAX_ENTRIES": MAX_ENTRIES,
        "AIS_RELAY_MAX_REPLAY_ENTRIES": MAX_REPLAY_ENTRIES,
        "AIS_RELAY_MAX_BUFFER_BYTES": MAX_BUFFER_BYTES,
        "AIS_RELAY_MAX_REPLAY_BYTES": MAX_REPLAY_BYTES,
        "AIS_RELAY_MAX_CLIENT_QUEUE_BYTES": MAX_CLIENT_QUEUE_BYTES,
        "AIS_RELAY_MAX_AUTH_LINE_BYTES": MAX_AUTH_LINE_BYTES,
        "AIS_RELAY_MAX_CLIENTS": MAX_CLIENTS,
        "AIS_RELAY_SEND_TIMEOUT_SEC": SEND_TIMEOUT,
        "AIS_RELAY_MAX_REPLAY_REQUESTS_PER_MINUTE": MAX_REPLAY_REQUESTS_PER_MINUTE,
    }
    for name, value in positive.items():
        if value <= 0:
            raise ValueError(f"{name} must be greater than zero")
    if REPLAY_ON_CONNECT_SEC < 0:
        raise ValueError("AIS_RELAY_REPLAY_ON_CONNECT_SEC must not be negative")
    if AUTH_TIMEOUT <= 0:
        raise ValueError("AIS_RELAY_AUTH_TIMEOUT must be greater than zero")
    if LOG_MAX_MB < 0:
        raise ValueError("AIS_RELAY_LOG_MAX_MB must not be negative")
    if LOG_BACKUPS < 0:
        raise ValueError("AIS_RELAY_LOG_BACKUPS must not be negative")


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
    if LOG_BACKUPS == 0:
        LOG_FH = open(base, "w", buffering=1)
        _log_bytes = 0
        log.info("Disk log truncated at max %.0f MB", LOG_MAX_MB)
        return
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
    global BUFFER_BYTES
    now = time.time()
    seq = next_seq()
    with BUFFER_LOCK:
        BUFFER.append((seq, now, line))
        BUFFER_BYTES += len(line)
        while BUFFER and (
            now - BUFFER[0][1] > RETENTION_SEC
            or len(BUFFER) > MAX_ENTRIES
            or BUFFER_BYTES > MAX_BUFFER_BYTES
        ):
            _, _, old_line = BUFFER.popleft()
            BUFFER_BYTES -= len(old_line)
    _append_disk(seq, now, line)
    return seq


def snapshot_since(earliest_ts=None, seq_from=None, limit=None, max_bytes=None):
    """Return the N most recent lines (bytes) filtered by time/sequence.

    Iterate the ring in reverse with early exit, without copying the whole
    buffer, to avoid CPU/memory spikes on repeated replays.
    """
    out_rev = []
    total_bytes = 0
    with BUFFER_LOCK:
        for seq, ts, line in reversed(BUFFER):
            if earliest_ts is not None and ts < earliest_ts:
                break  # older behind; no longer applies
            if seq_from is not None and seq < seq_from:
                break
            if max_bytes is not None and total_bytes + len(line) > max_bytes:
                break
            out_rev.append((seq, line))
            total_bytes += len(line)
            if limit is not None and len(out_rev) >= limit:
                break
    out_rev.reverse()
    return out_rev


def broadcast(seq, data):
    """Queue one message for every client without blocking UDP ingestion."""
    dead = []
    with CLIENTS_LOCK:
        for client in tuple(CLIENTS):
            if not client.enqueue_live(seq, data):
                dead.append(client)
        for client in dead:
            CLIENTS.discard(client)
    for client in dead:
        log.info("Slow/stuck client dropped, %d remaining", len(CLIENTS))
        client.close()


def remove_client(client):
    with CLIENTS_LOCK:
        CLIENTS.discard(client)
    client.close()


# ---- UDP: ingest NMEA from the AIS decoder ----
def udp_receiver():
    try:
        udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        udp.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        udp.bind((UDP_HOST, UDP_PORT))
        log.info("UDP listening on %s:%s", UDP_HOST, UDP_PORT)
        UDP_READY.set()
        udp.settimeout(1.0)
        while True:
            try:
                data, _ = udp.recvfrom(4096)
            except socket.timeout:
                continue
            if not data:
                continue
            seq = add_to_buffer(data)
            broadcast(seq, data)
    except Exception:
        log.exception("UDP receiver stopped")
        UDP_FAILED.set()
        raise


# ---- TCP: serve clients (network) ----
def _read_auth_line(conn):
    conn.settimeout(AUTH_TIMEOUT)
    data = bytearray()
    while len(data) < MAX_AUTH_LINE_BYTES:
        chunk = conn.recv(min(256, MAX_AUTH_LINE_BYTES - len(data)))
        if not chunk:
            return None
        data.extend(chunk)
        if b"\n" in data:
            line, remainder = bytes(data).split(b"\n", 1)
            return line.rstrip(b"\r"), remainder
    return None, b""


def _sender_loop(client):
    try:
        while True:
            item = client.pop()
            if item is None:
                return
            _, data = item
            client.send_direct(data)
    except (OSError, socket.timeout):
        client.close()


def handle_client(conn, addr):
    global ACTIVE
    client = ClientState(conn, addr)
    control_buffer = bytearray()
    try:
        if TOKEN:
            payload, remainder = _read_auth_line(conn)
            control_buffer.extend(remainder)
            expected = b"AUTH " + TOKEN.encode()
            if payload is None or not hmac.compare_digest(payload, expected):
                log.info("Rejected client %s (invalid/missing token)", addr)
                return
            log.info("Client %s authenticated", addr)
        else:
            log.info("Client connected: %s", addr)

        # Register as a pending client before taking the replay snapshot. New
        # live messages are queued while the replay is being prepared, so none
        # can fall between the snapshot and live stream.
        with CLIENTS_LOCK:
            CLIENTS.add(client)
            log.info("Client pending replay: %s (%d clients)", addr, len(CLIENTS))

        # Replay the recent window (recovers gaps from network outages)
        earliest = time.time() - REPLAY_ON_CONNECT_SEC
        replayed = snapshot_since(
            earliest_ts=earliest,
            limit=MAX_REPLAY_ENTRIES,
            max_bytes=MAX_REPLAY_BYTES,
        )

        # Seed replay before activation; pending live messages remain ordered
        # after the replay.
        if not client.seed(replayed):
            return
        sender = threading.Thread(target=_sender_loop, args=(client,), daemon=True)
        sender.start()
        if not client.activate():
            return
        log.info("Replay sent to %s: %d messages", addr, len(replayed))

        # Optional control: "REPLAY <seq>" re-sends from that sequence
        conn.settimeout(AUTH_TIMEOUT)
        while True:
            try:
                chunk = conn.recv(4096)
            except socket.timeout:
                continue
            except Exception:
                break
            if not chunk:
                break
            control_buffer.extend(chunk)
            if len(control_buffer) > MAX_AUTH_LINE_BYTES:
                log.warning("Control line too long from %s", addr)
                return
            while b"\n" in control_buffer:
                raw_line, remainder = bytes(control_buffer).split(b"\n", 1)
                control_buffer = bytearray(remainder)
                line = raw_line.strip()
                if not line.startswith(b"REPLAY "):
                    continue
                try:
                    now = time.time()
                    while client.replay_requests and now - client.replay_requests[0] >= 60:
                        client.replay_requests.popleft()
                    if len(client.replay_requests) >= MAX_REPLAY_REQUESTS_PER_MINUTE:
                        log.warning("Replay request limit reached for %s", addr)
                        continue
                    client.replay_requests.append(now)
                    seq_from = int(line.split()[1]) + 1
                    for seq, replay_line in snapshot_since(
                        seq_from=seq_from,
                        limit=MAX_REPLAY_ENTRIES,
                        max_bytes=MAX_REPLAY_BYTES,
                    ):
                        if not client.enqueue(seq, replay_line):
                            return
                except (ValueError, IndexError):
                    log.warning("Invalid replay command from %s", addr)
            # passive clients (nc, OpenCPN) send nothing: ignore
            time.sleep(0.05)
    except Exception as exc:
        log.info("End of connection %s: %s", addr, exc)
    finally:
        with CLIENTS_LOCK:
            CLIENTS.discard(client)
        with ACTIVE_LOCK:
            ACTIVE = max(0, ACTIVE - 1)
        client.close()
        log.info("Client %s disconnected", addr)


def tcp_server():
    global ACTIVE
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((TCP_HOST, TCP_PORT))
    srv.listen(16)
    srv.settimeout(1.0)
    log.info("TCP listening on %s:%s", TCP_HOST, TCP_PORT)
    while True:
        if UDP_FAILED.is_set():
            raise RuntimeError("UDP receiver stopped")
        try:
            conn, addr = srv.accept()
        except socket.timeout:
            continue
        with ACTIVE_LOCK:
            if ACTIVE >= MAX_CLIENTS:
                conn.close()
                log.warning("Rejected client %s: connection limit reached (%d)", addr, MAX_CLIENTS)
                continue
            ACTIVE += 1
        threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()


# ---- Startup ----
def load_disk_log():
    """If LOG_FILE is set, load the ring from entries within RETENTION_SEC."""
    if not LOG_FILE or not os.path.exists(LOG_FILE):
        return
    global _SEQ, BUFFER_BYTES
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
                line = e["line"].encode("ascii")
                BUFFER.append((e["seq"], e["ts"], line))
                BUFFER_BYTES += len(line)
                max_seq = max(max_seq, e["seq"])
                loaded += 1
    except Exception as exc:
        log.warning("Failed to load disk log: %s", exc)
    _SEQ = max(max_seq, _SEQ)
    with BUFFER_LOCK:
        while BUFFER and (len(BUFFER) > MAX_ENTRIES or BUFFER_BYTES > MAX_BUFFER_BYTES):
            _, _, line = BUFFER.popleft()
            BUFFER_BYTES -= len(line)
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
    try:
        validate_config()
    except ValueError as exc:
        log.error("Invalid configuration: %s", exc)
        sys.exit(2)
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
    udp_thread = threading.Thread(target=udp_receiver, name="udp-receiver", daemon=True)
    udp_thread.start()
    if not UDP_READY.wait(5.0) or UDP_FAILED.is_set():
        log.error("UDP receiver did not become ready")
        sys.exit(1)
    tcp_server()


if __name__ == "__main__":
    main()

