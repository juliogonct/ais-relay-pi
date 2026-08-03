#!/usr/bin/env python3
"""ais-relay: NMEA UDP -> ring buffer -> TCP (network clients).

Recibe un stream NMEA (p. ej. el que genera AIS-catcher) por UDP, mantiene un
buffer circular en memoria (y opcionalmente a disco) y lo re-sirve por TCP a
cualquier cliente de red que se conecte. Pensado para estaciones receptoras AIS
cuyo enlace de red es intermitente (WiFi, 4G/5G): los datos capturados en
local no se pierden durante un corte.

Comportamiento:
  * Pipe-through en tiempo real: cada datagrama NMEA se re-expide al instante a
    los clientes conectados (sin rate limit).
  * Replay a la reconexión: al aceptarse una conexión nueva se re-expide una
    ventana mínima y cautelosa del buffer (REPLAY_ON_CONNECT_SEC, por defecto
    30 s, ~ margen sobre la caída) y después se sigue en vivo. Así, lo capturado
    durante un corte de red (p. ej. WiFi/5G) se recupera al volver la conexión.
  * El reenvío es SOLO el gap + margen pequeño; NO pretende ser exacto. Los
    duplicados resultantes se filtran en el CLIENTE (idempotencia: p. ej. por
    MMSI en un consumidor que mantiene estado por buque). Para reenvío exacto
    desde el último marcador de un cliente, este puede enviar "REPLAY <seq>" y
    se re-expiden solo los mensajes con número de secuencia mayor (sin dup).

Acceso:
  * Por defecto se confía en el control de acceso de la red (ACL del overlay /
    firewall). El servidor escucha en TCP_HOST/TCP_PORT.
  * Opcional: si AIS_RELAY_TOKEN está definido, el cliente debe enviar como
    primera línea "AUTH <token>\n" antes de recibir datos.
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


# Configuración (sobreescribible por variables de entorno en la unidad systemd)
UDP_HOST = _env("AIS_RELAY_UDP_HOST", "127.0.0.1")
UDP_PORT = int(_env("AIS_RELAY_UDP_PORT", "10110"))
# Seguridad: la entrada UDP debe ser loopback (solo el decodificador local).
# Para permitir un decodificador remoto, fija explícitamente 1 (inseguro por defecto).
UDP_ALLOW_EXTERNAL = _env("AIS_RELAY_UDP_ALLOW_EXTERNAL", "0") == "1"
TCP_HOST = _env("AIS_RELAY_TCP_HOST", "0.0.0.0")
TCP_PORT = int(_env("AIS_RELAY_TCP_PORT", "10110"))
RETENTION_SEC = float(_env("AIS_RELAY_RETENTION_SEC", "3600"))   # anillo en memoria
MAX_ENTRIES = int(_env("AIS_RELAY_MAX_ENTRIES", "200000"))
REPLAY_ON_CONNECT_SEC = float(_env("AIS_RELAY_REPLAY_ON_CONNECT_SEC", "30"))
TOKEN = _env("AIS_RELAY_TOKEN", "")
AUTH_TIMEOUT = float(_env("AIS_RELAY_AUTH_TIMEOUT", "5"))
LOG_FILE = _env("AIS_RELAY_LOG_FILE", "")   # "" = sin persistencia a disco
# Límites (protección frente a clientes lentos/atascados y abuso de conexiones):
MAX_CLIENTS = int(_env("AIS_RELAY_MAX_CLIENTS", "64"))
# Timeout de escritura por socket: si un cliente no drena su buffer en este
# tiempo, se le desconecta (evita que un cliente congelado bloquee a los demás).
SEND_TIMEOUT = float(_env("AIS_RELAY_SEND_TIMEOUT_SEC", "5"))
# Acotes de replay y log a disco (evitan abuso de CPU/memoria y disco ilimitado):
MAX_REPLAY_ENTRIES = int(_env("AIS_RELAY_MAX_REPLAY_ENTRIES", "20000"))
# Tamaño máximo (MB) del JSONL a disco; al superarlo se rota. 0 = sin volcado.
LOG_MAX_MB = float(_env("AIS_RELAY_LOG_MAX_MB", "64"))
LOG_BACKUPS = int(_env("AIS_RELAY_LOG_BACKUPS", "2"))

# ---- Estado global ----
_SEQ = 0
_SEQ_LOCK = threading.Lock()
# Anillo: deque de tuplas (seq, timestamp, bytes_linea)
BUFFER = deque()
BUFFER_LOCK = threading.Lock()
CLIENTS = set()          # sockets TCP conectados
CLIENTS_LOCK = threading.Lock()
ACTIVE = 0               # conexiones activas (para el límite de clientes)
ACTIVE_LOCK = threading.Lock()
LOG_FH = None            # manejador opcional de fichero JSONL
_log_bytes = 0           # bytes acumulados del log a disco (para rotación)


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
            log.info("Log a disco activo: %s", LOG_FILE)


def _rotate_log():
    """Rota el JSONL a disco (tamaño max) manteniendo LOG_BACKUPS copias."""
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
    log.info("Log a disco rotado (max %.0f MB, %d backups)", LOG_MAX_MB, LOG_BACKUPS)


def _append_disk(seq, ts, line):
    """Persiste (opcional) cada mensaje como JSONL en disco, con rotación por tamaño."""
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
    except Exception as exc:  # no debe romper el flujo
        log.warning("fallo al escribir log a disco: %s", exc)


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
    """Devuelve las N líneas más recientes (bytes) filtradas por tiempo/seq.

    Itera el anillo en orden inverso con salida temprana, sin copiar el buffer
    completo, para no disparar CPU/memoria en replays repetidos.
    """
    out_rev = []
    with BUFFER_LOCK:
        for seq, ts, line in reversed(BUFFER):
            if earliest_ts is not None and ts < earliest_ts:
                break  # más atrás = más viejo; ya no aplica
            if seq_from is not None and seq < seq_from:
                break
            out_rev.append(line)
            if limit is not None and len(out_rev) >= limit:
                break
    out_rev.reverse()
    return out_rev


# ---- UDP: entra el NMEA de AIS-catcher ----
def udp_receiver():
    udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    udp.bind((UDP_HOST, UDP_PORT))
    log.info("UDP escuchando en %s:%s", UDP_HOST, UDP_PORT)
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
                log.info("Cliente lento/atascado desconectado, restan %d", len(CLIENTS))


# ---- TCP: sirve a clientes (tailnet) ----
def handle_client(conn, addr):
    global ACTIVE
    with ACTIVE_LOCK:
        if ACTIVE >= MAX_CLIENTS:
            log.warning(
                "Rechazado cliente %s: límite de conexiones alcanzado (%d)",
                addr, MAX_CLIENTS,
            )
            try:
                conn.close()
            except Exception:
                pass
            return
        ACTIVE += 1
    # Timeout de escritura acotado en TODAS las operaciones de este cliente.
    conn.settimeout(SEND_TIMEOUT)
    try:
        if TOKEN:
            conn.settimeout(AUTH_TIMEOUT)
            first = conn.recv(4096)
            conn.settimeout(SEND_TIMEOUT)
            payload = first.strip()
            expected = b"AUTH " + TOKEN.encode()
            if not (payload.startswith(b"AUTH ") and hmac.compare_digest(payload, expected)):
                log.info("Rechazado cliente %s (token invalido/ausente)", addr)
                conn.close()
                return
            log.info("Cliente %s autenticado", addr)
        else:
            log.info("Cliente conectado: %s", addr)

        # Replay de la ventana reciente (recupera huecos por cortes de red)
        earliest = time.time() - REPLAY_ON_CONNECT_SEC
        replayed = snapshot_since(earliest_ts=earliest, limit=MAX_REPLAY_ENTRIES)
        if replayed:
            try:
                conn.sendall(b"".join(replayed))
            except Exception:
                conn.close()
                return
            log.info("Replay enviado a %s: %d mensajes", addr, len(replayed))

        # Alta en el broadcast en vivo
        with CLIENTS_LOCK:
            CLIENTS.add(conn)
            log.info("Transmitiendo en vivo a %s (%d clientes)", addr, len(CLIENTS))

        # Control opcional: "REPLAY <seq>" re-expide desde esa secuencia
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
            # clientes pasivos (nc, OpenCPN) no envían nada: se ignoran
            time.sleep(0.05)
    except Exception as exc:
        log.info("Fin de conexion %s: %s", addr, exc)
    finally:
        with CLIENTS_LOCK:
            CLIENTS.discard(conn)
        with ACTIVE_LOCK:
            ACTIVE = max(0, ACTIVE - 1)
        try:
            conn.close()
        except Exception:
            pass
        log.info("Cliente %s desconectado", addr)


def tcp_server():
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((TCP_HOST, TCP_PORT))
    srv.listen(16)
    log.info("TCP escuchando en %s:%s", TCP_HOST, TCP_PORT)
    while True:
        conn, addr = srv.accept()
        threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()


# ---- Arranque ----
def load_disk_log():
    """Si hay LOG_FILE, carga en el anillo las entradas dentro de RETENTION_SEC."""
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
                f.readline()  # descartar línea parcial
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
        log.warning("No se pudo cargar log a disco: %s", exc)
    _SEQ = max(max_seq, _SEQ)
    with BUFFER_LOCK:
        while len(BUFFER) > MAX_ENTRIES:
            BUFFER.popleft()
    if loaded:
        log.info("Buffer repoblado desde disco: %d mensajes", loaded)


def _udp_is_loopback():
    """True si la entrada UDP está en loopback (único remitente local)."""
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
                "Entrada UDP en %s no es loopback y AIS_RELAY_UDP_ALLOW_EXTERNAL "
                "no está activado. Aborto por seguridad: usa 127.0.0.1 o fija "
                "AIS_RELAY_UDP_ALLOW_EXTERNAL=1 si es un decodificador remoto.",
                UDP_HOST,
            )
            sys.exit(1)
        log.warning(
            "Entrada UDP externa habilitada en %s:%s — cualquiera con acceso a "
            "la red podría inyectar NMEA. Asegúrate del firewall.",
            UDP_HOST, UDP_PORT,
        )
    _open_log()
    load_disk_log()
    threading.Thread(target=udp_receiver, daemon=True).start()
    tcp_server()


if __name__ == "__main__":
    main()

