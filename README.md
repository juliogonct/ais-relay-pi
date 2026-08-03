# ais-relay-pi

A NMEA AIS relay with a **replay buffer**, designed for AIS receiving stations
whose network link is intermittent (WiFi, 4G/5G, overlay/VPN). It receives an
NMEA stream over UDP, re-serves it over TCP to multiple clients, and recovers
data captured during a network outage thanks to an in-memory ring with replay
on reconnect.

It bridges an AIS decoder (e.g. **AIS-catcher**) and any consumer (OpenCPN,
`nc`, scripts, services that keep per-vessel state such as a Redis store) over
a private or public network.

```
SDR AIS ──► decoder (AIS-catcher) ──UDP :10110──► ais-relay-pi
                                                  ├─ real-time pipe-through
                                                  ├─ in-memory ring (N)
                                                  └─ TCP :10110 ──► clients
```

## Features

- **Real-time, no rate limiting**: every NMEA datagram (`!AIVDM`/`!AIVDO`) is
  forwarded instantly to all connected clients.
- **Minimal replay on reconnect**: on connect, a small, cautious window of the
  buffer is re-sent (`REPLAY_ON_CONNECT_SEC`, 30 s by default ≈ margin over the
  outage) and then the live stream continues. Data captured during an outage is
  not lost.
- **Bounded replay on demand**: a stateful client can send `REPLAY <seq>` to
  request messages after a sequence marker. The request is rate-limited and
  bounded by both message count and bytes. NMEA remains unchanged, so clients
  that do not receive sequence metadata should use client-side deduplication.
- **Client-side deduplication**: the replay is not exact by default; consumers
  that keep state (e.g. by MMSI) filter duplicates idempotently.
- **Optional auth**: token (`AUTH <token>` as the first line) when network-only
  access control is not enough.
- **Hardened**: bounded replay (CPU/memory), bounded connections and slow-client
  handling, disk-log rotation, and runs without privileges.
- **No external dependencies**: only the Python 3 standard library.

## Quick install

Install (or update) everything in one step — copies the program, the unit and
creates the single configuration file:

```bash
sudo ./deploy/install.sh
sudo nano /etc/ais-relay/ais-relay.conf     # edit your values (single place)
sudo systemctl restart ais-relay
```

Uninstall (keeps your configuration):

```bash
sudo ./deploy/uninstall.sh
```

Manual alternative (Debian / Raspberry Pi OS):

```bash
sudo cp ais-relay.py /usr/local/bin/ais-relay.py
sudo chmod +x /usr/local/bin/ais-relay.py
sudo cp deploy/ais-relay.service /etc/systemd/system/ais-relay.service

# Configuration (SINGLE file):
sudo mkdir -p /etc/ais-relay
sudo cp deploy/ais-relay.conf.example /etc/ais-relay/ais-relay.conf
sudo nano /etc/ais-relay/ais-relay.conf     # edit your values

sudo systemctl daemon-reload
sudo systemctl enable --now ais-relay.service
```

It can also run directly. In that mode, provide configuration through shell
environment variables or set `AIS_RELAY_CONFIG_FILE` to a readable config
file; the systemd unit is the recommended way to use the root-owned
`/etc/ais-relay/ais-relay.conf`.

```bash
python3 ais-relay.py
```

## Configuration

All configuration for your installation lives in **a single file**:
`/etc/ais-relay/ais-relay.conf` (a copy of `deploy/ais-relay.conf.example`).
The systemd unit loads it with an `EnvironmentFile`. If it is missing, the
script defaults are used.

| Variable | Default | Description |
|---|---|---|
| `AIS_RELAY_UDP_HOST` | `127.0.0.1` | UDP input host (your NMEA decoder) |
| `AIS_RELAY_UDP_PORT` | `10110` | UDP input port |
| `AIS_RELAY_UDP_ALLOW_EXTERNAL` | `0` | `1` allows a remote UDP input (insecure); default loopback only |
| `AIS_RELAY_TCP_HOST` | `0.0.0.0` | TCP listen host |
| `AIS_RELAY_TCP_PORT` | `10110` | TCP output port |
| `AIS_RELAY_RETENTION_SEC` | `3600` | In-memory ring window (s) |
| `AIS_RELAY_MAX_ENTRIES` | `200000` | Max messages kept in memory |
| `AIS_RELAY_REPLAY_ON_CONNECT_SEC` | `30` | Minimal replay on each reconnect (s) |
| `AIS_RELAY_TOKEN` | *(empty)* | If set, clients must send `AUTH <token>` |
| `AIS_RELAY_AUTH_TIMEOUT` | `5` | Authentication/control read timeout (s) |
| `AIS_RELAY_LOG_FILE` | *(empty)* | Persist the stream to disk (JSONL) and repopulate the buffer on startup |
| `AIS_RELAY_MAX_REPLAY_ENTRIES` | `20000` | Max messages per replay (prevents dumping the whole ring) |
| `AIS_RELAY_MAX_BUFFER_BYTES` | `67108864` | Max in-memory ring payload (bytes) |
| `AIS_RELAY_MAX_REPLAY_BYTES` | `2097152` | Max payload per replay (bytes) |
| `AIS_RELAY_MAX_CLIENT_QUEUE_BYTES` | `2097152` | Max queued payload per client (bytes) |
| `AIS_RELAY_MAX_AUTH_LINE_BYTES` | `4096` | Max authentication line size |
| `AIS_RELAY_MAX_REPLAY_REQUESTS_PER_MINUTE` | `30` | Replay requests allowed per client/minute |
| `AIS_RELAY_LOG_MAX_MB` | `64` | Max size (MB) of the disk JSONL before rotation. Disable by leaving `LOG_FILE` empty |
| `AIS_RELAY_LOG_BACKUPS` | `2` | Rotated disk-log copies |
| `AIS_RELAY_MAX_CLIENTS` | `64` | Max simultaneous TCP connections (beyond is rejected) |
| `AIS_RELAY_SEND_TIMEOUT_SEC` | `5` | Per-client write timeout; clients that do not drain are disconnected |

> If `AIS_RELAY_LOG_FILE` is set, the ring is repopulated from the file at
> startup, so the replay survives service restarts.

## Consumption

- **Passive client** (OpenCPN, `nc`): connect and read. You receive the minimal
  replay + the live stream.

  ```bash
  nc <host> <port>
  ```

- **Stateful client** (e.g. Redis by MMSI): if it has a sequence marker from a
  trusted side channel, it can request a bounded gap replay:

  ```
  REPLAY <last_processed_seq>
  ```

  and keep the stream open. The relay does not add sequence metadata to NMEA;
  ordinary clients should use the minimal time replay plus client-side
  deduplication.

- If `AIS_RELAY_TOKEN` is active, the first line must be `AUTH <token>`.

## Tests

Connect to a running `ais-relay` (host and port configurable via CLI args):

```bash
python tests/test_client.py <host> <port> [seconds]   # read live stream
python tests/test_replay.py   <host> [port]            # verify replay after a cut
```


## Network and access control

Designed to run on a **private network** (LAN and/or overlay/VPN). The scope is
controlled with `AIS_RELAY_TCP_HOST`:

- **LAN + VPN (default `0.0.0.0`):** listens on all interfaces, reachable from
  the local network and the overlay/VPN at the same time.
- **VPN only (recommended if LAN is not needed):** set `AIS_RELAY_TCP_HOST` to
  the overlay/VPN IP (e.g. `10.0.0.5`). The station is then reachable only over
  that network.

Access options:
- **By network:** the filter is set by the infrastructure (overlay ACL or
  firewall). This is the default.
- **By token:** set `AIS_RELAY_TOKEN` and clients must send `AUTH <token>` as
  the first line. Authentication is line-framed and bounded.

## Limits and slow clients

`ais-relay` is a live relay by default: if a client stops reading, its socket
buffer fills and, without protection, it **could stall the forwarding to
everyone else**. Configurable limits prevent this:

- `AIS_RELAY_SEND_TIMEOUT_SEC` (5 s default): a client that does not drain its
  buffer is **disconnected automatically**, so a stuck client does not freeze
  the rest.
- `AIS_RELAY_MAX_CLIENTS` (64): maximum simultaneous connections; beyond that
  they are rejected.

CPU/memory and disk are also bounded:

- `AIS_RELAY_MAX_REPLAY_ENTRIES` (20000) and `AIS_RELAY_MAX_REPLAY_BYTES`
  (2 MiB): any replay (reconnect or `REPLAY <seq>`) is capped by both count and
  bytes, so a client cannot force a large ring dump over and over.
- The in-memory ring is bounded by `AIS_RELAY_MAX_ENTRIES` +
  `AIS_RELAY_RETENTION_SEC` + `AIS_RELAY_MAX_BUFFER_BYTES`.
- Each client has a bounded output queue (`AIS_RELAY_MAX_CLIENT_QUEUE_BYTES`).
  A slow client is dropped without blocking UDP ingestion or other clients.
- Replay requests are rate-limited by
  `AIS_RELAY_MAX_REPLAY_REQUESTS_PER_MINUTE`.
- The disk JSONL rotates at `AIS_RELAY_LOG_MAX_MB`, keeping
  `AIS_RELAY_LOG_BACKUPS` copies (`base`, `.1`, …): disk never grows unbounded.

For environments with consumers that may stop reading, lower
`AIS_RELAY_SEND_TIMEOUT_SEC` (lower = slow clients are evicted sooner). It is
not recommended to disable these limits on networks with uncontrolled clients.

## Security

- **Runs without privileges:** the systemd unit uses `DynamicUser=yes`,
  `PrivateTmp=yes` and `NoNewPrivileges=yes` — the service is **not root**.
- **Constant-time token:** if `AIS_RELAY_TOKEN` is used, authentication is
  compared with `hmac.compare_digest` (not naively).
- **Loopback-only UDP input:** by default the NMEA input must come from
  `127.0.0.1` (the local decoder). Pointing elsewhere makes the process
  **abort** unless `AIS_RELAY_UDP_ALLOW_EXTERNAL=1` (remote decoder) — this
  prevents data injection on the network.
- **Bounded replay CPU:** `snapshot_since` iterates with early exit without
  copying the whole ring; combined with `AIS_RELAY_MAX_REPLAY_ENTRIES` it avoids
  CPU/memory abuse.
- **Limits:** slow clients (`SEND_TIMEOUT_SEC`), connection count
  (`MAX_CLIENTS`), memory/replay bytes and disk (`LOG_MAX_MB`/`LOG_BACKUPS`).
- **Systemd resource boundaries:** the unit applies `TasksMax`, `MemoryMax`,
  `CPUQuota`, `ProtectHome`, `ProtectSystem`, `PrivateDevices`, restricted
  address families, `RestrictRealtime`, `RestrictSUIDSGID`, and
  `LockPersonality`. It also creates `/var/lib/ais-relay` as the recommended
  state directory for optional disk logging.

> Note: the stream is **without TLS** (plain NMEA). On a trusted private
> network this is acceptable; if it ever crosses an untrusted network, consider
> encrypting (TLS/mTLS) or restricting it to the overlay with an ACL — this is
> an optional improvement.

The unit also rate-limits journald messages per service. The JSONL AIS stream,
when enabled, is separate from journald and is rotated by the relay.

## Project structure

```
ais-relay.py                 Main service (no external dependencies)
deploy/ais-relay.service     systemd unit template
deploy/ais-relay.conf.example     Single configuration file template
deploy/install.sh            Installer
deploy/uninstall.sh          Uninstaller
tests/                       Test clients
```

### Direct execution

When running `python3 ais-relay.py` directly, the program reads
`/etc/ais-relay/ais-relay.conf` itself unless variables are already present in
the environment. The systemd unit also supplies the same file through
`EnvironmentFile`.

## License

MIT — see [LICENSE](LICENSE).

