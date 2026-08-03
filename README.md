# ais-relay

Relay de NMEA AIS con **buffer de replay** para estaciones receptoras AIS.
Recibe un stream NMEA por UDP, lo re-sirve por TCP a múltiples clientes y
recupera lo capturado durante un **corte de red** (WiFi, 4G/5G, overlay/VPN)
gracias a un anillo en memoria con replay a la reconexión.

Sirve como puente entre un decodificador AIS (p. ej. **AIS-catcher**) y
cualquier consumidor (OpenCPN, `nc`, scripts, servicios que mantienen estado
por MMSI como una base Redis, etc.) a través de una red privada o pública.

```
SDR AIS ──► decodificador (AIS-catcher) ──UDP :10110──► ais-relay
                                                       ├─ pipe-through en vivo
                                                       ├─ anillo en memoria (N)
                                                       └─ TCP :10110 ──► clientes
```

## Características

- **En vivo, sin rate limit**: cada datagrama NMEA (`!AIVDM`/`!AIVDO`) se
  re-expide al instante a todos los clientes conectados.
- **Replay mínimo a la reconexión**: al conectar, se reenvía una ventana
  pequeña y cautelosa (`REPLAY_ON_CONNECT_SEC`, 30 s por defecto ≈ margen sobre
  la caída) y después se sigue en vivo. No se pierden los datos capturados
  durante un corte.
- **Reenvío exacto bajo demanda**: un cliente puede enviar `REPLAY <seq>` para
  recibir solo los mensajes con número de secuencia mayor (sin duplicados).
- **Deduplicación en el cliente**: el replay no es exacto por defecto; los
  consumidores que mantienen estado (p. ej. por MMSI) filtran duplicados de
  forma idempotente.
- **Auth opcional**: token (`AUTH <token>` como primera línea) para control de
  acceso cuando el filtro de red no es suficiente.
- **Sin dependencias externas**: solo la biblioteca estándar de Python 3.

## Instalación (rápida)

Instala (o actualiza) todo en un solo paso — copia el programa, la unidad y
crea el fichero de configuración único:

```bash
sudo ./deploy/install.sh
sudo nano /etc/ais-relay/ais-relay.conf     # edita tus valores (lugar único)
sudo systemctl restart ais-relay
```

Desinstalar (conserva tu configuración):

```bash
sudo ./deploy/uninstall.sh
```

Alternativa manual (si prefieres hacerlo a mano), en un host Linux
(Debian/Raspberry Pi OS):

```bash
sudo cp ais-relay.py /usr/local/bin/ais-relay.py
sudo chmod +x /usr/local/bin/ais-relay.py
sudo cp deploy/ais-relay.service /etc/systemd/system/ais-relay.service

# Configuración (UN ÚNICO fichero):
sudo mkdir -p /etc/ais-relay
sudo cp deploy/ais-relay.conf.example /etc/ais-relay/ais-relay.conf
sudo nano /etc/ais-relay/ais-relay.conf     # edita tus valores

sudo systemctl daemon-reload
sudo systemctl enable --now ais-relay.service
```

También puede ejecutarse directamente (aquí la configuración se pasa como
variables de entorno del shell, o creando `/etc/ais-relay/ais-relay.conf`):

```bash
python3 ais-relay.py
```


## Configuración

Toda la configuración de tu instalación vive en **un único fichero**:
`/etc/ais-relay/ais-relay.conf` (copia de `deploy/ais-relay.conf.example`).
La unidad systemd lo carga con `EnvironmentFile`. Si no existe, se usan los
valores por defecto del script.

| Variable | Default | Descripción |
|---|---|---|
| `AIS_RELAY_UDP_HOST` | `127.0.0.1` | Host de entrada NMEA por UDP |
| `AIS_RELAY_UDP_PORT` | `10110` | Puerto de entrada UDP |
| `AIS_RELAY_UDP_ALLOW_EXTERNAL` | `0` | `1` permite input UDP remoto (inseguro); por defecto solo loopback |
| `AIS_RELAY_TCP_HOST` | `0.0.0.0` | Host en el que sirve TCP |
| `AIS_RELAY_TCP_PORT` | `10110` | Puerto de salida TCP |
| `AIS_RELAY_RETENTION_SEC` | `3600` | Ventana del anillo en memoria (s) |
| `AIS_RELAY_MAX_ENTRIES` | `200000` | Máx. mensajes retenidos en memoria |
| `AIS_RELAY_REPLAY_ON_CONNECT_SEC` | `30` | Replay mínimo a cada reconexión (s) |
| `AIS_RELAY_TOKEN` | *(vacío)* | Si se define, el cliente debe enviar `AUTH <token>` |
| `AIS_RELAY_LOG_FILE` | *(vacío)* | Persistir el stream a disco (JSONL) y repoblar el buffer al arrancar |
| `AIS_RELAY_MAX_REPLAY_ENTRIES` | `20000` | Tope de mensajes por replay (evita volcar todo el anillo) |
| `AIS_RELAY_LOG_MAX_MB` | `64` | Tamaño máx. del JSONL a disco; al superarlo se rota. Desactivar el log = no definir `LOG_FILE` |
| `AIS_RELAY_LOG_BACKUPS` | `2` | Nº de copias rotadas del log a disco |
| `AIS_RELAY_MAX_CLIENTS` | `64` | Máx. conexiones TCP simultáneas (más allá se rechazan) |
| `AIS_RELAY_SEND_TIMEOUT_SEC` | `5` | Timeout de escritura por cliente; si no drena su buffer se le desconecta |


> Si se define `AIS_RELAY_LOG_FILE`, el anillo se repuebla desde el fichero al
> arrancar, de modo que el replay sobrevive a reinicios del servicio.

## Consumo

- **Cliente pasivo** (OpenCPN, `nc`): conectar y leer. Recibe replay mínimo + vivo.

  ```bash
  nc <host> <port>
  ```

- **Cliente con estado** (p. ej. Redis por MMSI): conectar y, para reenvío
  exacto del hueco, enviar el último número de secuencia procesado:

  ```
  REPLAY <última_seq_procesada>
  ```

  y mantener el stream. (Si el servidor reenvía `@SEQ <n>` como línea de
  control opcional, el cliente aprende los `seq` para futuras reconexiones.)

- Si `AIS_RELAY_TOKEN` está activo, la primera línea debe ser `AUTH <token>`.

## Tests

Conectan contra un `ais-relay` en ejecución (host y puerto configurables por
argumentos de línea de comandos):

```bash
python tests/test_client.py <host> <port> [segundos]   # lee stream en vivo
python tests/test_replay.py   <host> [port]            # verifica el replay tras un corte
```

## Red y control de acceso

Diseñado para funcionar en **red privada** (LAN y/o overlay/VPN). El alcance se
controla con `AIS_RELAY_TCP_HOST`:

- **LAN + VPN (por defecto `0.0.0.0`):** escucha en todas las interfaces, así
  que se alcanza por la red local y por el overlay/VPN a la vez.
- **Solo VPN (recomendado si no hace falta la LAN):** fija `AIS_RELAY_TCP_HOST`
  a la IP del overlay/VPN (p. ej. `10.0.0.5`). La estación queda accesible
  únicamente por esa red.

Opciones de acceso:
- **Por red:** el filtro lo pone la infraestructura (ACL del overlay/VPN o
  firewall). Es el modo por defecto.
- **Por token:** define `AIS_RELAY_TOKEN` y los clientes deberán enviar
  `AUTH <token>` como primera línea.

## Limitaciones y clientes lentos

`ais-relay` es un relay en vivo por defecto: si un cliente deja de leer, su
buffer de socket se llena y, sin protección, **podría bloquear el reenvío a
todos los demás**. Para evitarlo hay límites configurables:

- `AIS_RELAY_SEND_TIMEOUT_SEC` (5 s por defecto): un cliente que no drena su
  buffer en ese tiempo se **desconecta automáticamente**, de modo que un
  cliente atascado no congela al resto.
- `AIS_RELAY_MAX_CLIENTS` (64): máximo de conexiones simultáneas; las que
  superen el límite se rechazan.

Además se acota el **uso de CPU/memoria y el disco**:

- `AIS_RELAY_MAX_REPLAY_ENTRIES` (20000): cualquier replay (reconexión o
  `REPLAY <seq>`) se limita a los N mensajes más recientes, de modo que un
  cliente no puede forzar un volcado completo del anillo una y otra vez.
- El anillo en memoria ya está acotado por `AIS_RELAY_MAX_ENTRIES` +
  `AIS_RELAY_RETENTION_SEC`.
- El JSONL a disco rota al alcanzar `AIS_RELAY_LOG_MAX_MB`, manteniendo
  `AIS_RELAY_LOG_BACKUPS` copias (`base`, `.1`, …): el disco nunca crece sin
  límite.

Para entornos con consumidores que pueden quedarse sin leer, conviene ajustar
`AIS_RELAY_SEND_TIMEOUT_SEC` (más bajo = se desaloja antes al lento). No se
recomienda desactivar estos límites en redes con clientes no controlados.

## Seguridad

- **Sin privilegios:** la unidad systemd usa `DynamicUser=yes`, `PrivateTmp=yes`
  y `NoNewPrivileges=yes` → el servicio **no corre como root** (usuario efímero).
- **Token en tiempo constante:** si usas `AIS_RELAY_TOKEN`, la autenticación se
  compara con `hmac.compare_digest` (no constante de forma ingenua).
- **Input UDP solo loopback:** por defecto la entrada NMEA debe venir de
  `127.0.0.1` (el decodificador local). Si se apunta a otra interfaz, el
  proceso **aborta salvo** que se active `AIS_RELAY_UDP_ALLOW_EXTERNAL=1`
  (para un decodificador remoto) — evita la inyección de datos en la red.
- **CPU del replay acotada:** `snapshot_since` itera con salida temprana sin
  copiar el anillo completo; combinado con `AIS_RELAY_MAX_REPLAY_ENTRIES` evita
  abuso de CPU/memoria.
- **Límites:** clientes lentos (`SEND_TIMEOUT_SEC`), nº de conexiones
  (`MAX_CLIENTS`) y disco (`LOG_MAX_MB`/`LOG_BACKUPS`).

> Nota: el stream va **sin TLS** (NMEA en claro). En una red privada de
> confianza es aceptable; si cruzara redes no confiables, convendría cifrarlo
> (TLS/mTLS) o relegarlo al overlay con ACL — queda como mejora opcional.


## Estructura

```
ais-relay.py                 Servicio principal (sin dependencias externas)
deploy/ais-relay.service     Plantilla de unidad systemd
tests/                       Clientes de prueba
```

## Licencia

MIT — ver [LICENSE](LICENSE).
