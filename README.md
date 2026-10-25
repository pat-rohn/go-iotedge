# go-iotedge
Simple IoT-Server for receiving and storing timeseries

Example for a client can be found [here](https://github.com/pat-rohn/esp-enlightened)

## Version

`GET /version` returns the build date and Git commit hash as JSON:

```json
{
  "date": "2026-10-17T15:00:00Z",
  "commit": "fcee057af9844264c6c35a6260b5dcc5e0da31e2"
}
```

The dashboard sidebar shows the same metadata. Go automatically embeds VCS
metadata for builds made from a Git checkout. Release builds can supply explicit
values with linker flags:

```sh
go build -ldflags "-X github.com/pat-rohn/go-iotedge.buildDate=$(date -u +%Y-%m-%dT%H:%M:%SZ) -X github.com/pat-rohn/go-iotedge.commitHash=$(git rev-parse HEAD)" ./cmd/IoTServer
```

## Install and run as a service

Release builds come from [GoReleaser](https://goreleaser.com) (`.goreleaser.yaml`):
a `.deb` and a `.tar.gz` for linux amd64, arm64 and armv7, with the version,
build date and commit stamped into the binary (`GET /version`, System page).

On a Debian-based host (e.g. the ARMv7 box):

```sh
sudo apt install ./iotserver_<version>_armv7.deb
sudoedit /var/lib/iotserver/.iotserver/iot.json   # Password, SessionSecret, DBConfig, Security
sudo chown iotserver: /var/lib/iotserver/.iotserver/iot.json
sudo systemctl start iotserver
journalctl -u iotserver -f
```

The package installs `/usr/bin/IoTServer` and the systemd unit
`iotserver.service` (from `packaging/systemd/`), creates the `iotserver` user
and enables the service without starting it, so it never runs with the
default password. Config and a SQLite database live in `/var/lib/iotserver`.
Stopping the service (SIGTERM) stores buffered MQTT readings first. Without
the package, copy the binary and the unit by hand; the unit expects the same
user and paths.

Making a release (needs a git tag):

```sh
git tag v1.2.0
goreleaser release --clean              # draft release on GitHub
goreleaser release --snapshot --clean   # local build into dist/, nothing published
```

`go.mod` builds against the sibling checkout `../timeseries` (`replace`
directive), so release from this workspace, or push and tag `timeseries` and
drop the `replace` first.

## Dashboard

Log in at `/` with `Password`. Pages, all served from the binary (no
internet needed):

- **Overview** — online/stale/never-seen devices, what needs attention and
  the latest warnings and errors; updates itself every few seconds.
- **Devices** — every device with status, firmware and a link to its own
  page. A device's page has charts of Wi-Fi signal, free heap and uptime
  (24 h / 7 d / 30 d), its restarts and logs, and forms for interval, buffer
  and sensor offsets.
- **Measurements** — one tag over a range with min/max/average, a chart and
  CSV export.
- **Logs** — filter by device and level.
- **System** — version, database, MQTT broker, forwarding and the
  configuration settings that weaken the server.

Health reports are kept for `Monitoring.HealthHistoryDays` (default 30).
After 5 failed logins from one address, logins from it are refused for a
minute, doubling up to 15 minutes. Dark mode follows the operating system.

## API

`/api/v1` is the versioned JSON API, described in [openapi.yaml](openapi.yaml)
and served at `GET /api/v1/openapi.yaml`. Device routes (`POST /devices`,
`/measurements`, `/readings`, `/logs`, `/health`) take the same bodies as the
paths the firmware uses today, which stay as aliases. Admin routes (`GET
/devices`, `/logs`, `/monitoring`, `PUT /devices/{name}`,
`/devices/{name}/sensors/{sensor}`) need a dashboard login (`POST /login`).

## Security settings

Optional hardening in `iot.json`. Everything defaults to off, so an existing
setup keeps working unchanged.

```json
{
  "Password": "<unique>",
  "SessionSecret": "<unique, at least 32 bytes>",
  "Security": {
    "RequireCredentials": true,
    "ProtectAdminRoutes": true,
    "IngestAuth": "warn",
    "IngestToken": "<shared device token>"
  },
  "LogPersistLevel": "warning"
}
```

- `RequireCredentials`: refuse to start while `Password` or `SessionSecret`
  is empty or the shipped default (otherwise only a warning).
- `ProtectAdminRoutes`: `/device/configure` and `/sensor/configure` need a
  dashboard login and answer `401` without one.
- `IngestAuth` with `IngestToken`: shared token the devices send as
  `Authorization: Bearer <token>` on `/init-device`, `/timeseries/save`,
  `/upload-data`, `/update-sensor`, `/api/log` and `/monitoring/health`, and
  as the password (any username) when connecting to the MQTT broker.
  `off` (default) does not check; `warn` accepts clients without it but logs
  them once an hour and lists them on the dashboard; `enforce` answers `401`
  or refuses the MQTT connection. Home Assistant connects to the same broker
  and needs the token as its MQTT password before you switch to `enforce`.
- `LogPersistLevel`: lowest server log level stored in the `logs` table
  (default `warning`). Device logs from `/api/log` are always stored.

## Forwarding

An edge server can pass its MQTT readings on to a central go-iotedge instead
of storing them locally:

```json
{
  "Forwarding": {
    "URL": "http://central:3004",
    "Token": "<the central server's Security.IngestToken>"
  }
}
```

Readings are queued in the local `forward_outbox` table and sent in order to
`<URL>/timeseries/save` every `UploadInterval` seconds. A batch leaves the
queue only once the central server accepted it, so readings survive outages
and restarts; after a failure the forwarder waits 10 s, doubling up to 1 h.
HTTP ingest is not forwarded. The old `MQTTRedirectAddress` still works as
`Forwarding.URL` (without a token) and logs a deprecation warning.

## Timeseries
Check out [this](https://github.com/pat-rohn/timeseries) for how to set-up a postgres-database. Example query for Grafana:
```SQL
SELECT
  "time",
  value as "Temperature"
FROM
  measurements where 
  $__timeFilter("time") AND
  Tag ilike 'Wemos2Temperature'
```

## Example using [Grafana](https://grafana.com/)

![alt text](https://raw.githubusercontent.com/pat-rohn/go-iotedge/main/grafana-example.png)
