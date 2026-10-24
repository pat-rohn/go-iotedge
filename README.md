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
