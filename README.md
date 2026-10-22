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
  `/upload-data`, `/update-sensor`, `/api/log` and `/monitoring/health`.
  `off` (default) does not check; `warn` accepts requests without it but logs
  the client once an hour and lists it on the dashboard; `enforce` answers
  `401`. Switch to `enforce` once the dashboard list stays empty.
- `LogPersistLevel`: lowest server log level stored in the `logs` table
  (default `warning`). Device logs from `/api/log` are always stored.

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
