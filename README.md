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

Optional hardening in `iot.json`. Both switches default to `false`, so an
existing setup keeps working unchanged; none of them affects the endpoints
the firmware uses.

```json
{
  "Password": "<unique>",
  "SessionSecret": "<unique, at least 32 bytes>",
  "Security": {
    "RequireCredentials": true,
    "ProtectAdminRoutes": true
  },
  "LogPersistLevel": "warning"
}
```

- `RequireCredentials`: refuse to start while `Password` or `SessionSecret`
  is empty or the shipped default (otherwise only a warning).
- `ProtectAdminRoutes`: `/device/configure` and `/sensor/configure` need a
  dashboard login and answer `401` without one.
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
