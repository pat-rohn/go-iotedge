# Edge Server Workbook

## Purpose

Plan the hardening, modernisation and new web interface of go-iotedge. Started
from a review on 2026-10-22. Findings carry IDs (`[S1]`, `[T2]`, …) so phases
and commits can cite them.

Related: the firmware side is in `../../esp-enlightened` (`WEB_INTERFACE_WORKBOOK.md`,
`BUGS.md`), storage in `../../timeseries`. Cross-repo contracts are listed in the
workspace skill `../../.claude/skills/iot-workspace/SKILL.md`.

## Current State

- Go 1.25, single flat package `iotedge` (~2.3k lines + ~1.4k lines tests),
  CLI in `cmd/IoTServer` (cobra), config via viper (`~/.iotserver/iot.json`).
- HTTP: Gin on port 3004. Session-cookie login for `/dashboard` only; all device
  and configuration endpoints are unauthenticated.
- MQTT: embedded `mochi-co/mqtt` v1 broker on 1883 plus an internal paho client
  that connects back to it with 18 wildcard subscriptions after a fixed 2 s sleep.
  Readings are buffered in memory and flushed every `UploadInterval` s.
- Storage: `timeseries` library (sibling checkout via `replace`), SQLite or
  Postgres. Schema created inline with per-dialect SQL strings; process-wide
  singletons for device DB, logging DB and the `timeseries` handler.
- Logging: logrus, with a hook that persists every enabled log line to `logs`.
- Web UI: one server-rendered template (`templates/dashboard.html`) with
  Devices/Logs tabs, manual refresh, read-only; hand-written CSS with tokens.
- Tests: `go test ./...` green, ~3 min (mostly modernc SQLite compile). `go vet` clean.
- Deploy: `cmd/IoTServer/build.sh` cross-compiles ARMv7 and `scp`s to frick-server.

## Findings

### S — Security

| ID | Finding | Where | Fix |
| --- | --- | --- | --- |
| S1 | **SQL injection**: `InsertTimeseries` concatenates `Tag`, timestamps into SQL; reachable unauthenticated via `POST /upload-data` and `/timeseries/save` | `timeseries/dbhandler.go` `InsertTimeseries` | Parameterised batch insert in a transaction (`$n` / `?`), validate timestamp format |
| S2 | Device write and admin endpoints (`/device/configure`, `/sensor/configure`, `/timeseries/save`, ingest) unauthenticated | `server.go` routes | Admin routes behind session; ingest behind a bearer token (shared or per device, matching firmware `ApiToken`) |
| S3 | MQTT broker accepts any client and any topic | `mqtt.go` `StartMQTTBroker` | Auth/ACL hook (after M1) with per-device credentials |
| S4 | Logout broken: cookie is `HttpOnly`, JS `document.cookie` cannot clear it | `templates/dashboard.html` `logout()` | `POST /logout` → `session.Clear()`, `MaxAge: -1` |
| S5 | No login rate limiting; no CSRF protection on future forms | `handlers.go` `performLogin` | Simple per-IP backoff; CSRF token (or SameSite=Strict + Origin check) |
| S6 | Server starts with default password / session secret (only warns) | `server.go` | Refuse to start unless set (or generate secret on first run and store it) |
| S7 | `Secure: false` cookie TODO | `server.go` | Enable when served behind TLS (reverse proxy) |

### T — Correctness / timeseries

| ID | Finding | Where | Fix |
| --- | --- | --- | --- |
| T1 | After 100 000 rows the insert switches to hard-coded `measurements`, ignoring `table` | `timeseries/dbhandler.go` | Use the parameter (resolved by S1 rewrite) |
| T2 | `on conflict do nothing` without a unique key → duplicates never caught | timeseries schema | Unique index on `(tag, time)` (or drop the clause) |
| T3 | No index for Grafana queries `WHERE tag = … AND time BETWEEN …` | timeseries schema | Index `(tag, time)`; Timescale hypertable on Postgres |
| T4 | MQTT readings lost on crash/restart (memory buffer up to `UploadInterval`) | `mqtt.go` | Shorter flush, or write-through with batching; flush on shutdown |
| T5 | MQTT data path never shuts down; `StartSensorServer` stop channel never signalled; no SIGTERM handling | `cmd/IoTServer/main.go` | `signal.NotifyContext`, pass `ctx` everywhere, flush then close DB |
| T6 | Column typo `intervall` | `devicedb.go` | Rename in a migration (D2) |

### P — Performance / operations

| ID | Finding | Where | Fix |
| --- | --- | --- | --- |
| P1 | Log hook persists every enabled line; `GetDeviceDB` logs Info on each MQTT reading → one extra DB write per reading | `logging.go`, `devicedb.go` | Persist Warn+ only; inject DB once, no per-call lookup/log |
| P2 | `GetDeviceDB` per message: mutex + ping + config compare | `mqtt.go` closures | Construct dependencies in `main`, pass in |
| P3 | Tests take ~3 min | test suite | Faster after singletons go away (in-memory SQLite per test, `t.Parallel`) |
| P4 | Deploy by `scp`, no service definition in repo | `build.sh` | systemd unit + GoReleaser (linux/arm/v7, amd64), version ldflags |

### A — Architecture / dependencies

| ID | Finding | Recommendation |
| --- | --- | --- |
| A1 | `mochi-co/mqtt` v1 is deprecated; loopback paho client is fragile | `mochi-mqtt/server` v2, consume via `OnPublish` hook (no internal client, no wildcard list) |
| A2 | logrus in maintenance mode | `log/slog` (structured, stdlib) |
| A3 | Flat package, global singletons | `internal/{config,store,ingest/http,ingest/mqtt,monitoring,homeassistant,web}`, wire in `cmd/IoTServer` |
| A4 | Inline per-dialect DDL, no schema version | `goose` embedded migrations; `sqlc` for typed queries |
| A5 | Mixed URIs and casing; `Accept` header switches HTML/JSON | `/api/v1/...` JSON + OpenAPI spec; HTML routes separate; old paths kept as aliases until firmware moves |
| A6 | Plain measurements table | Postgres: TimescaleDB hypertable + retention/compression policy; SQLite stays for small edge installs |
| A7 | Gin | Keep (works); revisit only if moving to stdlib `net/http` routing during A3 |

### W — Web interface

Current dashboard: two tabs, read-only, manual reload, no charts, device
configuration only possible via CLI/API.

**Decision (proposed):** templ (type-safe Go templates) + htmx + SSE, embedded
in the binary. No Node toolchain, fits ARMv7, suits a single-admin tool.
Alternative considered: Angular SPA sharing code with `enlightened` — rejected
for now (second build pipeline, no real reuse: the app talks to devices, not
to this server).

Target pages:

| ID | Page | Content |
| --- | --- | --- |
| W1 | Overview | Online/stale/never-seen tiles, restarts 7 d, weak RSSI / unsynced clock list, latest errors; live via SSE |
| W2 | Devices | Searchable list; status, firmware, last seen, IP linking to the device's own page |
| W3 | Device detail | Health history (RSSI, heap, uptime), restart timeline, sensors with sparklines (uPlot), **editable** interval/buffer/sensor offsets |
| W4 | Measurements | Tag + range picker, chart, CSV export (quick look, not a Grafana replacement) |
| W5 | Logs | Filter by device/level/text, live tail, cursor pagination (replace offset multiples of 100) |
| W6 | System | Version, DB size, MQTT clients + msg rate, config warnings (default password, no TLS) |
| W8 | Device URL | Device list links each device's own page (`http://<ip>/`) with "seen x min ago"; see D1 |
| W7 | Cross-cutting | Dark mode (reuse CSS tokens), CSRF, accessible tables, mobile layout kept |

Needs health history storage: today `device_health` keeps only the latest row.
Add a `device_health_history` table (or write health metrics as timeseries tags
`<device>.rssi`, …) before W3.

## Phases

### Phase 0 — Security fixes ✅ Done 2026-10-22
- [x] S1 + T1 parameterised insert in `timeseries` (`7ffdb99`); also fixed the
      singleton handing out a closed handle after `Close` (`b13348b`)
- [x] S4 server-side logout (`02bc365`)
- [x] P1 `LogPersistLevel` (default warning); device logs were stored twice (`4ceb0d4`)
- [x] S6 `Security.RequireCredentials`, S2 `Security.ProtectAdminRoutes` —
      opt-in, default off (`1144242`)
- Verified live with SweetHome3 (MQTT) against both switches on: init-device,
  health, log, CO2 readings each minute, login/logout, 401 on configure.
  The firmware HTTP path `/timeseries/save` is covered by tests only.

### Phase 1 — Ingest auth and MQTT v2 ✅ Done 2026-10-24 (A2 moved to Phase 2)
- [x] S2 shared ingest token on HTTP, `Security.IngestAuth` off/warn/enforce,
      warn lists clients on the dashboard (`23e5a06`)
- [x] D1 device address from the request source IP, linked on the dashboard (`c06ce33`)
- [x] A1 `mochi-mqtt/server` v2, `OnPublished` hook, no internal client (`9892d7f`)
- [x] S3 ingest token as MQTT password, same modes (`840c092`)
- [x] T5 graceful shutdown via context; T4 flush buffered MQTT readings on
      shutdown (`f87d207`), verified with SIGTERM on the binary
- [x] timeseries `Execute` returned nil on errors (`c7c4b3d`)
- [x] Firmware: `IngestToken` config key, sent as HTTP bearer and MQTT password
      (esp-enlightened `89b4360`, `c82e630`); app field (enlightened `da92fbd`).
      flamelight flashed (OTA), SweetHome3 flashed (USB; no IP from router, known).
- [x] Live check 2026-10-24: frick-server runs `f87d207`; flamelight on new
      firmware stable 25 min (one 49 s MQTT drop right after the OTA reboot).
      Against a local server with `IngestAuth: enforce`: flamelight's
      init-device, health, log over HTTP and its MQTT connection all accepted
      with the token; requests without it refused. `devices.address` recorded.
      flamelight restored to frick-server, token cleared.
- Note: in `enforce` mode Home Assistant also needs the token as MQTT password.
- Note: forwarding (`MQTTRedirectAddress` → central `/timeseries/save`) sends
  no token yet; F1 adds it.

### Phase 2 — Structure and schema ✅ Done 2026-10-24
- [x] Tests also run on embedded Postgres: `IOTEDGE_TEST_POSTGRES=1 go test ./...`
      (`14d58e4`); found the unbounded pool, now capped at 20 (timeseries `f80f71a`)
- [x] T3 `(tag, time)` index on the timeseries table (timeseries `6026340`)
- [x] A4 versioned migrations (`schema.go`, `schema_migrations`), own small
      migrator instead of goose: the legacy upgrades need dialect-specific Go
      steps anyway (`ee37ec8`)
- [x] T2 unique `(tag, time)` index; duplicates deleted once on first start,
      first row kept, NULL times untouched (timeseries `600bd61`). timeseries
      tests also run on embedded Postgres (`TIMESERIES_TEST_POSTGRES=1`)
- [-] T6 rename `intervall`: won't do — cosmetic, and an older binary could no
      longer read `devices` after the rename (breaks rollback)
- [x] A3 singletons removed (`063b123`; timeseries `Open`, `c11eba6`);
      `internal/store` (DB layer, `5b31ed5`), `internal/homeassistant`
      (`e15a3ae`). HTTP/MQTT server stays in the root package for now; a
      `web` package comes with Phase 3. Side effect: the timeseries singleton
      no longer logs the DB config (it contained the Postgres password)
- [x] A5 `/api/v1` + `openapi.yaml` (served at `/api/v1/openapi.yaml`, test keeps it in
      sync with the router); firmware paths stay as aliases; admin routes login-only (`e3f61d3`)
- [x] P3 each test run has its own HOME, SQLite file and free ports (parallel
      runs safe, ~/.iotserver untouched); sleeps replaced by readiness waits;
      suite 38 s → ~10–16 s; `IOTEDGE_LOAD_TESTS=1` for the long MQTT load (`47ccc69`)
- [x] A2 slog: persisting handler replaces the logrus hook; timeseries still logs
      via logrus, bridged into slog (`f6dbf27`)

### Phase 3 — Web interface ✅ Done 2026-10-24
Built on html/template + htmx (vendored) + SSE instead of templ: no code
generation step in the build, existing templates reused.
- [x] Pages on a shared layout, CSRF token per session (`e2df3c6`)
- [x] W3 device page: health history (new `device_health_history`, migration 2),
      charts per metric, restarts, logs, editing interval/buffer/offsets (`1261db5`)
- [x] W1 live overview over SSE; request contexts end with the server (`09351ba`)
- [x] W4 measurements: tag + range, stats, chart, CSV (`1fb2518`; timeseries
      `Tags`/`Series`, `a9be4e5`)
- [x] W6 system page (`4bbd193`)
- [x] W7 dark mode, mobile, accessibility (`1cf8ed6`); W8 device URL (D1)
- [x] S5 login rate limiting (`03d6bfa`); CSRF on dashboard forms
- Charts follow the dataviz skill: one series per chart, no dual axes,
  crosshair + keyboard, table view, colours validated light and dark.

### Phase 4 — Operations ⬜
- [x] P4 systemd unit + GoReleaser (`47a73ed`): deb/tar.gz for amd64, arm64,
      armv7, version stamped; snapshot build verified locally. Releases need the
      `../timeseries` replace resolved for builds outside this workspace
- [x] A6 TimescaleDB notes in `doc/TIMESCALEDB.md` (not run against TimescaleDB here)
- [ ] S7 TLS via reverse proxy, `Secure` cookies

## Decisions (2026-10-22)

- **Ingest auth: one shared ingest token** (HTTP `Authorization: Bearer`, MQTT
  password). Opt-in like all Phase 0 hardening: devices without a firmware
  update must keep working until the token is enforced.
- **ARMv7 box is SQLite only.** Postgres is the central/other deployment.
- **Forwarding (`MQTTRedirectAddress`) stays**, redesigned (see F1).

### F1 — Forwarding ✅ Done 2026-10-24 (scope kept: MQTT readings only, not stored locally)

Implemented as `Forwarding {URL, Token}` with a durable ordered outbox and
backoff; `MQTTRedirectAddress` maps to it. Decided against forwarding HTTP
ingest and `StoreLocally` for now. Original proposal:

Today: only the MQTT buffer is forwarded, it then replaces local storage, it is
fire-and-forget (`go sendData`, data lost on failure) and unauthenticated.

Proposal: `Forwarding { URL, Token, StoreLocally, IntervalSeconds }`
- applies to every ingest path (HTTP and MQTT), not only MQTT
- durable outbox table, sent in batches, retried with backoff, survives restarts
- authenticates with the central server's shared ingest token
- targets `/api/v1/measurements` (A5); `/timeseries/save` stays as alias
- `StoreLocally` keeps a local copy (edge buffer and quick look)
- `MQTTRedirectAddress` still read and mapped to `Forwarding.URL` with a
  deprecation warning
