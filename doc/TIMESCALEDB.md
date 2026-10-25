# TimescaleDB for the Postgres deployment

go-iotedge stores readings in one plain table (`measurements` by default,
`TimeseriesTable` in the config). On a central Postgres that collects years of
readings, [TimescaleDB](https://docs.timescale.com) makes that table a
*hypertable*: split into time chunks, compressed when old, and optionally
pre-aggregated for Grafana. go-iotedge needs no change for it; these are the
steps on the database side.

The ARMv7 edge box uses SQLite and is not affected.

> These steps were written against the schema go-iotedge creates, but have not
> been run against a TimescaleDB instance in this repository's tests (the test
> suite uses plain embedded Postgres). Try them on a copy first.

## What go-iotedge expects of the table

| Column | Type | Notes |
| --- | --- | --- |
| `time` | `TIMESTAMP` (without time zone) | UTC. Firmware sends RFC3339 with `Z`, the MQTT path the server's UTC time. |
| `tag` | `TEXT NOT NULL` | e.g. `flamelightTemperature` |
| `value` | `DOUBLE PRECISION` | `NULL` for readings that were not a number |
| `comment` | `TEXT` | unused by the server |

Index `<table>_tag_time_uidx`, unique on `(tag, time)`. go-iotedge creates it on
start (after removing duplicate rows once) and inserts with
`ON CONFLICT DO NOTHING`, so a repeated reading is skipped. TimescaleDB requires
every unique index on a hypertable to contain the time column; this one does.

## 1. Before you start

1. Deploy a go-iotedge version with the unique `(tag, time)` index and start it
   once, so the one-time duplicate cleanup has run on the plain table. Check:
   ```sql
   SELECT indexname FROM pg_indexes WHERE tablename = 'measurements';
   -- measurements_tag_time_uidx must be listed
   ```
2. Back up the database (`pg_dump -Fc <db> > before-timescale.dump`). Turning a
   hypertable back into a plain table means restoring this backup.
3. Install the TimescaleDB package that matches your Postgres major version
   (see the TimescaleDB install docs for your distribution), add it to
   `shared_preload_libraries` in `postgresql.conf` and restart Postgres:
   ```
   shared_preload_libraries = 'timescaledb'
   ```

## 2. Convert the table

Stop go-iotedge while converting (`sudo systemctl stop iotserver`):
`migrate_data` rewrites the table into chunks and holds a lock until it is done,
which takes a while on a large table.

```sql
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- The time column of a hypertable must not be NULL. The duplicate cleanup
-- leaves such rows alone; they cannot be shown on a time axis anyway.
SELECT count(*) FROM measurements WHERE time IS NULL;
DELETE FROM measurements WHERE time IS NULL;

SELECT create_hypertable('measurements', 'time',
    chunk_time_interval => INTERVAL '7 days',
    migrate_data        => true);
```

Seven-day chunks suit a few dozen sensors reporting every minute or so; with
far more data, shorter chunks keep each chunk's index in memory.

Start go-iotedge again. On start it runs `CREATE TABLE IF NOT EXISTS` and finds
the unique index by name, so it leaves the hypertable as it is.

## 3. Compress old chunks

Readings older than a month are rarely changed. Compressing them by tag
usually shrinks them to a fraction of their size and keeps
`WHERE tag = … AND time BETWEEN …` (Grafana, the dashboard's measurements
page) fast:

```sql
ALTER TABLE measurements SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'tag',
    timescaledb.compress_orderby   = 'time DESC'
);
SELECT add_compression_policy('measurements', INTERVAL '30 days');
```

Late readings older than 30 days land in compressed chunks: a forwarding
backlog after a long outage, or a device that buffered for weeks. Make sure
your TimescaleDB version accepts `INSERT … ON CONFLICT DO NOTHING` into
compressed chunks (older 2.x releases rejected it); otherwise raise the
compression age above the longest backlog you expect.

## 4. Optional: drop very old raw data

```sql
SELECT add_retention_policy('measurements', INTERVAL '5 years');
```

This deletes whole chunks older than five years. Leave it out to keep
everything; use it together with step 5 to keep hourly values longer than raw
ones.

## 5. Optional: hourly values for long Grafana ranges

A continuous aggregate keeps hourly average, minimum and maximum per tag up to
date, so a one-year panel reads 8,760 rows per tag instead of every reading:

```sql
CREATE MATERIALIZED VIEW measurements_hourly
WITH (timescaledb.continuous) AS
SELECT time_bucket(INTERVAL '1 hour', time) AS bucket,
       tag,
       avg(value) AS value,
       min(value) AS min,
       max(value) AS max
FROM measurements
GROUP BY bucket, tag
WITH NO DATA;

SELECT add_continuous_aggregate_policy('measurements_hourly',
    start_offset      => INTERVAL '3 days',
    end_offset        => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour');

-- Fill it once for the existing history (can take a while):
CALL refresh_continuous_aggregate('measurements_hourly', NULL, now() - INTERVAL '1 hour');
```

Grafana query for long ranges:

```sql
SELECT bucket AS "time", value AS "Temperature"
FROM measurements_hourly
WHERE $__timeFilter(bucket) AND tag = 'flamelightTemperature'
ORDER BY bucket
```

Short ranges keep querying `measurements` directly, as in the README.

## 6. Check

```sql
SELECT hypertable_name, num_chunks, compression_enabled
FROM timescaledb_information.hypertables;

SELECT pg_size_pretty(hypertable_size('measurements'));

SELECT pg_size_pretty(before_compression_total_bytes) AS before,
       pg_size_pretty(after_compression_total_bytes)  AS after
FROM hypertable_compression_stats('measurements');

SELECT job_id, proc_name, schedule_interval, next_start
FROM timescaledb_information.jobs;
```

The dashboard's System page shows the database size (`pg_database_size`), so
the effect of compression is visible there too.

## Things to know

- The measurements page lists tags with `SELECT DISTINCT tag`. On a very large
  hypertable this reads every chunk; compression by `tag` keeps it cheaper.
- go-iotedge's forwarding (`Forwarding.URL` on an edge box) delivers to the
  central server through the normal ingest path, so forwarded readings end up
  in the hypertable like any other.
- A failed compression or aggregate job shows up in
  `timescaledb_information.job_stats`, not in go-iotedge's logs.
