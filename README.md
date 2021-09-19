# go-iotedge
Simple IoT-Server for receiving and storing timeseries

Example for a client can be found [here](https://github.com/pat-rohn/wemos-d1-lite)

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
