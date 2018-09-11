# Polygon Data Fetcher

This module builds a MarketStore background worker which fetches historical
price data of US stocks from [Polygon's API](https://polygon.io/).  It runs
as a goroutine behind the MarketStore process and keeps writing to the disk.
The module uses both the HTTP interface (to backfill from MarketStore's last
written record), and the NATS streaming interface (to receive real-time updates).

## Configuration
polygon.so comes with the server by default, so you can simply configure it
in the MarketStore configuration file.

### Options
Name | Type | Default | Description
--- | --- | --- | ---
api_key | string | none | Your polygon api key
base_url | string | none | The URL to use in the HTTP client
nats_servers | string | Comma separated list of nats servers to connect to
symbols | slice of strings | none | The symbols to retrieve chart bars for

### Example
Add the following to your config file:
```
bgworkers:
  - module: polygon.so
    config:
        api_key: your_api_key
        base_url: https://api.polygon.io
        nats_servers: nats://nats1.polygon.io:30401, nats://nats2.polygon.io:30402, nats://nats3.polygon.io:30403
        symbols:
          - AAPL
          - SPY
```
