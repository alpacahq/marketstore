# Polygon Data Fetcher

This module builds a MarketStore background worker which fetches hisotrical
price data of US stocks from [Polygon's API](https://polygon.io/).  It runs
as a goroutine behind the MarketStore process and keeps writing to the disk.
The module uses both the HTTP interface (to backfill from MarketStore's last
written record), and the NATS streaming interface (to receive real-time updates).

## Configuration
polygon.so comes with the server by default, so you can simply configure it
in the MarketStore configuration file. Add the following to your config file:

```
bgworkers:
    - module: polygon.so
      name: Polygon
      config:
          api_key: your_api_key
          base_url: https://api.polygon.io
          symbols:
            - AAPL
            - SPY
```