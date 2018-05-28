# GDAX Data Fetcher

This module builds a MarketStore background worker which fetches hisotrical
price data of cryptocurrencies from GDAX public API.  It runs as a goroutine
behind the MarketStore process and keeps writing to the disk.

## Configuration
gdaxfeeder.so comes with the server by default, so you can simply configure it
in MarketStore configuration file.  Add the following to your config file.

```
bgworkers:
    - module: gdaxfeeder.so
      name: GdaxFetcher
      config:
          query_start: "2018-01-01 00:00"
          # symbols:
          #     - BTC
          # base_timeframe: "1D"
```

`query_start` can specify what time of data to start fetch.  The fetcher keeps
filling data up to the current time eventually and writes new data as it is
generated.  Once data starts to fetch, it restarts from the last-written data
timestamp even after the server is restarted.  You can specify fewer symbols
if you don't need others.  Since GDAX API has rate limit, this may help to
fill the historical data if you start from old.  Note that the data fetch timestamp
is identical among symbols, so if one symbol lags other fetches may not be
up to speed.

`base_timeframe` can specify if you want other than 60 second bars. Specify
`1D` if you need only daily bars.  The daily bars are written at the boundary of
system timezone configured in the same file.


## Build
If you need to change the fetcher, you can build it by:

```
$ make configure
$ make all
```

It installs the new .so file to the first GOPATH/bin directory.


## Caveat
Since this is implemented based on the Go's plugin mechanism, it is supported only
on Linux & MacOS as of Go 1.10