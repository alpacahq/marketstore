# BitMEX Data Fetcher

This module builds a MarketStore background worker which fetches historical
price data of cryptocurrencies from BitMEX public API. It runs as a goroutine
behind the MarketStore process and keeps writing to the disk.

## Configuration

gdaxfeeder.so comes with the server by default, so you can simply configure it
in MarketStore configuration file.

### Options

| Name           | Type             | Default                  | Description                                               |
| -------------- | ---------------- | ------------------------ | --------------------------------------------------------- |
| query_start    | string           | 2017-01-01T00:00:00.000Z | The point in time from which to start fetching price data |
| base_timeframe | string           | [1m, 5m, 1h, 1d]         | The bar aggregation duration                              |
| symbols        | slice of strings | [.XBT, XBTM18, XBTU18]   | The symbols to retrieve data for                          |

Symbols available:

```text
.ADAXBT, .ADAXBT30M, .BCHXBT, .BCHXBT30M, .BVOL, .BVOL24H, .BVOL7D, .BXBT, .BXBT30M, .BXBTJPY,
.BXBTJPY30M, .DASHXBT, .DASHXBT30M, .EOSXBT, .EOSXBT30M, .ETCXBT, .ETCXBT30M, .ETHBON, .ETHBON2H, .ETHBON8H,
.ETHXBT, .ETHXBT30M, .EVOL7D, .LTCXBT, .LTCXBT30M, .NEOXBT, .NEOXBT30M, .USDBON, .USDBON2H, .USDBON8H,
.XBT, .XBT30M, .XBTBON, .XBTBON2H, .XBTBON8H, .XBTJPY, .XBTJPY30M, .XBTUSDPI, .XBTUSDPI2H, .XBTUSDPI8H,
.XLMXBT, .XLMXBT30M, .XMRXBT, .XMRXBT30M, .XRPXBT, .XRPXBT30M, .ZECXBT, .ZECXBT30M, ADAM18, BCHM18,
BTC/USD, EOSM18, ETHM18, LTCM18, XBT7D_D95, XBT7D_U105, XBTM18, XBTU18, XRPM18
```

#### Query Start

The fetcher keeps filling data up to the current time eventually and writes new data as it is
generated. Once data starts to fetch, it restarts from the last-written data
timestamp even after the server is restarted. You can specify fewer symbols
if you don't need others. Since BitMEX API has rate limit, this may help to
fill the historical data if you start from old. Note that the data fetch timestamp
is identical among symbols, so if one symbol lags other fetches may not be
up to speed.

#### Base Timeframe

The daily bars are written at the boundary of system timezone configured in the same file.

### Example

Add the following to your config file:

```text
bgworkers:
  - module: bitmexfeeder.so
    config:
      query_start: "2018-01-01 00:00"
      symbols:
        - .XBT
      base_timeframe: "1D"
```

## Build

If you need to change the fetcher, you can build it by:

```text
$ make configure
$ make all
```

It installs the new .so file to the first $GOPATH/bin directory.

## Caveat

Since this is implemented based on the Go's plugin mechanism, it is supported only
on Linux & MacOS as of Go 1.10
