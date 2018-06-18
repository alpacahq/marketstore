# BitMEX Data Fetcher

This module builds a MarketStore background worker which fetches historical
price data of cryptocurrencies from BitMEX public API. It runs as a goroutine
behind the MarketStore process and keeps writing to the disk.

## Configuration

gdaxfeeder.so comes with the server by default, so you can simply configure it
in MarketStore configuration file.

### Options

| Name           | Type             | Default                | Description                                               |
| -------------- | ---------------- | ---------------------- | --------------------------------------------------------- |
| query_start    | string           | 2017-01-01 00:00       | The point in time from which to start fetching price data |
| base_timeframe | string           | [1Min, 5Min, 1H, 1D]   | The bar aggregation duration                              |
| symbols        | slice of strings | [.XBT, XBTM18, XBTU18] | The symbols to retrieve data for                          |

Symbols available:

```text
.ADAXBT, .BCHXBT, .BXBT, .BXBTJPY,
.DASHXBT, .EOSXBT, .ETCXBT, .ETHBON,
.ETHXBT, .LTCXBT, .NEOXBT, .USDBON,
.XBT, .XBTBON, .XBTJPY, .XBTUSDPI,
.XLMXBT, .XMRXBT, .XRPXBT, .ZECXBT,
EOSM18, ETHM18, LTCM18, XBT7D_D95,
XBT7D_U105, XBTM18, XBTU18, XRPM18
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
