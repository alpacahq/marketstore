# Quanatee Modification

The modification brings the code more similar to the community-based binance plugin but with changes.

- All Symbols and Base Currencies must have their respective pairs (Function was removed since it was not a priority, may be added in the future)
- If `query_start` is before the starting date of the pair, `0` will be added to `OHLCV`. We accounted for this externally during aggregation
- If backfilling produces missing or corrupted data, `0` will be added to `OHLCV`. This case is most likely disruptions from the exchanges
- If real-time returns missing or corrupted data, `0` will be added to `OHLCV`.  We accounted for this externally during aggregation
- If disruption is at our end, or blank slate protocol is initated, we account for this externally by checking for missing data at regular intervals throughout the day

# Binance Data Fetcher

Replicates many of the features from [gdaxfeeder](https://github.com/alpacahq/marketstore/tree/master/contrib/gdaxfeeder)
This module builds a MarketStore background worker which fetches historical
price data of cryptocurrencies from Binance's public API. It runs as a goroutine
behind the MarketStore process and keeps writing to the disk.

## Configuration

binancefeeder.so comes with the server by default, so you can simply configure it
in MarketStore configuration file.

### Options

| Name            | Type             | Default                                                                  | Description                                               |
| --------------- | ---------------- | ------------------------------------------------------------------------ | --------------------------------------------------------- |
| query_start     | string           | none                                                                     | The point in time from which to start fetching price data |
| base_currencies | slice of strings | ["USDT"]                                                                 | Base currency for symbols. ex: BTC, ETH, USDT             |
| base_timeframe  | string           | 1Min, 1H, 1D                                                             | The bar aggregation duration                              |
| symbols         | slice of strings | [All "trading" symbols from https://api.binance.com/api/v1/exchangeInfo] | The symbols to retrieve data for                          |

#### Query Start

The fetcher keeps filling data up to the current time eventually and writes new data as it is
generated. It writes data every 30 \* your time interval. It then pauses for 1 second after each call. Note that the data fetch timestamp is identical among symbols, so if one symbol lags other fetches may not be
up to speed.

#### Base Timeframe

The daily bars are written at the boundary of system timezone configured in the same file.

### Example

Add the following to your config file:

```yml
bgworkers:
  - module: binancefeeder.so
    name: BinanceFetcher
    config:
      symbols:
        - ETH
      base_timeframe: '1Min'
      base_currencies:
        - USDT
        - BTC
      query_start: '2018-01-01 00:00'
```

## Build

If you need to change the fetcher, you can build it by:

```bash
$ make configure
$ make all
```

It installs the new .so file to the first GOPATH/bin directory.

## Caveat

Since this is implemented based on the Go's plugin mechanism, it is supported only
on Linux & MacOS as of Go 1.10
