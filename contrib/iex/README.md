# IEX Data Fetcher

This module builds a MarketStore background worker which fetches historical
price data of US stocks from [IEX's API](https://iextrading.com/).  It runs
as a goroutine behind the MarketStore process and keeps writing to the disk.
The module uses the HTTP interface to query the latest bars and stay up-to-date.
Note that only 1Min -> 1D bars are supported at this time.

## Configuration
iex.so comes with the server by default, so you can simply configure it
in the MarketStore configuration file.

### Options
Name | Type | Default | Description
--- | --- | --- | ---
daily | boolean | false | Pull daily (1D) bars
intraday | string | false | Pull intraday (1Min bars)
symbols | slice of strings | none | The symbols to retrieve chart bars for

### Example
Add the following to your config file:
```
bgworkers:
  - module: iex.so
    config:
        daily: true
        intraday: true
        symbols:
          - AAPL
          - SPY
```

### Backfilling
IEX's `/chart` API doesn't support querying intraday bar history further back
than the current market day. In order to properly backfill intraday bars before
running the plugin live, a backfill script has been included for the specific
purpose of backfilling the history of intraday (1Min) bars. Daily bars will
automatically be backfilled by the plugin for the trailing 5 years upon the recipt
of a 1D bar for a given symbol. Also note that intraday bars will be backfilled for
the given market day in the event of starting the system up after market open, or
unexpected intraday downtime.