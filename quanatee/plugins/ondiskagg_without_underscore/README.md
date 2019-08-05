# Quanatee Modification

Only aggregate data that does not have a prefix (denoted by `_` in the bucket) into user defined timeframes.
Buckets with a prefix indicate raw price data, relevant to Crypto since we aggregate the price data from multiple sources.
We do not use this for Forex and Equity data since we do not aggregate the data with other sources at this time.

The change reduces CPU overhead that does nothing since the generated timeframes from prefixed buckets would not be utilized.

# On-disk Aggregate Trigger

This module builds a MarketStore trigger which updates the downsample data upon
the writes on the underlying timeframe.  This is typical use case for long historical
price data where you don't want to read all the minute level data for years
but want to keep the consistency between timeframes.  In a way, this provides
materialized views.

## Configuration
ondiskagg.so comes with the server by default, so you can simply configure it
in MarketStore configuration file.

### Options
Name | Type | Default | Description
--- | --- | --- | ---
on | string | none | The file glob pattern to match on
filter | string | none | Filters pushes to '1D' timeframes and above based on market hours. Only 'nasdaq' is supported at this time.
destinations | slice of strings | Downsample target time windows

### Example
Add the following to your config file:
```
triggers:
  - module: ondiskagg.so
    on: */1Min/OHLCV
    config:
        filter: "nasdaq"
        destinations:
            - 5Min
            - 15Min
            - 1H
            - 1D
```


## Build
If you need to change the code, you can build it from this directory by:

```
$ make all
```

It installs the new .so file to the first GOPATH/bin directory.


## Caveat
Since this is implemented based on the Go's plugin mechanism, it is supported only
on Linux & MacOS as of Go 1.10
