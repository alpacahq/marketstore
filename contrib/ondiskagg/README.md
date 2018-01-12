# On-disk Aggregate Trigger

This module buidls a MarketStore trigger which updates the downsample data upon
the writes on the underlying timeframe.  This is typical use case for long historical
price data where you don't want to read all the minute level data for years
but want to keep the consistency between timeframes.  In a way, this provides
materialized views.

## Configuration
ondiskagg.so comes with the server by default, so you can simply configure it
in MarketStore configuration file.  Add the following to your config file.

```
triggers:
    - module: ondiskagg.so
      on: */1Min/OHLCV
      config:
          # filter: "nasdaq"
          destinations:
              - 5Min
              - 15Min
              - 1H
              - 1D
```

`destinations` are downsample target time windows.  Optionally, if filter
is set to `nasdaq`, it filters the scan data by NASDAQ market hours for 1D or
upper timeframes.

## Build
If you need to change the code, you can build it from this directory by:

```
$ make all
```

It installs the new .so file to the first GOPATH/bin directory.


## Caveat
Since this is implemented based on the Go's plugin mechanism, it is supported only
on Linux as of Go 1.9
