# MarketStore
![Build Status](https://circleci.com/gh/alpacahq/marketstore/tree/master.png?7989cb00be70f055e0cb19184b212a8ed21b0cbb) [![GoDoc](http://img.shields.io/badge/godoc-reference-blue.svg)](https://godoc.org/github.com/alpacahq/marketstore) [Telegram group!](https://t.me/joinchat/HKxN3BGm6mE5YBt79CMM3Q)

## Introduction
MarketStore is a database server optimized for financial timeseries data.
You can think of it as an extensible DataFrame service that is accessible from anywhere in your system, at higher scalability.

It is designed from the ground up to address scalability issues around handling large amounts of financial market data used in algorithmic trading backtesting, charting, and analyzing price history with data spanning many years, and granularity down to tick-level for the all US equities or the exploding crypto currencies space. If you are struggling with managing lots of HDF5 files, this is perfect solution to your problem.

The batteries are included with the basic install - you can start pulling crypto price data from [GDAX](https://docs.gdax.com/#get-historic-rates) and writing it to the db with a simple [plugin](#plugins) configuration.

MarketStore enables you to query DataFrame content over the network at as low latency as your local HDF5 files from disk, and appending new data to the end is two orders of magnitude faster than DataFrame would be. This is because the storage format is optimized for the type of data and use cases as well as for modern filesystem/hardware
characteristics.

MarketStore is production ready! At [Alpaca](https://alpaca.markets) it has been used in production for years in serious business. If you encounter a bug or are interested in getting involved, please see the [contribution section](#development) for more details.

## Install

### Docker
If you want to get started right away, you can bootstrap a marketstore db instance using our latest [docker image](https://hub.docker.com/r/alpacamarkets/marketstore/tags/).

``` sh
docker run -p 5993:5993 alpacamarkets/marketstore:v2.1.2
```

### Source
MarketStore is implemented in Go (with some CGO), so you can build it from
source pretty easily. You need Go 1.9+ and [dep](https://github.com/golang/dep).
``` sh
go get -u github.com/alpacahq/marketstore
```
and then in the repo directory, install dependencies using
``` sh
make configure
```
then compile and install the project binaries using
``` sh
make install
```
Optionally, you can install the project's included plugins using
``` sh
make plugins
```

## Usage
Run it:
``` sh
marketstore
```
To learn how to format a proper db query, please see [this](./frontend/)

## Configuration
In order to run MarketStore, a YAML config file is needed. A default file (mkts.yml) is included in the repo. The path to this file is passed in to the launcher binary with the `-config` flag, or by default it finds a file named mkts.yml in the directory it is running from.

### Options
Flag | Type | Description
--- | --- | ---
root_directory | string | Allows the user to specify the directory in which the MarketStore database resides
listen_port | int | Port that MarketStore will serve through
timezone | string | System timezone by name of TZ database (e.g. America/New_York)
log_level | string  | Allows the user to specify the log level (info | warning | error)
queryable | bool | Allows the user to run MarketStore in polling-only mode, where it will not respond to query
stop_grace_period | int | Sets the amount of time MarketStore will wait to shutdown after a SIGINT signal is received
wal_rotate_interval | int | Frequency (in mintues) at which the WAL file will be trimmed after being flushed to disk  
stale_threshold | int | Threshold (in days) by which MarketStore will declare a symbol stale
enable_add | bool | Allows new symbols to be added to DB via /write API
enable_remove | bool | Allows symbols to be removed from DB via /write API  
triggers | slice | List of trigger plugins
bgworkers | slice | List of background worker plugins

### Example mkts.yml
```
root_directory: /project/data/mktsdb
listen_port: 5993
log_level: info
queryable: true
stop_grace_period: 0
wal_rotate_interval: 5
stale_threshold: 5
enable_add: true
enable_remove: false
```


## Clients
After starting up a MarketStore instance on your machine, you're all set to be able to read and write tick data.

### Python
[pymarketstore](https://github.com/alpacahq/pymarketstore) is the standard
python client.

```
In [1]: import pymarketstore as pymkts

## query data

In [2]: param = pymkts.Params('BTC', '1Min', 'OHLCV', limit=10)

In [3]: cli = pymkts.Client()

In [4]: reply = cli.query(param)

In [5]: reply.first().df()
Out[5]:
                               Open      High       Low     Close     Volume
Epoch
2018-01-17 17:19:00+00:00  10400.00  10400.25  10315.00  10337.25   7.772154
2018-01-17 17:20:00+00:00  10328.22  10359.00  10328.22  10337.00  14.206040
2018-01-17 17:21:00+00:00  10337.01  10337.01  10180.01  10192.15   7.906481
2018-01-17 17:22:00+00:00  10199.99  10200.00  10129.88  10160.08  28.119562
2018-01-17 17:23:00+00:00  10140.01  10161.00  10115.00  10115.01  11.283704
2018-01-17 17:24:00+00:00  10115.00  10194.99  10102.35  10194.99  10.617131
2018-01-17 17:25:00+00:00  10194.99  10240.00  10194.98  10220.00   8.586766
2018-01-17 17:26:00+00:00  10210.02  10210.02  10101.00  10138.00   6.616969
2018-01-17 17:27:00+00:00  10137.99  10138.00  10108.76  10124.94   9.962978
2018-01-17 17:28:00+00:00  10124.95  10142.39  10124.94  10142.39   2.262249

## write data

In [7]: import numpy as np

In [8]: import pandas as pd

In [9]: data = np.array([(pd.Timestamp('2017-01-01 00:00').value / 10**9, 10.0)], dtype=[('Epoch', 'i8'), ('Ask', 'f4')])

In [10]: cli.write(data, 'TEST/1Min/Tick')
Out[10]: {'responses': None}

In [11]: cli.query(pymkts.Params('TEST', '1Min', 'Tick')).first().df()
Out[11]:
                            Ask
Epoch
2017-01-01 00:00:00+00:00  10.0

```

### Command-line
The `mkts` cli tool included with the project and built with
`make all` allows a user to write/read data to time series buckets.
Use the `runtest.sh` wrapper under `cmd/tools/mkts/examples` to see some examples of its usage.

This test script will create a bucket, load example tick data from a csv into the bucket, and run a simple query.

The last few lines of output should match the following:

```
=============================  ==========  ==========  ==========  
                        Epoch  Bid         Ask         Nanoseconds  
=============================  ==========  ==========  ==========  
2016-12-31 02:37:57 +0000 UTC  1.05185     1.05197     139999810   
2016-12-31 02:38:02 +0000 UTC  1.05185     1.05198     389999832   
2016-12-31 02:38:09 +0000 UTC  1.05188     1.052       389999583   
2016-12-31 02:38:09 +0000 UTC  1.05189     1.05201     889999385   
2016-12-31 02:38:10 +0000 UTC  1.05186     1.05197     139999706   
2016-12-31 02:38:10 +0000 UTC  1.05186     1.05192     389999188   
2016-12-31 02:38:10 +0000 UTC  1.05181     1.05189     639999508   
2016-12-31 02:38:10 +0000 UTC  1.05182     1.0519      889999829   
2016-12-31 02:38:11 +0000 UTC  1.05181     1.05189     389999631   
2016-12-31 02:38:18 +0000 UTC  1.0518      1.0519      139999900   
=============================  ==========  ==========  ==========  
Elapsed parse time: 19.523 ms
Elapsed query time: 4.707 ms
```


## Plugins
Go plugin architecture works best with Go1.10+ on linux. For more on plugins, see the [plugins package](./contrib/plugins/) Some featured plugins are covered here -

### Streaming
You can receive realtime bars updates through the WebSocket streaming feature. The
db server accepts a WebSocket connection on `/ws`, and we have built a plugin that
pushes the data.  Take a look at [the package](./contrib/stream/)
for more details.

### GDAX Data Feeder
The batteries are included so you can start pulling crypto price data from [GDAX](https://docs.gdax.com/#get-historic-rates)
right after you install MarketStore. Then you can query DataFrame content
over the network at as low latency as your local HDF5 files from disk, and
appending new data to the end is two orders of magnitude faster than
DataFrame would be.  This is because the storage format is optimized for
the type of data and use cases as well as for modern filesystem/hardware
characteristics.

You can start pulling data from GDAX if you configure the data poller.
For more information, see [the package](./contrib/gdaxfeeder/)

### On-Disk Aggregation
This plugin allows you to only worry about writing tick/minute level data. This plugin handles time-based aggregation
on disk. For more, see [the package](./contrib/ondiskagg/)


## Development
If you are interested in improving MarketStore, you are more than welcome! Just file issues or requests in github or contact oss@alpaca.markets. Before opening a PR please be sure tests pass-

``` sh
make unittest
```

### Plugins Development
We know the needs and requirements in this space are diverse.  MarketStore
provides strong core functionality with flexible plug-in architecture.
If you want to build your own, look around [plugins](./plugins/)
