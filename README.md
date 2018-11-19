# MarketStore
![Build Status](https://circleci.com/gh/alpacahq/marketstore/tree/master.png?7989cb00be70f055e0cb19184b212a8ed21b0cbb) [![GoDoc](http://img.shields.io/badge/godoc-reference-blue.svg)](https://godoc.org/github.com/alpacahq/marketstore) [![chatroom icon](https://patrolavia.github.io/telegram-badge/chat.png)](https://t.me/joinchat/HKxN3BGm6mE5YBt79CMM3Q)

Read this in [日本語(Japanese)](README.ja.md)

## Introduction
MarketStore is a database server optimized for financial time-series data.
You can think of it as an extensible DataFrame service that is accessible from anywhere in your system, at higher scalability.

It is designed from the ground up to address scalability issues around handling large amounts of financial market data used in algorithmic trading backtesting, charting, and analyzing price history with data spanning many years, and granularity down to tick-level for the all US equities or the exploding crypto currencies space. If you are struggling with managing lots of HDF5 files, this is perfect solution to your problem.

The batteries are included with the basic install - you can start pulling crypto price data from [GDAX](https://docs.gdax.com/#get-historic-rates) and writing it to the db with a simple [plugin](#plugins) configuration.

MarketStore enables you to query DataFrame content over the network at as low latency as your local HDF5 files from disk, and appending new data to the end is two orders of magnitude faster than DataFrame would be. This is because the storage format is optimized for the type of data and use cases as well as for modern filesystem/hardware
characteristics.

MarketStore is production ready! At [Alpaca](https://alpaca.markets) it has been used in production for years in serious business. If you encounter a bug or are interested in getting involved, please see the [contribution section](#development) for more details.

## Install

### Docker
If you want to get started right away, you can bootstrap a marketstore db instance using our latest [docker image](https://hub.docker.com/r/alpacamarkets/marketstore/tags/). The image comes pre-loaded with the default mkts.yml file and declares the VOLUME `/data`, as its root directory. To run the container with the defaults:
``` sh
docker run -i -p 5993:5993 alpacamarkets/marketstore:latest
```

If you want to run a custom `mkts.yml` with your instance, you can create a new container, load your mkts.yml file into it, then run it.
``` sh
docker create --name mktsdb -p 5993:5993 alpacamarkets/marketstore:latest
docker cp mkts.yml mktsdb:/etc/mkts.yml
docker start -i mktsdb
```

Open a session with your running docker instance using
``` sh
marketstore connect --url localhost:5993
```

### Source
MarketStore is implemented in Go (with some CGO), so you can build it from
source pretty easily. You need Go 1.11+ as it uses `go mod` to manage dependencies.
``` sh
go get -u github.com/alpacahq/marketstore
```
and then in the repo directory, install dependencies using
``` sh
make vendor
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
You can list available commands by running
```
marketstore
```
or
```
$GOPATH/bin/marketstore
```
depending on your GOPATH.

You can create a new configuration file named `mkts.yml`, populated with defaults by running:
```
$GOPATH/bin/marketstore init
```
and then start the marketstore server with:
```
$GOPATH/bin/marketstore start
```

The output will look something like:
```
example@alpaca:~/go/bin/src/github.com/alpacahq/marketstore$ marketstore
I0619 16:29:30.102101    7835 log.go:14] Disabling "enable_last_known" feature until it is fixed...
I0619 16:29:30.102980    7835 log.go:14] Initializing MarketStore...
I0619 16:29:30.103092    7835 log.go:14] WAL Setup: initCatalog true, initWALCache true, backgroundSync true, WALBypass false:
I0619 16:29:30.103179    7835 log.go:14] Root Directory: /example/go/bin/src/github.com/alpacahq/marketstore/project/data/mktsdb
I0619 16:29:30.144461    7835 log.go:14] My WALFILE: WALFile.1529450970104303654.walfile
I0619 16:29:30.144486    7835 log.go:14] Found a WALFILE: WALFile.1529450306968096708.walfile, entering replay...
I0619 16:29:30.244778    7835 log.go:14] Beginning WAL Replay
I0619 16:29:30.244861    7835 log.go:14] Partial Read
I0619 16:29:30.244882    7835 log.go:14] Entering replay of TGData
I0619 16:29:30.244903    7835 log.go:14] Replay of WAL file /example/go/bin/src/github.com/alpacahq/marketstore/project/data/mktsdb/WALFile.1529450306968096708.walfile finished
I0619 16:29:30.289401    7835 log.go:14] Finished replay of TGData
I0619 16:29:30.340760    7835 log.go:14] Launching rpc data server...
I0619 16:29:30.340792    7835 log.go:14] Initializing websocket...
I0619 16:29:30.340814    7835 plugins.go:14] InitializeTriggers
I0619 16:29:30.340824    7835 plugins.go:42] InitializeBgWorkers
```

## Configuration
In order to run MarketStore, a YAML config file is needed. A default file (mkts.yml) can be created using `marketstore init`. The path to this file is passed in to the `start` command with the `--config` flag, or by default it finds a file named mkts.yml in the directory it is running from.

### Options
Var | Type | Description
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

### Default mkts.yml
```
root_directory: data
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
python client. Make sure that in another terminal, you have marketstore running

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
Connect to a marketstore instance with
```
// For a local db-
marketstore connect --dir <path>
// For a server-
marketstore connect --url <address>
```
and run commands through the sql session.

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
