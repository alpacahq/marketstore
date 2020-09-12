# MarketStore
[![CircleCI](https://circleci.com/gh/alpacahq/marketstore.svg?style=shield)](https://circleci.com/gh/alpacahq/marketstore) [![GoDoc](http://img.shields.io/badge/godoc-reference-blue.svg)](https://godoc.org/github.com/alpacahq/marketstore) [![chatroom icon](https://patrolavia.github.io/telegram-badge/chat.png)](https://t.me/joinchat/HKxN3BGm6mE5YBt79CMM3Q)
[![codecov](https://codecov.io/gh/alpacahq/marketstore/branch/master/graph/badge.svg)](https://codecov.io/gh/alpacahq/marketstore)


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
```sh
docker run -i -p 5993:5993 alpacamarkets/marketstore:latest
```

If you want to run a custom `mkts.yml` you can create a new container
and load your mkts.yml file into it:
```sh
docker create --name mktsdb -p 5993:5993 alpacamarkets/marketstore:latest
docker cp mkts.yml mktsdb:/etc/mkts.yml
docker start -i mktsdb
```

You can also [bind mount](https://docs.docker.com/storage/bind-mounts/)
the container to a local host config file: a custom `mkts.yml`:
```sh
docker run -v /full/path/to/mkts.yml:/etc/mkts.yml -i -p 5993:5993 alpacamarkets/marketstore:latest
```
This allows you to test out the image [included
plugins](https://github.com/alpacahq/marketstore/tree/master/plugins#included)
with ease if you prefer to skip the copying step suggested above.

By default the container will not persist any written data to your
container's host storage. To accomplish this, bind the `data` directory to
a local location:
```sh
docker run -v "/path/to/store/data:/data" -i -p 5993:5993 alpacamarkets/marketstore:latest
```
Once data is written to the server you should see a file tree layout
like the following that will persist across container runs:
```sh
>>> tree /<path_to_data>/marketstore
/<path_to_data>/marketstore
├── category_name
├── WALFile.1590868038674814776.walfile
├── SYMBOL_1
├── SYMBOL_2
├── SYMBOL_3
```

If you have built the
[cmd](https://github.com/alpacahq/marketstore/tree/master/cmd) package
locally, you can open a session with your running docker instance using:
```sh
marketstore connect --url localhost:5993
```

### Source
MarketStore is implemented in Go, so you can build it from
source pretty easily. You need Go 1.11+ as it uses `go mod` to manage dependencies.
``` sh
go get -u github.com/alpacahq/marketstore
```
then compile and install the project binaries using
``` sh
make install
```
Optionally, you can install the project's included plugins using
``` sh
make plugins
```

### Homebrew on macOS

You can also install marketstore using the [Homebrew](https://brew.sh)
package manager for macOS.

```sh
$ brew tap zjhmale/marketstore
$ brew install --HEAD marketstore
```

To upgrade marketstore in the future, use `upgrade` instead of `install`.

Then you are equipped with marketstore service plist also

```sh
$ brew services start marketstore
$ brew services stop marketstore
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
listen_port | int | Port that MarketStore will serve through for JSON-RPC API
grpc_listen_port | int | Port that MarketStore will serve through for GRPC API
timezone | string | System timezone by name of TZ database (e.g. America/New_York)
log_level | string  | Allows the user to specify the log level (info | warning | error)
queryable | bool | Allows the user to run MarketStore in polling-only mode, where it will not respond to query
stop_grace_period | int | Sets the amount of time MarketStore will wait to shutdown after a SIGINT signal is received
wal_rotate_interval | int | Frequency (in mintues) at which the WAL file will be trimmed after being flushed to disk  
stale_threshold | int | Threshold (in days) by which MarketStore will declare a symbol stale
enable_add | bool | Allows new symbols to be added to DB via /write API
enable_remove | bool | Allows symbols to be removed from DB via /write API  
disable_variable_compression | bool | disables the default compression of variable data
triggers | slice | List of trigger plugins
bgworkers | slice | List of background worker plugins

### Default mkts.yml
```yml
root_directory: data
listen_port: 5993
grpc_listen_port: 5995
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
* query data
```python
import pymarketstore as pymkts
param = pymkts.Params('BTC', '1Min', 'OHLCV', limit=10)
cli = pymkts.Client()
reply = cli.query(param)
reply.first().df()
```
shows
```python
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
```
* write data
```python
import numpy as np
import pandas as pd
data = np.array([(pd.Timestamp('2017-01-01 00:00').value / 10**9, 10.0)], dtype=[('Epoch', 'i8'), ('Ask', 'f4')])
cli.write(data, 'TEST/1Min/Tick')
# Out[10]: {'responses': None}

cli.query(pymkts.Params('TEST', '1Min', 'Tick')).first().df()
```
shows
```python
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
Go plugin architecture works best with Go1.10+ on linux. For more on plugins, see the [plugins package](./plugins/) Some featured plugins are covered here -

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


## Replication
You can replicate data from a master instance to other marketstore instances. 
In `mkts.yml` config file, please set the below config:

- master instance
```
replication:
  # when enabled=true, this instance works as master instance and accept connections from replicas
  enabled: true
  # port to be used for the replication protocol
  listen_port: 5996
```

- replica instance(s)
```
replication:
  # when master_host is set, this instance works as a replica instance
  master_host: "127.0.0.1:5995"
```

### limitations
- Currently, the replication connection is initialized only when the marketstore (on the replica instance) is started.
Please start the master instance first when you want to replicate data.

- Currently, only `write` API result is supported. `delete` API result won't be reflected to replica instances.


## Development
If you are interested in improving MarketStore, you are more than welcome! Just file issues or requests in github or contact oss@alpaca.markets. Before opening a PR please be sure tests pass-

``` sh
make unittest
```

### Plugins Development
We know the needs and requirements in this space are diverse.  MarketStore
provides strong core functionality with flexible plug-in architecture.
If you want to build your own, look around [plugins](./plugins/)
