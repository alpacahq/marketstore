# MarketStore

MarketStore is a database server optimized for financial timeseries data.
You can think it as a DataFrame service extensible and accessible from
anywhere in your system, at higher scalability.

It is designed from ground to address scalability issue to handle large
amount of financial market data, in such as algorithm trading backtesting,
charting, and analyzing price history with many year amount of data like
tick-level for the entire US equities or exploding crypto currencies
space.  If you are struggling with managing lots of HDF5 files, this is
perfect solution to your problem.

The battery is inside so you can start pulling crypto price data from GDAX
right after you install MarketStore. Then you can query DataFrame content
over network at as low latency as your local HDF5 files from disk, and
appending new data to the end is two orders of magnitude faster than
DataFrame would be.  This is because the storage format is optimized for
the type of data and use cases as well as for modern filesystem/hardware
characteristics.


## Install

MarketStore is in pure Go (with some CGO code), so you can build it from
source pretty easily.  If you want to start right away, use our docker image.

## Build From Source

### Prerequisite

You need go 1.9+ and [glide](http://glide.sh/).

```
$ brew install glide
```

or

```
$ go install github.com/Masterminds/glide
```

```
$ mkdir -p /go/src/github.com/alpacahq
$ cd /go/src/github.com/alpacahq
$ git clone https://github.com/alpacahq/marketstore.git
$ cd marketstore
$ make configure
$ make all plugins
```

### Test

```
$ make unittest
```


## Tutorial

### Python Client
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

### Command-Line Client mkts

Let's test out marketstore by running the ```runtest.sh``` example under ```cmd/tools/mkts/examples```.

This test script will create a bucket, load example tick data into the bucket, and run a simple query.

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

## Configuration

In order to run MarketStore, a configuration .yaml file is needed. A default file is included in the codebase
above and is called mkts_config.yaml. This path to this file is passed in to the launcher binary with the
'-config' flag, or by default it finds a file with that name in the directory it is running from. This file
should look as follows:

```shell
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

* __root_directory__: allows the user to specify the directory in which the MarketStore database resides (string)
* __listen_port__: specifies the port that MarketStore will serve through (integer)
* __timezone__: system timezone by name of TZ database (e.g. America/New_York) default=UTC
* __log_level__: allows the user to specify the log level (string: info, warning, error)
* __queryable__: allows the user to run MarketStore in polling-only mode, where it will not respond to query (bool)
* __stop_grace_period__: sets the amount of time MarketStore will wait to shutdown after a SIGINT signal is received (integer: seconds)
* __wal_rotate_interval__: frequency at which the WAL file will be trimmed after being flushed to disk (integer, minutes)
* __stale_threshold__: threshold by which MarketStore will declare a symbol stale (integer, days)
* __enable_add__: flag allowing new symbols to be added to DB via /write API
* __enable_remove__: flag allowing symbols to be removed from DB via /write API
* __triggers__: list of trigger plugins
* __bgworkers__: list of background worker plugins


## GDAX Data Fetcher

You can start pulling data from GDAX if you configure the data poller.
For more information, see [GDAX Feeder Document](./contrib/gdaxfeeder/)

## On-Disk Aggregate

You only need to collect tick/minute level data.  Time-based aggregation
on disk can be done via [On-Disk Aggregate](./contrib/ondiskagg/)


## Plug-in Architecture

We know the needs and requirements in this space is diverse.  MarketStore
server provides strong core functionality with flexible plug-in architecture.
If you want to build your own, look around [plugins](./plugins/)

## Bug Report & Contribution

If you are interested in improving MarketStore, more than welcome!  Just file issues or request in github or contact oss@alpaca.markets.

## Is This Production-Ready?

Yes, absolutely!  It has been used in production for years in serious business.
But we also never felt this is complete.  You can use it for your purpose
and give more feedback.
