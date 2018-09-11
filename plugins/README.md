# Plugin Development

MarketStore exposes interfaces that allow for third-party Go plugin module integrations. The interfaces come in two flavors, a `trigger` plugin and a `bgworker` plugin.

Third-party plugins can be built as `.so` bundles, using the Go `build` command using the `-buildmode=plugin` flag and placed in the $GOPATH/bin directory. Once there, they can be referenced in the MarketStore YAML config file that is supplied to the `marketstore` startup cmd via the `triggers` or `bgworkers` flags.

Plugins, when included and configured in the MarketStore YAML config, are booted up on startup with the `marketstore` command. The included `mkts.yml` file shows some commented-out examples of configuration.

## Trigger
Triggers are small applications that perform an action when data is written to the db that matches certain parameters. A trigger interface has to implement the following function -
```go
NewTrigger(config map[string]interface{}) (Trigger, error)
```
The trigger instance returned by this function will be called on Fire() with the filePath (relative to root directory) and indexes that have been written (appended or updated). It is guaranteed that the new content has been written on disk when Fire() is called, so it is safe to read it from disk. Keep in mind that the trigger might be called on the startup, due to the WAL recovery.

### Config example
```
triggers:
  - module: xxxTrigger.so
    on: "*/1Min/OHLCV"
    config: <according to the plugin>
```
The "on" value is matched with the file path to decide whether the trigger is fired or not. It can contain wildcard character "*". As of now, trigger fires only on the running state. Trigger on WAL replay may be added later.

### Included
* [On-disk-aggregation](https://github.com/alpacahq/marketstore/tree/master/contrib/ondiskagg) - updates the downsample data upon the writes on the underlying timeframe.
* [Streaming](https://github.com/alpacahq/marketstore/tree/master/contrib/stream) - pushes data through MarketStore's streaming interface.


## BgWorker
Small applications that run as independent processes in the background and can perform tick data transactions on the database. A bgworker interface has to implement the following function -
```go
NewBgWorker(config map[string]interface{}) (BgWorker, error)
```

Background workers run under the MarketStore server by implementing the
interface, started at the very beginning of the server lifecycle before the
query interface starts. The MarketStore server does not handle panics that happen within the plugin.  A plugin can recover from panics, but should be careful not to screw the MarketStore server state if touching internal API.  It is often better to just let it go.

### Config Example
```
bgworkers:
  - module: xxxWorker.so
    name: datafeed
    config: <according to the plulgin>
```

### Included
* [Slait](https://github.com/alpacahq/marketstore/tree/master/contrib/slait) - subscribes to Slait (https://github.com/alpacahq/slait) and writes the data pushed through the websocket connection to the MarketStore database on disk.
* [GDAXFeeder](https://github.com/alpacahq/marketstore/tree/master/contrib/gdaxfeeder) - fetches historical price data of cryptocurrencies from GDAX public API.
* [Polygon](https://github.com/alpacahq/marketstore/tree/master/contrib/polygon) - fetches historical
price data of US stocks from [Polygon's API](https://polygon.io/).
