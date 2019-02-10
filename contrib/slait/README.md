# Slait Subscriber

This module builds a MarketStore background worker which subscribes to Slait (https://github.com/alpacahq/slait) and writes the data pushed through the websocket connection to the MarketStore database on disk.

## Configuration
slait.so comes with the server by default, so you can simply configure it in the MarketStore configuration file.

### Options
Name | Type | Default | Description
--- | --- | --- | ---
endpoint | string | none | Slait queue address to connect to
topic | string | none | Designates a slait data flow category
attribute_group | string | none | The MarketStore attribute group
shape | [string : string] map | none | Shape of data to write

#### Topic
Specifies the topic of data that the plugin should subscribe to. Note that this will subscribe to all partitions under a specific topic. See the Slait documentation in the link above to understand more about how topics and partitions work.

#### Attribute Group
Specifies the type of data that MarketStore is going to store on disk. This is the same attribute group as in the TimeBucketKey. For OHLCV bars, the TimeBucketKey will look like "BTC-USD/1Min/OHLCV" where OHLCV is the attribute group.

#### Shape
This needs to match the on-disk shape of the `attribute_group` in the MarketStore database. The first entry of each tuple is the column name, and the second is the data type. Note only the following data types are supported in this plugin as of now [int32, int64, float32, float64].

### Example
Add the following to your config file:
```
bgworkers:
  - module: slait.so
    config:
        endpoint: localhost:5000
        topic: bars
        partitions:
        - - gdax
          - D  <- means variable length
        - - binance
          - 10Sec
        - - nasdq
          - 1Min
        attribute_group: OHLCV
        shape:
        - - Epoch
          - int64
        - - Open
          - float64
        - - High
          - float64
        - - Low
          - float64
        - - Close
          - float64
        - - Volume
          - float64
```

## Build
If you need to change the subscriber, you can build it by:

```
make configure
make all
```

It installs the new .so file to the first GOPATH/bin directory.


## Caveat
Since this is implemented based on the Go's plugin mechanism, it is supported only
on Linux as of Go 1.9
