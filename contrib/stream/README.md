# Streaming Trigger

This module builds a MarketStore trigger which pushes data through MarketStore's
streaming interface. The push is triggered by writes to the on-disk data, both
in the base timeframe as well as the aggregates. Aggregated data entries will be
placed on a 'shelf' and will be given a shelf-life that indicates when they will
be delivered. These entries will be updated with the latest aggregated candle up
until the expiration time.

Note that all data is transmitted with [MessagePack][msgp] encoding.

## Configuration
stream.so comes with the server by default, so you can simply configure it
in MarketStore configuration file.  

### Options
Name | Type | Default | Description
--- | --- | --- | ---
on | string | none | The file glob pattern to match on
filter | string | none | Filters pushes to '1D' timeframes and above based on market hours. Only 'nasdaq' is supported at this time.

### Example
Add the following to your config file:
```
triggers:
  - module: stream.so
    on: */*/*
    config:
        filter: "nasdaq"
```


## Protocol
Streams are organized by time bucket keys, just like the on-disk data is inside
MarketStore. When subscribing to a stream, or many streams, the stream name
should take the form of `<Symbol>/<Timeframe>/<AttributeGroup>`, where any, none
or all of the 3 parts of the composite key can be replaced with * to subscribe to
all of them. For example, to subscribe to all 5Min bars for BTC-USD, one would
simply subscribe to the stream: `BTC-USD/5Min/OHLCV`. If one wanted to subscribe
to all timeframes of BTC-USD, then the stream name would be: `BTC-USD/*/OHLCV`.

Note that to modify your subscription, another subscribe message must be sent
over the websocket connection. The set of streams in the new subscribe message
will replace any previously subscribed streams.

Subscribing with the included GoLang MarketStore client is as simple as:

```
handler := func(pl stream.Payload) error { fmt.Println(string(pl.Data)) }
cancelC := make(chan struct{})
streams := []string{"BTC-USD/*/*", "ETH-USD/1Min/OHLCV"}

done, err := client.Subscribe(handler, cancelC, streams)

if err != nil {
    panic(err)
}

<-done
```

All the messages are encoded in MessagePack. The message flow at low level looks
as follows.

```
<Connection Made on "/ws">
Client: {"streams": ["BTC-USD/*/*", "ETH-USD/1Min/OHLCV"]}
Server: {"streams": ["BTC-USD/*/*", "ETH-USD/1Min/OHLCV"]}
Server: {"key": "ETH-USD/1Min/OHLCV", "data": {'Low': 1088.54, 'Close': 1088.54, 'Volume': 23.002666809999997, 'Epoch': 1516368000, 'Open': 1088.54, 'High': 1088.55}}
Server: {'key': 'BTC/1Min/OHLCV', 'data': {'Epoch': 1516386000, 'Open': 11301.01, 'High': 11301.01, 'Low': 11300.0, 'Close': 11301.01, 'Volume': 28.9793876}}
...
```

If an error occurs during the "streams" request (i.e. the streams format is not
valid), it will return error as below.

```
Server: {"error": "error message for details"}
```

## Build
If you need to change the code, you can build it from this directory by:

```
$ make all
```

It installs the new .so file to the first $GOPATH/bin directory.


## Caveat
Since this is implemented based on the Go's plugin mechanism, it is supported only
on Linux & MacOS as of Go 1.10

[msgp]: https://msgpack.org/index.html
