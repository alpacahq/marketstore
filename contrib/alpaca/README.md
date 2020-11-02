# Alpaca Data Connector

This module builds a MarketStore background worker which current
price data of US stocks from [Alpaca's streaming market data API](https://alpaca.markets/docs/api-documentation/api-v2/market-data/streaming/).
It runs as a goroutine behind the MarketStore process and keeps writing to the disk.
The module uses the websocket streaming interface (to receive real-time updates).


## Configuration
alpaca.so comes with the server by default, so you can simply configure it
in the MarketStore configuration file.

### Options
Name | Type | Default | Description
--- | --- | --- | ---
api_key | string | none | Your alpaca api key id
api_secret | string | none | The secret corresponding to your api_key
ws_server | string | wss://data.alpaca.markets/stream | The websocket server to connect to
ws_worker_count | int | 10 | The number of workers to use for WS message processing
minute_bar_symbols | slice of strings | none | The symbols to retrieve minute bars for
quote_symbols | slice of strings | none | The symbols to retrieve quotes for
trade_symbols | slice of strings | none | The symbols to retrieve trades for

(bars, quotes, trades)

### Example
Add the following to your config file:
```
bgworkers:
  - module: alpaca.so
    config:
      api_key: your_alpaca_key_id
      api_secret: your_alpaca_secret
      ws_server: wss://data.alpaca.markets/stream
      ws_worker_count: 10
      minute_bar_symbols:
        - '*'
      quote_symbols:
        - VOO
        - SPY
      trade_symbols:
        - AAPL
```
