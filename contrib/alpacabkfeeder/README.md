# Alpaca Broker API Feeder

* This plugin retrieves quotes data for all symbols in (a) specified exchange(s) by [Alpaca Broker API](https://alpaca.markets/docs/broker/market-data/) and store it to the local marketstore server.
* You need an API Key ID and API Secret Key to call Alpaca API. Please sign up for Alpaca and generate your API keys first to use this plugin.
* This plugin is able to collect daily candlestick chart data for specified date ranges and backfill it to the local marketstore too. This historical backfill process is run at marketstore startup and at the configured time everyday (see `update_time` config )

## Example configuration
```yaml
bgworkers:
  # -----------------------
  # AlpacaBrokerAPIFeeder gets the realtime/historical stock data
  # by Alpaca Broker API (https://alpaca.markets/docs/broker/)
  # and writes it to the local marketstore at fixed intervals.
  # -----------------------
  - module: alpacabkfeeder.so
    config:
      # exchange list
      exchanges:
        # - AMEX
        # - ARCA
        # - BATS
        # - NYSE
        - NASDAQ
        # - NYSEARCA
        # - OTC
      # time when target symbols in the exchanges are updated every day.
      # this time is also used for the historical data back-fill.
      # This config can be manually overridden by "ALPACA_BROKER_FEEDER_UPDATE_TIME" environmental variable.
      update_time: "13:30:00" # (UTC). = every day at 08:30:00 (EST)
      # Alpava Broker API Feeder writes data to "{symbol}/{timeframe}/TICK" TimeBucketKey
      timeframe: "1Sec"
      # API Key ID and Secret for Alpaca Broker API
      # This config can be manually overridden by "ALPACA_BROKER_FEEDER_API_KEY_ID" and "ALPACA_BROKER_FEEDER_API_SECRET_KEY"
      # environment variables.
      api_key_id: "foobar"
      api_secret_key: "fizzbuzz"
      # Interval [sec] to call Alpaca Broker API
      interval: 10
      # If a non-zero value is set for off_hours_interval,
      # the data-feeding is executed every off_hours_interval[minute] even when the market is closed.
      off_hours_interval: 5
      # The data-feeding is executed when 'minute' of the current time matches off_hours_schedule
      # even when the market is cloded. Example: "10" -> execute at 00:10, 01:10, 02:10,...,23:10
      # Numbers separated by commas are allowed.  Example: "0,15,30,45" -> execute every 15 minutes.
      # Whitespaces are ignored.
      # If both off_hours_interval and off_hours_schedule are specified at the same time,
      # off_hours_interval will be ignored.
      off_hours_schedule: "0,15,30,45"
      # Alpaca Broker API Feeder runs from openTime ~ closeTime (UTC)
      openTime: "14:30:00" # 14:30(UTC) = 09:30 (EST)
      closeTime: "21:00:00" # 21:00(UTC) = 16:00 (EST)
      # Alpaca Broker API Feeder doesn't run on the following days of the week
      closedDaysOfTheWeek:
        - "Saturday"
        - "Sunday"
      # Alpaca Broker API Feeder doesn't run on the closed dates (in JST)
      # (cf. https://www.jpx.co.jp/corporate/about-jpx/calendar/ )
      closedDays:
        - "2021-12-24"
        - "2022-01-17"
        - "2022-02-21"
        - "2022-04-15"
        - "2022-05-30"
        - "2022-06-20"
        - "2022-07-04"
        - "2022-09-05"
        - "2022-11-24"
        # - "2022-11-25" # 01:00pm(est) early close
        - "2022-12-26"
      # if back-fill is enabled, historical daily chart data for all symbols in the target exchanges
      # are aggregated using Alpaca Broker API (=ListBars endpoint) and stored to "{symbol}/{timeframe}/OHLCV" bucket.
      backfill:
        enabled: true
        since: "2020-01-01"
        timeframe: "1D"
```

# Build

## Run
```bash
$ pwd         
/Users/dakimura/projects/go/src/github.com/alpacahq/marketstore
$ make plugins
(omitted)
/Library/Developer/CommandLineTools/usr/bin/make -C contrib/alpacabkfeeder
GOFLAGS= go build -o /Users/dakimura/projects/go/bin/alpacabkfeeder.so -buildmode=plugin .
$ make build
$ ./marketstore start --config mkts.yml 
(omitted)
{"level":"info","timestamp":"2021-12-17T09:18:53.061+0900","msg":"Trying to load module from path: /Users/dakimura/projects/go/bin/alpacabkfeeder.so...\n"}
{"level":"info","timestamp":"2021-12-17T09:18:53.373+0900","msg":"Success loading module /Users/dakimura/projects/go/bin/alpacabkfeeder.so.\n"}
{"level":"info","timestamp":"2021-12-17T09:18:53.374+0900","msg":"loaded Alpaca Broker Feeder config..."}
```

## Check the stored data
```bash
marketstore$ ./marketstore connect --url localhost:5993
{"level":"info","timestamp":"2019-05-16T10:18:43.751+0900","msg":"Running single threaded"}
Connected to remote instance at: http://localhost:5993
Type `\help` to see command options
# to get the realtime data...
» \show AAPL/1Sec/TICK 1970-01-01
(...)
# to get the backfill data...
» \show AAPL/1D/OHLCV 1970-01-01
(...)
```
