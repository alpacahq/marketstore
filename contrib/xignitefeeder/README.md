# Xignite Feeder

* This plugin retrieves quotes data for all symbols in (a) specified exchange(s) by [QUICK Xignite API](https://www.marketdata-cloud.quick-co.jp/Products/) and store it to the local marketstore server.
* You need an API token to call Xignite API. Register to Xignite and generate your API token first.
* This plugin is also able to collect daily candlestick chart data from a specified date and backfill it to the local marketstore. this historical backfill process can be executed when marketstore is started and the configurated time everyday (see `updatingHour` configuration )

## Example configuration
```yaml
bgworkers:
  # -----------------------
  # XigniteFeeder gets the realtime stock data by Xignite API (https://www.quick.co.jp/qxapis/index.php)
  # and writes its to the local marketstore at fixed intervals.
  # -----------------------
  - module: xignitefeeder.so
    config:
      # exchange list
      exchanges:
        - XTKS # Tokyo Stock Exchange
        - XJAS # Jasdaq
        #- XNGO # Nagoya Stock Exchange
        #- XSAP # Sapporo Stock Exchange
        #- XFKA # Fukuoka Stock Exchange
        #- XTAM # Tokyo PRO Market
      # Xignite feeder also retrieves data of Index Symbols (ex. ＴＯＰＩＸ（東証１部株価指数）) every day.
      # To get target indices, index groups that the indices belong are necessary.
      # (cf. https://www.marketdata-cloud.quick-co.jp/Products/QUICKIndexHistorical/Overview/ListSymbols )
      index_groups:
        - INDXJPX # JAPAN EXCHANGE GROUP
        - IND_NIKKEI # NIKKEI INDICES
      # time when target symbols in the exchanges are updated everyday.
      # this time is also used for the historical data backfill (UTC)
      updatingHour: 22 # (UTC). = every day at 07:00:00 (JST)
      # XigniteFeeder writes data to "{identifier}/{timeframe}/TICK" TimeBucketKey
      timeframe: "1Sec"
      # Auth token for Xignite API
      token: "D***0"
      # Timeout [sec] for Xignite API
      timeout: 10
      # Interval [sec] to call Xignite API
      interval: 10
      # XigniteFeeder runs from openTime ~ closeTime (UTC)
      openTime: "23:00:00" # 08:00 (JST)
      closeTime: "06:10:00" # 15:10 (JST)
      # XigniteFeeder doesn't run on the following days
      closedDaysOfTheWeek:
        - "Saturday"
        - "Sunday"
      # XigniteFeeder doesn't run on the closed dates (in JST)
      # (cf. https://www.jpx.co.jp/corporate/about-jpx/calendar/ )
      closedDays:
        - "2019-01-01"
        - "2019-01-02"
        - "2019-01-03"
        - "2019-01-14"
        - "2019-02-11"
        - "2019-03-21"
        - "2019-04-29"
        - "2019-04-30"
        - "2019-05-01"
        - "2019-05-02"
        - "2019-05-03"
        - "2019-05-04"
        - "2019-05-05"
        - "2019-05-06"
        - "2019-07-15"
        - "2019-08-11"
        - "2019-08-12"
        - "2019-09-16"
        - "2019-09-23"
        - "2019-10-14"
        - "2019-10-22"
        - "2019-11-03"
        - "2019-11-04"
        - "2019-11-23"
        - "2019-12-31"
        - "2020-01-01"
        - "2020-01-02"
        - "2020-01-03"
        - "2020-01-13"
        - "2020-02-11"
        - "2020-02-23"
        - "2020-02-24"
        - "2020-03-20"
        - "2020-04-29"
        - "2020-05-03"
        - "2020-05-04"
        - "2020-05-05"
        - "2020-05-06"
        - "2020-07-23"
        - "2020-07-24"
        - "2020-08-10"
      # if backfill is enabled, historical daily chart data for all symbols in the target exchanges
      # are aggregated using Xignite API (=GetQuotesRange endpoint) and stored to "{symbol}/{timeframe}/OHLCV" bucket.
      backfill:
        enabled: true
        since: "2008-01-01"
        timeframe: "1D"
      # In addition to the daily-chart backfill above,
      # Xignite Feeder can feed 5-minute chart data of the target symbols for the past X business days. The data is stored to {symbol}/{timeframe}/OHLCV bucket (e.g. "1400/5Min/OHLCV" )
      recentBackfill:
        enabled: false
        days: 7 # Xignite Feeder feeds the data for {days} business days
        timeframe: "5Min"
```

# Build

## Run
```bash
DaitonoMacBook-puro:marketstore[xignitefeeder]$ pwd
/Users/dakimura/go/src/github.com/dakimura/marketstore

DaitonoMacBook-puro:marketstore[xignitefeeder]$ make plugins
(...)
go build -o /Users/dakimura/go/bin/iex.so -buildmode=plugin .
/Library/Developer/CommandLineTools/usr/bin/make -C contrib/xignitefeeder
go build -o /Users/dakimura/go/bin/xignitefeeder.so -buildmode=plugin .
go: finding github.com/modern-go/concurrent latest

DaitonoMacBook-puro:marketstore[xignitefeeder]$ ./marketstore start
{"level":"info","timestamp":"2019-05-16T10:14:03.586+0900","msg":"Running single threaded"}
(...)
{"level":"debug","timestamp":"2019-05-16T10:14:27.043+0900","msg":"[Xignite API] Delay(sec) in GetQuotes response= 0.297678"}
{"level":"debug","timestamp":"2019-05-16T10:14:27.308+0900","msg":"[Xignite API] request url=https://api.marketdata-cloud.quick-co.jp/QUICKEquityRealTime.json/GetQuotes"}
{"level":"debug","timestamp":"2019-05-16T10:14:27.437+0900","msg":"Data has been saved to marketstore successfully."}
{"level":"debug","timestamp":"2019-05-16T10:14:27.993+0900","msg":"[Xignite API] Delay(sec) in GetQuotes response= 0.240362"}
{"level":"debug","timestamp":"2019-05-16T10:14:28.310+0900","msg":"[Xignite API] request url=https://api.marketdata-cloud.quick-co.jp/QUICKEquityRealTime.json/GetQuotes"}
{"level":"debug","timestamp":"2019-05-16T10:14:28.402+0900","msg":"Data has been saved to marketstore successfully."}
```

## Check the stored data
```bash
DaitonoMacBook-puro:marketstore[xignitefeeder]$ ./marketstore connect --url localhost:5993
{"level":"info","timestamp":"2019-05-16T10:18:43.751+0900","msg":"Running single threaded"}
Connected to remote instance at: http://localhost:5993
Type `\help` to see command options
» \show 7203/1Sec/TICK 1970-01-01
=============================  ==========  ==========  
                        Epoch  Ask         Bid         
=============================  ==========  ==========  
2019-05-15 09:49:47 +0000 UTC    6496        6495        
2019-05-15 09:49:52 +0000 UTC    6496        6495        
2019-05-15 09:49:53 +0000 UTC    6496        6495        
2019-05-15 09:49:54 +0000 UTC    6496        6495        
2019-05-15 09:49:56 +0000 UTC    6496        6495        
2019-05-15 09:49:57 +0000 UTC    6496        6495        
2019-05-15 09:49:58 +0000 UTC    6496        6495 
(...)

# to get the backfill data...
» \show 7203/1D/OHLCV 1970-01-01
(...)
```
