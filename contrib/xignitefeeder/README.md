# Xignite Feeder

* This plugin retrieves quotes data by [QUICK Xignite API](https://www.marketdata-cloud.quick-co.jp/Products/) and store it to the local marketstore server. 
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
      # time when target symbols in the exchanges are updated everyday.
      # this time is also used for the historical data backfill (
      updatingHour: 18 #:00:00
      # XigniteFeeder writes data to "{identifier}/{timeframe}/TICK" TimeBucketKey
      timeframe: "1Sec"
      # Auth token for Xignite API
      token: "D***0"
      # Timeout [sec] for Xignite API
      timeout: 10
      # Interval [sec] to call Xignite API
      interval: 1
      # XigniteFeeder runs from openTime ~ closeTime (UTC)
      openTime: "23:55:00" # 08:55 (JST)
      closeTime: "06:10:00" # 15:10 (JST)
      # XigniteFeeder doesn't run on the following days
      closedDaysOfTheWeek:
        - "Saturday"
        - "Sunday"
      # XigniteFeeder doesn't run on the closed dates (in JST)
      # (cf. https://www.jpx.co.jp/corporate/about-jpx/calendar/ )
      closedDays:
        - "2019/01/01"
        - "2019/01/02"
        - "2019/01/03"
        - "2019/01/14"
        - "2019/02/11"
        - "2019/03/21"
        - "2019/04/29"
        - "2019/04/30"
        - "2019/05/01"
        - "2019/05/02"
        - "2019/05/03"
        - "2019/05/04"
        - "2019/05/05"
        - "2019/05/06"
        - "2019/07/15"
        - "2019/08/11"
        - "2019/08/12"
        - "2019/09/16"
        - "2019/09/23"
        - "2019/10/14"
        - "2019/10/22"
        - "2019/11/03"
        - "2019/11/04"
        - "2019/11/23"
        - "2019/12/31"
        - "2020/01/01"
        - "2020/01/02"
        - "2020/01/03"
        - "2020/01/13"
        - "2020/02/11"
        - "2020/02/23"
        - "2020/02/24"
        - "2020/03/20"
        - "2020/04/29"
        - "2020/05/03"
        - "2020/05/04"
        - "2020/05/05"
        - "2020/05/06"
        - "2020/07/23"
        - "2020/07/24"
        - "2020/08/10"
      # if backfill is enabled, historical daily chart data for all symbols in the target exchanges
      # are aggregated using Xignite API (=GetQuotesRange endpoint) and stored to "{symbol}/{timeframe}/OHLCV" bucket.
      backfill:
        enabled: true
        since: "2008/04/01"
        timeframe: "1D"
```