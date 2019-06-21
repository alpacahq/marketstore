### go-binance

A Golang SDK for [binance](https://www.binance.com) API.

[![Build Status](https://travis-ci.org/adshao/go-binance.svg?branch=master)](https://travis-ci.org/adshao/go-binance)
[![GoDoc](https://godoc.org/github.com/adshao/go-binance?status.svg)](https://godoc.org/github.com/adshao/go-binance)
[![Go Report Card](https://goreportcard.com/badge/github.com/adshao/go-binance)](https://goreportcard.com/report/github.com/adshao/go-binance)
[![codecov](https://codecov.io/gh/adshao/go-binance/branch/master/graph/badge.svg)](https://codecov.io/gh/adshao/go-binance)

All the REST APIs listed in [binance API document](https://github.com/binance-exchange/binance-official-api-docs) are implemented, as well as the websocket APIs.

For best compatibility, please use Go >= 1.8.

Make sure you have read binance API document before continuing.

### Installation

```shell
go get github.com/adshao/go-binance
```

### Importing

```golang
import (
    "github.com/adshao/go-binance"
)
```

### Documentation

[![GoDoc](https://godoc.org/github.com/adshao/go-binance?status.svg)](https://godoc.org/github.com/adshao/go-binance)

### REST API

#### Setup

Init client for API services. Get APIKey/SecretKey from your binance account.

```golang
var (
    apiKey = "your api key"
    secretKey = "your secret key"
)
client := binance.NewClient(apiKey, secretKey)
```

A service instance stands for a REST API endpoint and is initialized by client.NewXXXService function.

Simply call API in chain style. Call Do() in the end to send HTTP request.

Following are some simple examples, please refer to [godoc](https://godoc.org/github.com/adshao/go-binance) for full references.

#### Create Order

```golang
order, err := client.NewCreateOrderService().Symbol("BNBETH").
        Side(binance.SideTypeBuy).Type(binance.OrderTypeLimit).
        TimeInForce(binance.TimeInForceGTC).Quantity("5").
        Price("0.0030000").Do(context.Background())
if err != nil {
    fmt.Println(err)
    return
}
fmt.Println(order)

// Use Test() instead of Do() for testing.
```

#### Get Order

```golang
order, err := client.NewGetOrderService().Symbol("BNBETH").
    OrderID(4432844).Do(context.Background())
if err != nil {
    fmt.Println(err)
    return
}
fmt.Println(order)
```

#### Cancel Order

```golang
_, err := client.NewCancelOrderService().Symbol("BNBETH").
    OrderID(4432844).Do(context.Background())
if err != nil {
    fmt.Println(err)
    return
}
```

#### List Open Orders

```golang
openOrders, err := client.NewListOpenOrdersService().Symbol("BNBETH").
    Do(context.Background())
if err != nil {
    fmt.Println(err)
    return
}
for _, o := range openOrders {
    fmt.Println(o)
}
```

#### List Orders

```golang
orders, err := client.NewListOrdersService().Symbol("BNBETH").
    Do(context.Background())
if err != nil {
    fmt.Println(err)
    return
}
for _, o := range orders {
    fmt.Println(o)
}
```

#### List Ticker Prices

```golang
prices, err := client.NewListPricesService().Do(context.Background())
if err != nil {
    fmt.Println(err)
    return
}
for _, p := range prices {
    fmt.Println(p)
}
```

#### Show Depth

```golang
res, err := client.NewDepthService().Symbol("LTCBTC").
    Do(context.Background())
if err != nil {
    fmt.Println(err)
    return
}
fmt.Println(res)
```

#### List Klines

```golang
klines, err := client.NewKlinesService().Symbol("LTCBTC").
    Interval("15m").Do(context.Background())
if err != nil {
    fmt.Println(err)
    return
}
for _, k := range klines {
    fmt.Println(k)
}
```

#### List Aggregate Trades

```golang
trades, err := client.NewAggTradesService().
    Symbol("LTCBTC").StartTime(1508673256594).EndTime(1508673256595).
    Do(context.Background())
if err != nil {
    fmt.Println(err)
    return
}
for _, t := range trades {
    fmt.Println(t)
}
```

#### Get Account

```golang
res, err := client.NewGetAccountService().Do(context.Background())
if err != nil {
    fmt.Println(err)
    return
}
fmt.Println(res)
```

#### Start User Stream

```golang
res, err := client.NewStartUserStreamService().Do(context.Background())
if err != nil {
    fmt.Println(err)
    return
}
fmt.Println(res)
```

### Websocket

You don't need Client in websocket API. Just call binance.WsXxxServe(args, handler, errHandler).

#### Depth

```golang
wsDepthHandler := func(event *binance.WsDepthEvent) {
    fmt.Println(event)
}
errHandler := func(err error) {
    fmt.Println(err)
}
doneC, stopC, err := binance.WsDepthServe("LTCBTC", wsDepthHandler, errHandler)
if err != nil {
    fmt.Println(err)
    return
}
// use stopC to exit
go func() {
    time.Sleep(5 * time.Second)
    stopC <- struct{}{}
}()
// remove this if you do not want to be blocked here
<-doneC
```

#### Kline

```golang
wsKlineHandler := func(event *binance.WsKlineEvent) {
    fmt.Println(event)
}
errHandler := func(err error) {
    fmt.Println(err)
}
doneC, _, err := binance.WsKlineServe("LTCBTC", "1m", wsKlineHandler, errHandler)
if err != nil {
    fmt.Println(err)
    return
}
<-doneC
```

#### Aggregate

```golang
wsAggTradeHandler := func(event *binance.WsAggTradeEvent) {
    fmt.Println(event)
}
errHandler := func(err error) {
    fmt.Println(err)
}
doneC, _, err := binance.WsAggTradeServe("LTCBTC", wsAggTradeHandler, errHandler)
if err != nil {
    fmt.Println(err)
    return
}
<-doneC
```

#### User Data

```golang
wsHandler := func(message []byte) {
    fmt.Println(string(message))
}
errHandler := func(err error) {
    fmt.Println(err)
}
doneC, _, err := binance.WsUserDataServe(listenKey, wsHandler, errHandler)
if err != nil {
    fmt.Println(err)
    return
}
<-doneC
```
