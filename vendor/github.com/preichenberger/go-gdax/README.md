Go GDAX [![GoDoc](http://img.shields.io/badge/godoc-reference-blue.svg)](http://godoc.org/github.com/preichenberger/go-gdax) [![Build Status](https://travis-ci.org/preichenberger/go-gdax.svg?branch=master)](https://travis-ci.org/preichenberger/go-gdax)
========

## Summary

Go client for [CoinBase Pro](https://pro.coinbase.com) formerly known as GDAX

## Installation
```sh
go get github.com/preichenberger/go-gdax
```
***!!! As of 0.5 this library uses strings and is not backwards compatible with previous versions, to install previous versions, please use a tool like [GoDep](https://github.com/golang/dep)***

## Documentation
For full details on functionality, see [GoDoc](http://godoc.org/github.com/preichenberger/go-gdax) documentation.

### Setup
How to create a client:

```go

import (
  "os"
  gdax "github.com/preichenberger/go-gdax"
)

secret := os.Getenv("COINBASE_SECRET")
key := os.Getenv("COINBASE_KEY")
passphrase := os.Getenv("COINBASE_PASSPHRASE")

// or unsafe hardcode way
secret = "exposedsecret"
key = "exposedkey"
passphrase = "exposedpassphrase"

client := gdax.NewClient(secret, key, passphrase)
```

### HTTP Settings
```go
import (
  "net/http"
  "time"
)

client.HttpClient = &http.Client {
  Timeout: 15 * time.Second,
}
```

### Decimals
To manage precision correctly, this library sends all price values as strings. It is recommended to use a decimal library
like Spring's [Decimal](https://github.com/shopspring/decimal) if you are doing any manipulation of prices.

Example:
```go
import (
  "github.com/shopspring/decimal"
)

book, err := gdax.getBook("BTC-USD", 1)
if err != nil {
    println(err.Error())  
}

lastPrice, err := decimal.NewFromString(book.Bids[0].Price)
if err != nil {
    println(err.Error())  
}

order := gdax.Order{
  Price: lastPrice.Add(decimal.NewFromFloat(1.00)).String(),
  Size: "2.00",
  Side: "buy",
  ProductId: "BTC-USD",
}

savedOrder, err := client.CreateOrder(&order)
if err != nil {
  println(err.Error())
}

println(savedOrder.Id)
```

### Retry
You can set a retry count which uses exponential backoff: (2^(retry_attempt) - 1) / 2 * 1000 * milliseconds
```
client.RetryCount = 3 # 500ms, 1500ms, 3500ms
```

### Cursor
This library uses a cursor pattern so you don't have to keep track of pagination.

```go
var orders []gdax.Order
cursor = client.ListOrders()

for cursor.HasMore {
  if err := cursor.NextPage(&orders); err != nil {
    println(err.Error())
    return
  }

  for _, o := range orders {
    println(o.Id)
  }
}

```

### Websockets
Listen for websocket messages

```go
  import(
    ws "github.com/gorilla/websocket"
  )

  var wsDialer ws.Dialer
  wsConn, _, err := wsDialer.Dial("wss://ws-feed.pro.coinbase.com", nil)
  if err != nil {
    println(err.Error())
  }

  subscribe := gdax.Message{
    Type:      "subscribe",
    Channels: []gdax.MessageChannel{
      gdax.MessageChannel{
        Name: "heartbeat",
        ProductIds: []string{
          "BTC-USD",
        },
      },
      gdax.MessageChannel{
        Name: "level2",
        ProductIds: []string{
          "BTC-USD",
        },
      },
    },
  }
  if err := wsConn.WriteJSON(subscribe); err != nil {
    println(err.Error())
  }

  for true {
    message := gdax.Message{}
    if err := wsConn.ReadJSON(&message); err != nil {
      println(err.Error())
      break
    }

    if message.Type == "match" {
      println("Got a match")
    }
  }

```

### Time
Results return coinbase time type which handles different types of time parsing that GDAX returns. This wraps the native go time type

```go
  import(
    "time"
    gdax "github.com/preichenberger/go-gdax"
  )

  coinbaseTime := gdax.Time{}
  println(time.Time(coinbaseTime).Day())
```

### Examples
This library supports all public and private endpoints

Get Accounts:
```go
  accounts, err := client.GetAccounts()
  if err != nil {
    println(err.Error())
  }

  for _, a := range accounts {
    println(a.Balance)
  }
```

List Account Ledger:
```go
  var ledgers []gdax.LedgerEntry

  accounts, err := client.GetAccounts()
  if err != nil {
    println(err.Error())
  }

  for _, a := range accounts {
    cursor := client.ListAccountLedger(a.Id)
    for cursor.HasMore {
      if err := cursor.NextPage(&ledgers); err != nil {
        println(err.Error())
      }

      for _, e := range ledgers {
        println(e.Amount)
      }
  }
```

Create an Order:
```go
  order := gdax.Order{
    Price: "1.00",
    Size: "1.00",
    Side: "buy",
    ProductId: "BTC-USD",
  }

  savedOrder, err := client.CreateOrder(&order)
  if err != nil {
    println(err.Error())
  }

  println(savedOrder.Id)
```

Transfer funds:
```go
  transfer := gdax.Transfer {
    Type: "deposit",
    Amount: "1.00",
  }

  savedTransfer, err := client.CreateTransfer(&transfer)
  if err != nil {
    println(err.Error())
  }
```

Get Trade history:
```go
  var trades []gdax.Trade
  cursor := client.ListTrades("BTC-USD")

  for cursor.HasMore {
    if err := cursor.NextPage(&trades); err != nil {
      for _, t := range trades {
        println(trade.CoinbaseId)
      }
    }
  }
```

### Testing
To test with Coinbase's public sandbox set the following environment variables:
  - TEST_COINBASE_SECRET
  - TEST_COINBASE_KEY
  - TEST_COINBASE_PASSPHRASE

Then run `go test`
```sh
TEST_COINBASE_SECRET=secret TEST_COINBASE_KEY=key TEST_COINBASE_PASSPHRASE=passphrase go test
```
