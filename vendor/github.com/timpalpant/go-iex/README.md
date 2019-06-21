# go-iex
A Go library for accessing the IEX Developer API.

[![GoDoc](https://godoc.org/github.com/timpalpant/go-iex?status.svg)](http://godoc.org/github.com/timpalpant/go-iex)
[![Build Status](https://travis-ci.org/timpalpant/go-iex.svg?branch=master)](https://travis-ci.org/timpalpant/go-iex)
[![Coverage Status](https://coveralls.io/repos/timpalpant/go-iex/badge.svg?branch=master&service=github)](https://coveralls.io/github/timpalpant/go-iex?branch=master)
[![Go Report Card](https://goreportcard.com/badge/github.com/timpalpant/go-iex)](https://goreportcard.com/badge/github.com/timpalpant/go-iex)

go-iex is a library to access the [IEX Developer API](https://www.iextrading.com/developer/docs/) from [Go](http://www.golang.org).
It provides a thin wrapper for working with the JSON REST endpoints and [IEXTP1 pcap data](https://www.iextrading.com/trading/market-data/#specifications).

[IEX](https://www.iextrading.com) is a fair, simple and transparent stock exchange dedicated to investor protection.
IEX provides realtime and historical market data for free through the IEX Developer API.
By using the IEX API, you agree to the [Terms of Use](https://www.iextrading.com/api-terms/). IEX is not affiliated
and does not endorse or recommend this library.

## Usage

### pcap2csv

You can use the included `pcap2csv` tool to create intraday minute bars from the pcap data files:

```
$ go install github.com/timpalpant/go-iex/pcap2csv
$ pcap2csv < input.pcap > output.csv
```

which produces a CSV with OHLC data:

```csv
symbol,time,open,high,low,close,volume
AAPL,2017-07-10T14:33:00Z,148.9100,149.0000,148.9100,148.9800,2527
AMZN,2017-07-10T14:33:00Z,364.8900,364.9600,364.8900,364.9600,1486
DISH,2017-07-10T14:33:00Z,49.6600,49.6600,49.6200,49.6200,1049
ELLO,2017-07-10T14:33:00Z,10.0300,10.1100,10.0300,10.1100,3523
FB,2017-07-10T14:33:00Z,46.4000,46.4000,46.3400,46.3400,1633
FICO,2017-07-10T14:33:00Z,57.4200,57.4200,57.3500,57.3800,1717
GOOD,2017-07-10T14:33:00Z,18.7700,18.7700,18.7300,18.7300,2459
```

### pcap2json

If you just need a tool to convert the provided pcap data files into JSON, you can use the included `pcap2json` tool:

```
$ go install github.com/timpalpant/go-iex/pcap2json
$ pcap2json < input.pcap > output.json
```

### Fetch real-time top-of-book quotes

```Go
package main

import (
  "fmt"
  "net/http"

  "github.com/timpalpant/go-iex"
)

func main() {
  client := iex.NewClient(&http.Client{})

  quotes, err := client.GetTOPS([]string{"AAPL", "SPY"})
  if err != nil {
      panic(err)
  }

  for _, quote := range quotes {
      fmt.Printf("%v: bid $%.02f (%v shares), ask $%.02f (%v shares) [as of %v]\n",
          quote.Symbol, quote.BidPrice, quote.BidSize,
          quote.AskPrice, quote.AskSize, quote.LastUpdated)
  }
}
```

### Fetch historical top-of-book quote (L1 tick) data.

Historical tick data (TOPS and DEEP) can be parsed using the `PcapScanner`.

```Go
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/timpalpant/go-iex"
	"github.com/timpalpant/go-iex/iextp/tops"
)

func main() {
	client := iex.NewClient(&http.Client{})

	// Get historical data dumps available for 2016-12-12.
	histData, err := client.GetHIST(time.Date(2016, time.December, 12, 0, 0, 0, 0, time.UTC))
	if err != nil {
		panic(err)
	} else if len(histData) == 0 {
		panic(fmt.Errorf("Found %v available data feeds", len(histData)))
	}

	// Fetch the pcap dump for that date and iterate through its messages.
	resp, err := http.Get(histData[0].Link)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	packetDataSource, err := iex.NewPcapDataSource(resp.Body)
	if err != nil {
		panic(err)
	}
	pcapScanner := iex.NewPcapScanner(packetDataSource)

	// Write each quote update message to stdout, in JSON format.
	enc := json.NewEncoder(os.Stdout)

	for {
		msg, err := pcapScanner.NextMessage()
		if err != nil {
			if err == io.EOF {
				break
			}

			panic(err)
		}

		switch msg := msg.(type) {
		case *tops.QuoteUpdateMessage:
			enc.Encode(msg)
		default:
		}
	}
}
```

### Iterate over data from a live multicast UDP stream of the DEEP feed.

IEX's live multicast data can also be parsed using the `PcapScanner`.

```Go
package main

import (
	"encoding/json"
	"io"
	"net"
	"os"

	"github.com/timpalpant/go-iex"
	"github.com/timpalpant/go-iex/iextp/deep"
)

func main() {
        multicastAddress := "233.215.21.4:10378"
	addr, err := net.ResolveUDPAddr("udp", multicastAddress)
	if err != nil {
	        panic(err)
	}

	conn, err := net.ListenMulticastUDP("udp", nil, addr)
	if err != nil {
	        panic(err)
	}

	packetDataSource := iex.NewPacketConnDataSource(conn)
	pcapScanner := iex.NewPcapScanner(packetDataSource)

	// Write each quote update message to stdout, in JSON format.
	enc := json.NewEncoder(os.Stdout)

	for {
		msg, err := pcapScanner.NextMessage()
		if err != nil {
			if err == io.EOF {
				break
			}

			panic(err)
		}

		switch msg := msg.(type) {
		case *deep.PriceLevelUpdateMessage:
			enc.Encode(msg)
		default:
		}
	}
}
```
## Contributing

Pull requests and issues are welcomed!

## License

go-iex is released under the [GNU Lesser General Public License, Version 3.0](https://www.gnu.org/licenses/lgpl-3.0.en.html)
