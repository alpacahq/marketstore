package main

import (
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/alpacahq/marketstore/cmd/plugins/datafeeds"
	"github.com/alpacahq/marketstore/utils/io"
)

var Datafeed datafeeds.DatafeedType

func init() {
	Datafeed.Init = Init
	Datafeed.Get = Get
	Datafeed.Poll = Poll
	Datafeed.Recv = Recv
}

func Init(baseURL string, destinations []*io.TimeBucketKey) (feedState interface{}, exampleData io.ColumnSeriesMap, err error) {
	exampleData, _ = Poll(nil, nil)
	return new(url.URL), exampleData, nil
}

func Get(feedState interface{}, Input interface{}) (quotes interface{}, err error) {
	return "OK", nil
}

func Poll(feedState interface{}, Input interface{}) (results io.ColumnSeriesMap, err error) {
	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", []int64{time.Now().UTC().Unix()})
	cs.AddColumn("Open", []float32{100.})
	cs.AddColumn("High", []float32{200.})
	cs.AddColumn("Low", []float32{300.})
	cs.AddColumn("Close", []float32{400.})
	cs.AddColumn("Volume", []int32{500})
	key := io.NewTimeBucketKey("TESTFEED/1Min/OHLCV", "Symbol/Timeframe/AttributeGroup")
	csm := io.NewColumnSeriesMap()
	csm.AddColumnSeries(*key, cs)
	return csm, nil
}

func Recv() <-chan interface{} {
	return make(chan interface{})
}

func main() {
	baseurl := "http://download.finance.yahoo.com/d/quotes.csv"
	syms := "AAPL GOOG MSFT TSLA"
	symbols := strings.Split(syms, " ")

	var destinations []*io.TimeBucketKey
	for _, sym := range symbols {
		destinations = append(destinations,
			io.NewTimeBucketKey(sym+"/1Min/BIDASKSHARES",
				"Symbol/Timeframe/AttributeGroup"))
	}
	feedState, exampleData, err := Datafeed.Init(baseurl, destinations)
	if err != nil {
		fmt.Printf("FeedState: %v: ExampleData: %v\nError: %v\n", feedState, exampleData, err)
		os.Exit(1)
	}
}
