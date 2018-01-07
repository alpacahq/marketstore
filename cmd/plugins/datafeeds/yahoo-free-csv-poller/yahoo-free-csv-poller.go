package main

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/alpacahq/marketstore/cmd/plugins/datafeeds"
	"github.com/alpacahq/marketstore/utils/io"
)

type feedStateType struct {
	BaseURL      string
	parsedURL    *url.URL
	destinations []*io.TimeBucketKey
}

var (
	Datafeed  datafeeds.DatafeedType
	emptyChan <-chan interface{}
)

func init() {
	Datafeed.Init = Init
	Datafeed.Get = Get
	Datafeed.Poll = Poll
	emptyChan = make(<-chan interface{})
	Datafeed.Recv = func() <-chan interface{} { return emptyChan }
}

func Init(baseURL string, destinations []*io.TimeBucketKey) (feedState interface{}, exampleData io.ColumnSeriesMap, err error) {
	fs := new(feedStateType)

	fs.BaseURL = "http://download.finance.yahoo.com/d/quotes.csv"
	if len(baseURL) == 0 {
		baseURL = fs.BaseURL
	} else {
		fs.BaseURL = baseURL
	}
	if len(destinations) == 0 {
		return nil, nil,
			fmt.Errorf("destinations must be a slice of TimeBucketKeys\n")
	}
	fs.destinations = destinations

	fs.parsedURL, err = url.Parse(baseURL)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to parse baseURL: %s", err.Error())
	}

	/*
		Setup the polling URL

		Available parameters from here:
		http://kx.cloudingenium.com/content-providers/how-to-obtain-stock-quotes-from-yahoo-finance-you-can-query-them-via-excel-too/
	*/
	format := "sbab6t1d1" //"f=" parameter: symbol, bid, ask, bid size, trade time, trade date
	params := &url.Values{}
	params.Add("f", format)

	var symbols []string
	for _, tbk := range destinations {
		symbols = append(symbols, tbk.GetItemInCategory("Symbol"))
	}
	addSymbolsToURL(symbols, params) // arguments is a single string with a list of symbols
	fs.parsedURL.RawQuery = params.Encode()

	/*
		Do a sample query to ensure the URL is working and return status
	*/
	_, err = Get(fs, nil)
	if err != nil {
		return nil, nil, err
	}
	exampleData, _ = SnapShotQuoteToCSM(nil, destinations)
	return fs, exampleData, nil
}

func Get(feedState interface{}, Input interface{}) (quotes interface{}, err error) {
	fs := feedState.(*feedStateType)

	res, err := http.Get(fs.parsedURL.String())
	if err != nil {
		fmt.Printf("Price fetch didn't work with URL: %s\n", fs.parsedURL.String())
		return nil, err
	}
	quotes, err = csv.NewReader(res.Body).ReadAll()
	if err != nil {
		fmt.Println("Unable to decode response")
	}
	return quotes, nil
}

func Poll(feedState interface{}, Input interface{}) (results io.ColumnSeriesMap, err error) {
	fs := feedState.(*feedStateType)

	i_quotes, err := Get(fs, nil)
	if err != nil {
		return nil, err
	}
	quotes := i_quotes.([][]string)
	if quotes == nil {
		return nil, fmt.Errorf("unexpected error in return from source")
	}
	return SnapShotQuoteToCSM(quotes, fs.destinations)
}

func SnapShotQuoteToCSM(quotes [][]string, destinations []*io.TimeBucketKey) (csm io.ColumnSeriesMap, err error) {
	csm = io.NewColumnSeriesMap()
	if quotes == nil {
		key := io.NewTimeBucketKey("TESTFEED/1Min/BIDASKSHARES", "Symbol/Timeframe/AttributeGroup")
		/*
			Return a valid result with zero items if we get an empty quotes
		*/
		cs := io.NewColumnSeries()
		cs.AddColumn("Epoch", []int64{})
		cs.AddColumn("Bid", []float32{})
		cs.AddColumn("Ask", []float32{})
		cs.AddColumn("Shares", []int32{})
		csm.AddColumnSeries(*key, cs)
	} else {
		var i int
		/*
			Quotes are one per symbol, returned in order of destinations
		*/
		for _, record := range quotes {
			sq := NewSnapshotQuote(record)
			cs := io.NewColumnSeries()
			cs.AddColumn("Epoch", []int64{sq.Timestamp.UTC().Unix()})
			cs.AddColumn("Bid", []float32{sq.Bid})
			cs.AddColumn("Ask", []float32{sq.Ask})
			cs.AddColumn("Shares", []int32{sq.Shares})
			csm.AddColumnSeries(*destinations[i], cs)
			i++
		}
	}
	return csm, nil
}

type SnapshotQuote struct {
	Symbol    string
	Timestamp time.Time
	Bid, Ask  float32
	Shares    int32
}

func NewSnapshotQuote(record []string) *SnapshotQuote {
	loc, _ := time.LoadLocation("US/Eastern")
	if len(record) != 6 {
		fmt.Printf("Not enough elements in CSV string for quote, have %s need 6 elements\n",
			record)
	}
	sq := new(SnapshotQuote)

	var err error

	sq.Symbol = record[0]
	floatnum, _ := strconv.ParseFloat(record[1], 32)
	sq.Bid = float32(floatnum)
	floatnum, _ = strconv.ParseFloat(record[2], 32)
	sq.Ask = float32(floatnum)
	shares, _ := strconv.Atoi(record[3])
	sq.Shares = int32(shares)
	var buffer bytes.Buffer
	buffer.WriteString(record[5])
	buffer.WriteString(" ")
	buffer.WriteString(record[4])
	ts := buffer.String()
	sq.Timestamp, err = time.ParseInLocation("1/2/2006 3:04pm", ts, loc)
	if err != nil {
		fmt.Println("Error: ", err)
	}
	return sq
}

func (sq *SnapshotQuote) String() string {
	return fmt.Sprintf("Symbol: %s Bid: %6.2f Ask: %6.2f Shares: %d Time: %v",
		sq.Symbol, sq.Bid, sq.Ask, sq.Shares, sq.Timestamp)
}

func addSymbolsToURL(symbols []string, params *url.Values) {
	var buffer bytes.Buffer
	for i, symbol := range symbols {
		buffer.WriteString(symbol)
		if i < len(symbols)-1 {
			buffer.WriteString("+")
		}
	}
	allSymbols := buffer.String()
	params.Add("s", allSymbols)
}

func main() {
	syms := "AAPL GOOG MSFT TSLA SEE RDS-B VGK MAT CNC STZ HDV APA BAC JPM UBS MACMX QSPIX CG TGT"
	symbols := strings.Split(syms, " ")

	var destinations []*io.TimeBucketKey
	for _, sym := range symbols {
		destinations = append(destinations,
			io.NewTimeBucketKey(sym+"/1Min/BIDASKSHARES",
				"Symbol/Timeframe/AttributeGroup"))
	}
	feedState, _, err := Datafeed.Init("", destinations)
	if err != nil {
		fmt.Printf("init error:%s\n", err)
		os.Exit(1)
	}
	csm, err := Datafeed.Poll(feedState, nil)
	for key, cs := range csm {
		fmt.Println(key.GetItemInCategory("Symbol"))
		for _, name := range cs.GetColumnNames() {
			col := cs.GetColumn(name)
			fmt.Printf("%s: %v ", name, col)
		}
		fmt.Printf("\n")
	}
}
