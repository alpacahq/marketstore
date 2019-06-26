package api

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/valyala/fasthttp"
	"gopkg.in/matryer/try.v1"
)

const (
	aggURL     = "%v/v1/historic/agg/%v/%v"
	tradesURL  = "%v/v1/historic/trades/%v/%v"
	quotesURL  = "%v/v1/historic/quotes/%v/%v"
	symbolsURL = "%v/v1/meta/symbols"
)

var (
	baseURL = "https://api.polygon.io"
	servers = "nats://nats1.polygon.io:30401, nats://nats2.polygon.io:30402, nats://nats3.polygon.io:30403"
	apiKey  string
	NY, _   = time.LoadLocation("America/New_York")
)

type GetAggregatesResponse struct {
	Symbol  string `json:"symbol"`
	AggType string `json:"aggType"`
	Map     struct {
		O string `json:"o"`
		C string `json:"c"`
		H string `json:"h"`
		L string `json:"l"`
		V string `json:"v"`
		D string `json:"d"`
	} `json:"map"`
	Ticks []struct {
		Open        float64 `json:"o"`
		Close       float64 `json:"c"`
		High        float64 `json:"h"`
		Low         float64 `json:"l"`
		Volume      int     `json:"v"`
		EpochMillis int64   `json:"d"`
	} `json:"ticks"`
}

func SetAPIKey(key string) {
	apiKey = key
}

func SetBaseURL(url string) {
	baseURL = url
}

func SetNatsServers(serverList string) {
	servers = serverList
}


type ListSymbolsResponse struct {
	Symbols []struct {
		Symbol          string `json:"symbol"`
		Name            string `json:"name"`
		Type            string `json:"type"`
		Updated         string `json:"updated"`
		IsOTC           bool   `json:"isOTC"`
		PrimaryExchange int    `json:"primaryExchange"`
		ExchSym         string `json:"exchSym"`
		URL             string `json:"url"`
	} `json:"symbols"`
}

func ListSymbols() (*ListSymbolsResponse, error) {
	resp := ListSymbolsResponse{}
	page := 0

	for {
		u, err := url.Parse(fmt.Sprintf(symbolsURL, baseURL))
		if err != nil {
			return nil, err
		}

		q := u.Query()
		q.Set("apiKey", apiKey)
		q.Set("sort", "symbol")
		q.Set("perpage", "200")
		q.Set("page", strconv.FormatInt(int64(page), 10))

		u.RawQuery = q.Encode()

		code, body, err := fasthttp.Get(nil, u.String())
		if err != nil {
			return nil, err
		}

		if code >= fasthttp.StatusMultipleChoices {
			return nil, fmt.Errorf("status code %v", code)
		}

		r := &ListSymbolsResponse{}

		err = json.Unmarshal(body, r)

		if err != nil {
			return nil, err
		}

		if len(r.Symbols) == 0 {
			break
		}

		resp.Symbols = append(resp.Symbols, r.Symbols...)

		page++
	}

	return &resp, nil
}

type StreamAggregate struct {
	Symbol      string  `json:"sym"`
	Open        float64 `json:"o"`
	High        float64 `json:"h"`
	Low         float64 `json:"l"`
	Close       float64 `json:"c"`
	Volume      int     `json:"v"`
	EpochMillis int64   `json:"s"`

	// unneeded
	X int     `json:"-"`
	A float64 `json:"-"`
	T float64 `json:"-"`
	E int64   `json:"-"`
}


// PolyTrade is the reference structure sent
// by polygon for quote data
type PolyTrade struct {
	Symbol     string  `json:"sym"`
	Exchange   int     `json:"-"`
	Price      float64 `json:"p"`
	Size       int64   `json:"s"`
	Timestamp  int64   `json:"t"`
	Conditions []int   `json:"c"`
}

// PolyQuote is the reference structure sent
// by polygon for quote data
type PolyQuote struct {
	Symbol      string  `json:"sym"`
	Condition   int     `json:"-"`
	BidExchange int     `json:"-"`
	AskExchange int     `json:"-"`
	BidPrice    float64 `json:"bp"`
	AskPrice    float64 `json:"ap"`
	BidSize     int64   `json:"bs"`
	AskSize     int64   `json:"as"`
	Timestamp   int64   `json:"t"`
}

// HistoricAggregates is the structure that defines
// aggregate data served through polygon's REST API.
type HistoricAggregates struct {
	Symbol        string `json:"symbol"`
	AggregateType string `json:"aggType"`
	Map           struct {
		O string `json:"o"`
		C string `json:"c"`
		H string `json:"h"`
		L string `json:"l"`
		V string `json:"v"`
		D string `json:"d"`
	} `json:"map"`
	Ticks []AggTick `json:"ticks"`
}

// AggTick is the structure that contains the actual
// tick data included in a HistoricAggregates response
type AggTick struct {
	EpochMilliseconds int64   `json:"d"`
	Open              float64 `json:"o"`
	High              float64 `json:"h"`
	Low               float64 `json:"l"`
	Close             float64 `json:"c"`
	Volume            int     `json:"v"`
}

// HistoricTrades is the structure that defines trade
// data served through polygon's REST API.
type HistoricTrades struct {
	Day string `json:"day"`
	Map struct {
		C1 string `json:"c1"`
		C2 string `json:"c2"`
		C3 string `json:"c3"`
		C4 string `json:"c4"`
		E  string `json:"e"`
		P  string `json:"p"`
		S  string `json:"s"`
		T  string `json:"t"`
	} `json:"map"`
	MsLatency int         `json:"msLatency"`
	Status    string      `json:"status"`
	Symbol    string      `json:"symbol"`
	Ticks     []TradeTick `json:"ticks"`
	Type      string      `json:"type"`
}

// TradeTick is the structure that contains the actual
// tick data included in a HistoricTrades response
type TradeTick struct {
	Timestamp  int64   `json:"t"`
	Price      float64 `json:"p"`
	Size       int     `json:"s"`
	Exchange   string  `json:"e"`
	Condition1 int     `json:"c1"`
	Condition2 int     `json:"c2"`
	Condition3 int     `json:"c3"`
	Condition4 int     `json:"c4"`
}

// HistoricQuotes is the structure that defines quote
// data served through polygon's REST API.
type HistoricQuotes struct {
	Day string `json:"day"`
	Map struct {
		AE string `json:"aE"`
		AP string `json:"aP"`
		AS string `json:"aS"`
		BE string `json:"bE"`
		BP string `json:"bP"`
		BS string `json:"bS"`
		C  string `json:"c"`
		T  string `json:"t"`
	} `json:"map"`
	MsLatency int         `json:"msLatency"`
	Status    string      `json:"status"`
	Symbol    string      `json:"symbol"`
	Ticks     []QuoteTick `json:"ticks"`
	Type      string      `json:"type"`
}

// QuoteTick is the structure that contains the actual
// tick data included in a HistoricQuotes response
type QuoteTick struct {
	Timestamp   int64   `json:"t"`
	BidExchange string  `json:"bE"`
	AskExchange string  `json:"aE"`
	BidPrice    float64 `json:"bP"`
	AskPrice    float64 `json:"aP"`
	BidSize     int     `json:"bS"`
	AskSize     int     `json:"aS"`
	Condition   int     `json:"c"`
}

// GetHistoricAggregates requests polygon's REST API for historic aggregates
// for the provided resolution based on the provided query parameters.
func GetHistoricAggregates(
	symbol,
	resolution string,
	from, to time.Time,
	limit *int) (*HistoricAggregates, error) {

	u, err := url.Parse(fmt.Sprintf(aggURL, baseURL, resolution, symbol))
	if err != nil {
		return nil, err
	}

	q := u.Query()
	q.Set("apiKey", apiKey)

	if !from.IsZero() {
		q.Set("from", from.Format(time.RFC3339))
	}

	if !to.IsZero() {
		q.Set("to", to.Format(time.RFC3339))
	}

	if limit != nil {
		q.Set("limit", strconv.FormatInt(int64(*limit), 10))
	}

	u.RawQuery = q.Encode()

	resp, err := http.Get(u.String())
	if err != nil {
		return nil, err
	}

	if resp.StatusCode >= http.StatusMultipleChoices {
		return nil, fmt.Errorf("status code %v", resp.StatusCode)
	}

	agg := &HistoricAggregates{}

	if err = unmarshal(resp, agg); err != nil {
		return nil, err
	}

	return agg, nil
}

// GetHistoricTrades requests polygon's REST API for historic trades
// on the provided date .
func GetHistoricTrades(symbol, date string) (totalTrades *HistoricTrades, err error) {
	var (
		offset = int64(0)
		resp   *http.Response
		u      *url.URL
		q      url.Values
		trades = &HistoricTrades{}
	)

	for {
		u, err = url.Parse(fmt.Sprintf(tradesURL, baseURL, symbol, date))
		if err != nil {
			return nil, err
		}

		q = u.Query()
		q.Set("apiKey", apiKey)
		q.Set("limit", strconv.FormatInt(10000, 10))

		if offset > 0 {
			q.Set("offset", strconv.FormatInt(offset, 10))
		}

		u.RawQuery = q.Encode()

		if err = try.Do(func(attempt int) (bool, error) {
			resp, err = http.Get(u.String())
			return (attempt < 5), err
		}); err != nil {
			return nil, err
		}

		if resp.StatusCode >= http.StatusMultipleChoices {
			return nil, fmt.Errorf("status code %v", resp.StatusCode)
		}

		if err = unmarshal(resp, trades); err != nil {
			return nil, err
		}

		if totalTrades == nil {
			totalTrades = trades
		} else {
			totalTrades.Ticks = append(totalTrades.Ticks, trades.Ticks...)
		}

		if len(trades.Ticks) == 10000 {
			offset = trades.Ticks[len(trades.Ticks)-1].Timestamp
		} else {
			break
		}
	}

	return totalTrades, nil
}

// GetHistoricQuotes requests polygon's REST API for historic quotes
// on the provided date.
func GetHistoricQuotes(symbol, date string) (totalQuotes *HistoricQuotes, err error) {
	var (
		offset = int64(0)
		resp   *http.Response
		u      *url.URL
		q      url.Values
		quotes = &HistoricQuotes{}
	)

	for {
		u, err = url.Parse(fmt.Sprintf(quotesURL, baseURL, symbol, date))
		if err != nil {
			return nil, err
		}

		q = u.Query()
		q.Set("apiKey", apiKey)
		q.Set("limit", strconv.FormatInt(10000, 10))

		if offset > 0 {
			q.Set("offset", strconv.FormatInt(offset, 10))
		}

		u.RawQuery = q.Encode()

		if err = try.Do(func(attempt int) (bool, error) {
			resp, err = http.Get(u.String())
			return (attempt < 5), err
		}); err != nil {
			return nil, err
		}

		if resp.StatusCode >= http.StatusMultipleChoices {
			return nil, fmt.Errorf("status code %v", resp.StatusCode)
		}

		if err = unmarshal(resp, quotes); err != nil {
			return nil, err
		}

		if totalQuotes == nil {
			totalQuotes = quotes
		} else {
			totalQuotes.Ticks = append(totalQuotes.Ticks, quotes.Ticks...)
		}

		if len(quotes.Ticks) == 10000 {
			offset = quotes.Ticks[len(quotes.Ticks)-1].Timestamp
		} else {
			break
		}
	}

	return totalQuotes, nil
}

func unmarshal(resp *http.Response, data interface{}) error {
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	return json.Unmarshal(body, data)
}
