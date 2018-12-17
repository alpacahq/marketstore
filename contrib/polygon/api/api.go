package api

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"runtime"
	"strconv"
	"time"

	"github.com/alpacahq/marketstore/utils/log"
	"github.com/eapache/channels"
	nats "github.com/nats-io/go-nats"
	"github.com/valyala/fasthttp"
	try "gopkg.in/matryer/try.v1"
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

func SetAPIKey(key string) {
	apiKey = key
}

func SetBaseURL(url string) {
	baseURL = url
}

func SetNatsServers(serverList string) {
	servers = serverList
}

// func GetAggregates(symbol string, from, to time.Time) (*GetAggregatesResponse, error) {
// 	resp := GetAggregatesResponse{}

// 	from = from.In(NY)
// 	if to.IsZero() {
// 		to = from.Add(7 * 24 * time.Hour)
// 	}

// 	retry := 0

// 	for {
// 		url := fmt.Sprintf("%s/v1/historic/agg/%s/%s?apiKey=%s&from=%s&to=%s",
// 			baseURL, "minute", symbol,
// 			apiKey,
// 			from.Format("2006-01-02"),
// 			to.Format("2006-01-02"))

// 		res, err := http.Get(url)

// 		if err != nil {
// 			return nil, err
// 		}

// 		if res.StatusCode >= http.StatusMultipleChoices {
// 			return nil, fmt.Errorf("status code %v", res.StatusCode)
// 		}

// 		r := &GetAggregatesResponse{}

// 		body, err := ioutil.ReadAll(res.Body)

// 		if err != nil {
// 			return nil, err
// 		}

// 		err = json.Unmarshal(body, r)

// 		if err != nil {
// 			return nil, err
// 		}

// 		// Sometimes polygon returns empty data set even though the data
// 		// is there. Here we retry up to 5 times to ensure the data
// 		// is really empty. This does add overhead, but since it is only
// 		// called for the beginning backfill, it is worth it to not miss
// 		// any data. Usually the data is returned within 3 retries.
// 		if len(r.Ticks) == 0 {
// 			if retry <= 5 && from.Before(time.Now()) {
// 				retry++
// 				continue
// 			} else {
// 				retry = 0
// 				break
// 			}
// 		}

// 		resp.Ticks = append(resp.Ticks, r.Ticks...)

// 		from = to.Add(24 * time.Hour)
// 		to = from.Add(24 * 7 * time.Hour)
// 	}

// 	return &resp, nil
// }

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

const (
	AggPrefix   = "AM."
	QuotePrefix = "Q."
	TradePrefix = "T."
)

// Stream from the polygon nats server
func Stream(handler func(m *nats.Msg), prefix string, symbols []string) (err error) {
	nc, _ := nats.Connect(
		servers,
		nats.Token(apiKey))

	sem := make(chan struct{}, runtime.NumCPU())
	c := channels.NewInfiniteChannel()

	go func() {
		for msg := range c.Out() {
			sem <- struct{}{}
			go func(m interface{}) {
				defer func() { <-sem }()
				handler(m.(*nats.Msg))
			}(msg)
		}
	}()

	go func() {
		for {
			<-time.After(10 * time.Second)
			if c.Len() > 0 {
				switch prefix {
				case AggPrefix:
					log.Info("[polygon] aggregate stream channel depth: %v", c.Len())
				case QuotePrefix:
					log.Info("[polygon] quote stream channel depth: %v", c.Len())
				case TradePrefix:
					log.Info("[polygon] trade stream channel depth: %v", c.Len())
				}
			}
		}
	}()

	if symbols != nil && len(symbols) > 0 {
		for _, symbol := range symbols {
			if _, err = nc.Subscribe(
				prefix+symbol,
				handler); err != nil {
				return
			}
		}
	} else {
		_, err = nc.Subscribe(prefix+"*", func(m *nats.Msg) {
			c.In() <- m
		})
	}

	return
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
	offset := int64(0)
	for {
		u, err := url.Parse(fmt.Sprintf(tradesURL, baseURL, symbol, date))
		if err != nil {
			return nil, err
		}

		q := u.Query()
		q.Set("apiKey", apiKey)
		q.Set("limit", strconv.FormatInt(10000, 10))

		if offset > 0 {
			q.Set("offset", strconv.FormatInt(offset, 10))
		}

		u.RawQuery = q.Encode()

		var resp *http.Response

		if err = try.Do(func(attempt int) (bool, error) {
			resp, err = http.Get(u.String())
			return (attempt < 5), err
		}); err != nil {
			return nil, err
		}

		if resp.StatusCode >= http.StatusMultipleChoices {
			return nil, fmt.Errorf("status code %v", resp.StatusCode)
		}

		trades := &HistoricTrades{}

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
	offset := int64(0)
	for {
		u, err := url.Parse(fmt.Sprintf(quotesURL, baseURL, symbol, date))
		if err != nil {
			return nil, err
		}

		q := u.Query()
		q.Set("apiKey", apiKey)
		q.Set("limit", strconv.FormatInt(10000, 10))

		if offset > 0 {
			q.Set("offset", strconv.FormatInt(offset, 10))
		}

		u.RawQuery = q.Encode()

		var resp *http.Response

		if err = try.Do(func(attempt int) (bool, error) {
			resp, err = http.Get(u.String())
			return (attempt < 5), err
		}); err != nil {
			return nil, err
		}

		if resp.StatusCode >= http.StatusMultipleChoices {
			return nil, fmt.Errorf("status code %v", resp.StatusCode)
		}

		quotes := &HistoricQuotes{}

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

	// ts := time.Unix(0, totalQuotes.Ticks[0].Timestamp*1000*100)
	// for _, tick := range totalQuotes.Ticks[1:] {
	// 	newTS := time.Unix(0, tick.Timestamp*1000*100)
	// 	if newTS.Before(ts) {
	// 		log.Fatal("NOT STRICTLY INCREASING [%v < %v]", newTS.UnixNano(), ts.UnixNano())
	// 	} else {
	// 		ts = newTS
	// 	}
	// }

	// log.Fatal("STRICTLY INCREASING")

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
