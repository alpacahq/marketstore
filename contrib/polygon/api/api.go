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
	servers = "ws://socket.polygon.io:30328" // default
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

func SetWSServers(serverList string) {
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
