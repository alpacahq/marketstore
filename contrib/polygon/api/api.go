package api

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"

	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/alpacahq/marketstore/utils/log"
	"github.com/valyala/fasthttp"
	"gopkg.in/matryer/try.v1"
)

const (
	aggURL     = "%v/v1/historic/agg/%v/%v"
	tradesURL  = "%v/v2/ticks/stocks/trades/%v/%v"
	quotesURL  = "%v/v1/historic/quotes/%v/%v"
	tickersURL = "%v/v2/reference/tickers"
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

type ListTickersResponse struct {
	Page    int    `json:"page"`
	PerPage int    `json:"perPage"`
	Count   int    `json:"count"`
	Status  string `json:"status"`
	Tickers []struct {
		Ticker      string `json:"ticker"`
		Name        string `json:"name"`
		Market      string `json:"market"`
		Locale      string `json:"locale"`
		Type        string `json:"type"`
		Currency    string `json:"currency"`
		Active      bool   `json:"active"`
		PrimaryExch string `json:"primaryExch"`
		Updated     string `json:"updated"`
		Codes       struct {
			Cik     string `json:"cik"`
			Figiuid string `json:"figiuid"`
			Scfigi  string `json:"scfigi"`
			Cfigi   string `json:"cfigi"`
			Figi    string `json:"figi"`
		} `json:"codes"`
		URL string `json:"url"`
	} `json:"tickers"`
}

func includeExchange(exchange string) bool {
	// Polygon returns all tickers on all exchanges, which yields over 34k symbols
	// If we leave out OTC markets it will still have over 11k symbols
	if exchange == "CVEM" || exchange == "GREY" || exchange == "OTO" ||
		exchange == "OTC" || exchange == "OTCQB" || exchange == "OTCQ" {
		return false
	}
	return true
}

func ListTickers() (*ListTickersResponse, error) {
	resp := ListTickersResponse{}
	page := 0

	for {
		u, err := url.Parse(fmt.Sprintf(tickersURL, baseURL))
		if err != nil {
			return nil, err
		}

		q := u.Query()
		q.Set("apiKey", apiKey)
		q.Set("sort", "ticker")
		q.Set("perpage", "50")
		q.Set("market", "stocks")
		q.Set("locale", "us")
		q.Set("active", "true")
		q.Set("page", strconv.FormatInt(int64(page), 10))

		u.RawQuery = q.Encode()

		code, body, err := fasthttp.Get(nil, u.String())
		if err != nil {
			return nil, err
		}

		if code >= fasthttp.StatusMultipleChoices {
			return nil, fmt.Errorf("status code %v", code)
		}

		r := &ListTickersResponse{}

		err = json.Unmarshal(body, r)

		if err != nil {
			return nil, err
		}

		if len(r.Tickers) == 0 {
			break
		}

		for _, ticker := range r.Tickers {
			if includeExchange(ticker.PrimaryExch) {
				resp.Tickers = append(resp.Tickers, ticker)
			}
		}

		page++
	}

	log.Info("[polygon] Returning %v symbols\n", len(resp.Tickers))

	return &resp, nil
}

// GetHistoricAggregates requests polygon's REST API for historic aggregates
// for the provided resolution based on the provided query parameters.
func GetHistoricAggregates(
	symbol,
	resolution string,
	from, to time.Time,
	limit *int) (*HistoricAggregates, error) {
	// FIXME: Move this to Polygon API v2

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
func GetHistoricTrades(symbol, date string, batchSize int) (totalTrades *HistoricTrades, err error) {
	var (
		offset = int64(0)
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
		q.Set("limit", strconv.Itoa(batchSize))

		if offset > 0 {
			q.Set("timestamp", strconv.FormatInt(offset, 10))
		}

		u.RawQuery = q.Encode()

		resp, err := download(u.String(), 5)
		if err != nil {
			return nil, err
		}

		trades := &HistoricTrades{}
		if err = unmarshal(resp, trades); err != nil {
			return nil, err
		}

		if totalTrades == nil {
			totalTrades = trades
		} else {
			totalTrades.Results = append(totalTrades.Results, trades.Results...)
		}

		if len(trades.Results) == batchSize {
			offset = trades.Results[len(trades.Results)-1].SipTimestamp
			if offset == 0 {
				log.Fatal("Unable to paginate: Timestamp was empty for %v @ %v", symbol, date)
			}
		} else {
			break
		}
	}

	totalTrades.Ticker = symbol
	totalTrades.Success = true
	totalTrades.ResultsCount = len(totalTrades.Results)

	return totalTrades, nil
}

// GetHistoricQuotes requests polygon's REST API for historic quotes
// on the provided date.
func GetHistoricQuotes(symbol, date string, batchSize int) (totalQuotes *HistoricQuotes, err error) {
	// FIXME: Move this to Polygon API v2
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
		q.Set("limit", strconv.Itoa(batchSize))

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

		if len(quotes.Ticks) == batchSize {
			offset = quotes.Ticks[len(quotes.Ticks)-1].Timestamp
		} else {
			break
		}
	}

	return totalQuotes, nil
}

func download(url string, retryCount int) (*http.Response, error) {
	var (
		client = &http.Client{}
		resp   *http.Response
	)

	if err := try.Do(func(attempt int) (bool, error) {
		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			return attempt < retryCount, err
		}
		req.Header.Add("Accept-encoding", "gzip")
		resp, err = client.Do(req)
		return attempt < retryCount, err
	}); err != nil {
		return nil, err
	}

	if resp.StatusCode >= http.StatusMultipleChoices {
		return nil, fmt.Errorf("status code %v", resp.StatusCode)
	}

	return resp, nil
}

func unmarshal(resp *http.Response, data interface{}) (err error) {
	defer resp.Body.Close()

	var reader io.ReadCloser
	switch resp.Header.Get("Content-Encoding") {
	case "gzip":
		reader, err = gzip.NewReader(resp.Body)
		if err != nil {
			return err
		}
		defer reader.Close()
	default:
		reader = resp.Body
	}

	body, err := ioutil.ReadAll(reader)
	if err != nil {
		return err
	}

	return json.Unmarshal(body, data)
}
