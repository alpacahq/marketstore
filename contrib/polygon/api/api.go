package api

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"strings"

	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/alpacahq/marketstore/v4/utils/log"
	"gopkg.in/matryer/try.v1"
)

const (
	aggURL     = "%v/v2/aggs/ticker/%v/range/%v/%v/%v/%v"
	tradesURL  = "%v/v2/ticks/stocks/trades/%v/%v"
	quotesURL  = "%v/v1/historic/quotes/%v/%v"
	tickersURL = "%v/v2/reference/tickers"
	retryCount = 10
)

var (
	baseURL      = "https://api.polygon.io"
	servers      = "wss://socket.polygon.io"
	apiKey       string
	NY, _        = time.LoadLocation("America/New_York")
	completeDate = "2006-01-02"
	client       *http.Client
)

func init() {
	client = &http.Client{
		Transport: &http.Transport{
			MaxIdleConnsPerHost: 100,
			MaxConnsPerHost:     100,
		},
		Timeout: 10 * time.Second,
	}
}

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
	Page    int      `json:"page"`
	PerPage int      `json:"perPage"`
	Count   int      `json:"count"`
	Status  string   `json:"status"`
	Tickers []Ticker `json:"tickers"`
}

type Ticker struct {
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

func ListTickers() ([]Ticker, error) {
	page := 0
	resp := make([]Ticker, 0)

	for {
		r, err := ListTickersPerPage(page)
		if err != nil {
			return nil, err
		}

		if len(r) == 0 {
			break
		}

		for _, ticker := range r {
			resp = append(resp, ticker)
		}

		page++
	}

	log.Info("[polygon] Returning %v symbols\n", len(resp))

	return resp, nil
}

func ListTickersPerPage(page int) ([]Ticker, error) {
	var resp ListTickersResponse
	tickers := make([]Ticker, 0)

	u, err := url.Parse(fmt.Sprintf(tickersURL, baseURL))
	if err != nil {
		return nil, err
	}

	q := u.Query()
	q.Set("apiKey", apiKey)
	q.Set("sort", "ticker")
	q.Set("perpage", "2000")
	q.Set("market", "stocks")
	q.Set("locale", "us")
	q.Set("active", "true")
	q.Set("page", strconv.FormatInt(int64(page), 10))
	u.RawQuery = q.Encode()

	err = downloadAndUnmarshal(u.String(), retryCount, &resp)
	if err != nil {
		return nil, err
	}

	for _, ticker := range resp.Tickers {
		if includeExchange(ticker.PrimaryExch) {
			tickers = append(tickers, ticker)
		}
	}

	return tickers, nil
}

// GetHistoricAggregates requests polygon's REST API for aggregates
// for the provided resolution based on the provided parameters.
func GetHistoricAggregates(
	ticker,
	timespan string,
	multiplier int,
	from, to time.Time,
	limit *int) (*HistoricAggregates, error) {
	// FIXME: This function does not handle pagination

	u, err := url.Parse(fmt.Sprintf(aggURL, baseURL, ticker, multiplier, timespan, from.Format(completeDate), to.Format(completeDate)))
	if err != nil {
		return nil, err
	}

	q := u.Query()
	q.Set("apiKey", apiKey)

	if limit != nil {
		q.Set("limit", strconv.FormatInt(int64(*limit), 10))
	}

	u.RawQuery = q.Encode()

	agg := &HistoricAggregates{}
	err = downloadAndUnmarshal(u.String(), retryCount, agg)
	if err != nil {
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

		trades := &HistoricTrades{}
		err := downloadAndUnmarshal(u.String(), retryCount, trades)
		if err != nil {
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

		err = downloadAndUnmarshal(u.String(), retryCount, quotes)
		if err != nil {
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

func downloadAndUnmarshal(url string, retryCount int, data interface{}) error {
	// It is required to retry both the download() and unmarshal() calls
	// as network errors (e.g. Unexpected EOF) can come also from unmarshal()
	err := try.Do(func(attempt int) (bool, error) {
		resp, err := download(url, retryCount)
		if err == nil {
			err = unmarshal(resp, data)
		}

		if err != nil && strings.Contains(err.Error(), "GOAWAY") {
			// Polygon's way to tell that we are too fast
			log.Warn("parallel connection number may reach polygon limit, url: %s", url)
			time.Sleep(5 * time.Second)
		}

		return attempt < retryCount, err
	})

	return err
}

func download(url string, retryCount int) (*http.Response, error) {
	var resp *http.Response

	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	// The returned JSON's size can be greatly reduced by enabling compression
	req.Header.Add("Accept-Encoding", "gzip")
	resp, err = client.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
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
