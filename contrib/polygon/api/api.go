package api

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"gopkg.in/matryer/try.v1"

	"github.com/alpacahq/marketstore/v4/utils/log"
)

const (
	aggURL         = "%v/v2/aggs/ticker/%v/range/%v/%v/%v/%v"
	tradesURL      = "%v/v2/ticks/stocks/trades/%v/%v"
	quotesURL      = "%v/v1/historic/quotes/%v/%v"
	tickersURL     = "%v/v2/reference/tickers"
	retryCount     = 10
	jsonDumpFormat = "20060102"
	aggFileName    = "bars_%s_%s_%s_%d%s_%d.json.gz"
	tradeFileName  = "trades_%s_%s_%d_%d.json.gz"
	quoteFileName  = "quotes_%s_%s_%d_%d.json.gz"
)

var (
	baseURL      = "https://api.polygon.io"
	servers      = "wss://socket.polygon.io"
	apiKey       string
	completeDate = "2006-01-02"
	CacheDir     = ""
	FromCache    = false
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

func SetBaseURL(bURL string) {
	baseURL = bURL
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

func ListTickersPerPage(client *http.Client, page int) ([]Ticker, error) {
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

	body, err := download(client, u.String())
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(body, &resp)
	if err != nil {
		return nil, err
	}

	for i := range resp.Tickers {
		if includeExchange(resp.Tickers[i].PrimaryExch) {
			tickers = append(tickers, resp.Tickers[i])
		}
	}

	return tickers, nil
}

// GetHistoricAggregates requests polygon's REST API for aggregates
// for the provided resolution based on the provided parameters.
func GetHistoricAggregates(
	client *http.Client,
	ticker,
	timespan string,
	multiplier int,
	from, to time.Time,
	limit *int,
	unadjusted bool,
) (*HistoricAggregates, error) {
	// FIXME: This function does not handle pagination

	u, err := url.Parse(fmt.Sprintf(aggURL, baseURL, ticker, multiplier, timespan,
		from.Format(completeDate),
		to.Format(completeDate)),
	)
	if err != nil {
		return nil, err
	}

	q := u.Query()
	q.Set("apiKey", apiKey)
	q.Set("unadjusted", strconv.FormatBool(unadjusted))

	limitN := 0
	if limit != nil {
		q.Set("limit", strconv.FormatInt(int64(*limit), 10))
		limitN = *limit
	}

	u.RawQuery = q.Encode()
	filename := fmt.Sprintf(aggFileName, ticker,
		from.Format(jsonDumpFormat),
		to.Format(jsonDumpFormat),
		multiplier, timespan, limitN,
	)
	var body []byte

	if FromCache {
		body, err = readFromCache(filename)
	}
	if !FromCache || err != nil {
		body, err = download(client, u.String())
		if err != nil {
			return nil, err
		}
		_ = jsonDump(body, filename)
	}

	agg := &HistoricAggregates{}
	err = json.Unmarshal(body, agg)
	if err != nil {
		return nil, err
	}

	return agg, nil
}

// GetHistoricTrades requests polygon's REST API for historic trades
// on the provided date .
func GetHistoricTrades(client *http.Client, symbol, date string, batchSize int,
) (totalTrades *HistoricTrades, err error) {
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
		filename := fmt.Sprintf(tradeFileName, symbol, date, offset, batchSize)
		var body []byte
		var err error

		if FromCache {
			body, err = readFromCache(filename)
		}
		if !FromCache || err != nil {
			body, err = download(client, u.String())
			if err != nil {
				return nil, err
			}
			err = jsonDump(body, filename)
			if err != nil {
				return nil, fmt.Errorf("jsonDump")
			}
		}

		trades := &HistoricTrades{}
		err = json.Unmarshal(body, trades)
		if err != nil {
			return nil, err
		}

		if totalTrades == nil {
			totalTrades = trades
		} else {
			totalTrades.Results = append(totalTrades.Results, trades.Results...)
		}

		if len(trades.Results) == batchSize {
			offset = trades.Results[len(trades.Results)-1].SIPTimestamp
			if offset == 0 {
				return nil, fmt.Errorf("unable to paginate: Timestamp was empty for %v @ %v", symbol, date)
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
func GetHistoricQuotes(client *http.Client, symbol, date string, batchSize int,
) (totalQuotes *HistoricQuotes, err error) {
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
		filename := fmt.Sprintf(quoteFileName, symbol, date, offset, batchSize)
		var body []byte
		var err error

		if FromCache {
			body, err = readFromCache(filename)
		}
		if !FromCache || err != nil {
			body2, err2 := download(client, u.String())
			if err2 != nil {
				return nil, err2
			}
			_ = jsonDump(body2, filename)
		}

		err = json.Unmarshal(body, quotes)
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

func download(client *http.Client, endpointURL string) (body []byte, err error) {
	// It is required to retry both the download() and unmarshal() calls
	// as network errors (e.g. Unexpected EOF) can come also from unmarshal()
	err = try.Do(func(attempt int) (bool, error) {
		body, err = request(client, endpointURL)
		if err != nil && strings.Contains(err.Error(), "GOAWAY") {
			const sleepTime = 5 * time.Second
			// Polygon's way to tell that we are too fast
			log.Warn("parallel connection number may reach polygon limit, url: %s", endpointURL)
			time.Sleep(sleepTime)
		}
		return attempt < retryCount, err
	})
	return body, err
}

func request(client *http.Client, endpointURL string) ([]byte, error) {
	var resp *http.Response

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, endpointURL, http.NoBody)
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

	defer resp.Body.Close()

	var reader io.ReadCloser
	switch resp.Header.Get("Content-Encoding") {
	case "gzip":
		reader, err = gzip.NewReader(resp.Body)
		if err != nil {
			return nil, err
		}
		defer reader.Close()
	default:
		reader = resp.Body
	}

	body, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	return body, err
}

func jsonDump(body []byte, filename string) error {
	if CacheDir == "" {
		return nil
	}
	filename = filepath.Join(CacheDir, filename)
	f, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0o755)
	if err != nil {
		log.Error("[polygon] cannot create file: %s (%v)", filename, err)
		return err
	}
	defer f.Close()

	writer := gzip.NewWriter(f)
	defer writer.Close()

	_, err = writer.Write(body)
	if err != nil {
		return fmt.Errorf("failed to write gzip: %w", err)
	}
	log.Info("[polygon] saved: %s", filename)
	return nil
}

func readFromCache(filename string) (bytes []byte, err error) {
	if CacheDir == "" {
		return nil, nil
	}
	filename = filepath.Join(CacheDir, filename)
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	reader, err := gzip.NewReader(f)
	if err != nil {
		log.Warn("[polygon] cannot create Gzip Reader for %s (%v)", filename, err)
		return nil, err
	}
	defer reader.Close()

	bytes, err = io.ReadAll(reader)
	if err != nil {
		log.Warn("[polygon] failed to read file: %s (%v)", filename, err)
		return nil, err
	}

	log.Info("[polygon] cache loaded: %s", filename)
	return bytes, err
}
