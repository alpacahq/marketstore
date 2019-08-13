package main

import (
	"encoding/json"
	"fmt"
	"math"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"
    //"strings"
    
	"github.com/alpacahq/marketstore/executor"
	"github.com/alpacahq/marketstore/planner"
	"github.com/alpacahq/marketstore/plugins/bgworker"
	"github.com/alpacahq/marketstore/utils"
	"github.com/alpacahq/marketstore/utils/io"
	"github.com/alpacahq/marketstore/utils/log"
    
)

// Quote - stucture for historical price data
type Quote struct {
	Symbol    string      `json:"symbol"`
	Precision int64       `json:"-"`
	Epoch     []int64     `json:"epoch"`
	Open      []float64   `json:"open"`
	High      []float64   `json:"high"`
	Low       []float64   `json:"low"`
	Close     []float64   `json:"close"`
	Volume    []float64   `json:"volume"`
}

// Quotes - an array of historical price data
type Quotes []Quote

// ClientTimeout - connect/read timeout for client requests
const ClientTimeout = 10 * time.Second

// NewQuote - new empty Quote struct
func NewQuote(symbol string, bars int) Quote {
	return Quote{
		Symbol: symbol,
		Epoch:  make([]int64,   bars),
		Open:   make([]float64, bars),
		High:   make([]float64, bars),
		Low:    make([]float64, bars),
		Close:  make([]float64, bars),
		Volume: make([]float64, bars),
	}
}

func GetTiingoPrices(symbol string, from, to time.Time, realTime bool, period string, token string) (Quote, error) {

	resampleFreq := "1hour"
	switch period {
	case "1Min":
		resampleFreq = "1min"
	case "5Min":
		resampleFreq = "5min"
	case "15Min":
		resampleFreq = "15min"
	case "30Min":
		resampleFreq = "30min"
	case "1H":
		resampleFreq = "1hour"
	case "2H":
		resampleFreq = "2hour"
	case "4H":
		resampleFreq = "4hour"
	case "6H":
		resampleFreq = "6hour"
	case "8H":
		resampleFreq = "8hour"
	}

	type priceData struct {
		TradesDone     float64 `json:"tradesDone"`
		Close          float64 `json:"close"`
		VolumeNotional float64 `json:"volumeNotional"`
		Low            float64 `json:"low"`
		Open           float64 `json:"open"`
		Date           string  `json:"date"` // "2017-12-19T00:00:00Z"
		High           float64 `json:"high"`
		Volume         float64 `json:"volume"`
	}

	type tiingoData struct {
		Ticker        string      `json:"ticker"`
		BaseCurrency  string      `json:"baseCurrency"`
		QuoteCurrency string      `json:"quoteCurrency"`
		PriceData     []priceData `json:"priceData"`
	}

	var cryptoData []tiingoData

    api_url := fmt.Sprintf(
                        "https://api.tiingo.com/tiingo/crypto/prices?tickers=%s&resampleFreq=%s&startDate=%s",
                        symbol,
                        resampleFreq,
                        url.QueryEscape(from.Format("2006-1-2")))
    
    if !realTime {
        api_url = api_url + "&endDate=" + url.QueryEscape(to.Format("2006-1-2"))
    }
    
	client := &http.Client{Timeout: ClientTimeout}
	req, _ := http.NewRequest("GET", api_url, nil)
	req.Header.Set("Authorization", fmt.Sprintf("Token %s", token))
	resp, err := client.Do(req)

	if err != nil {
		log.Info("Crypto: symbol '%s' not found\n", symbol)
		return NewQuote(symbol, 0), err
	}
	defer resp.Body.Close()

	contents, _ := ioutil.ReadAll(resp.Body)
	err = json.Unmarshal(contents, &cryptoData)
	if err != nil {
		log.Info("Crypto: symbol '%s' error: %v\n contents: %s", symbol, err, contents)
		return NewQuote(symbol, 0), err
	}
	if len(cryptoData) < 1 {
		log.Info("Crypto: symbol '%s' No data returned from %v-%v", symbol, from, to)
		return NewQuote(symbol, 0), err
	}
    
	numrows := len(cryptoData[0].PriceData)
	quote := NewQuote(symbol, numrows)

	for bar := 0; bar < numrows; bar++ {
        dt, _ := time.Parse(time.RFC3339, cryptoData[0].PriceData[bar].Date)
        // Only add data collected between from (timeStart) and to (timeEnd) range to prevent overwriting or confusion when aggregating data
        if dt.Unix() >= from.Unix() && dt.Unix() <= to.Unix() {
            log.Info("Added From: %v, Stamp: %v, To: %v", from, dt, to)
            quote.Epoch[bar] = dt.Unix()
            quote.Open[bar] = cryptoData[0].PriceData[bar].Open
            quote.High[bar] = cryptoData[0].PriceData[bar].High
            quote.Low[bar] = cryptoData[0].PriceData[bar].Low
            quote.Close[bar] = cryptoData[0].PriceData[bar].Close
            //quote.Volume[bar] = float64(cryptoData[0].PriceData[bar].Volume)
        } else {
            log.Info("Unadded From: %v, Stamp: %v, To: %v", from, dt, to)
        }
	}
    
    log.Info("1 len(Epochs) %v", len(quote.Epoch))

	return quote, nil
}

// GetTiingoPricesFromSymbols - create a list of prices from symbols in string array
func GetTiingoPricesFromSymbols(symbols []string, from, to time.Time, realTime bool, period string, token string) (Quotes, error) {

	quotes := Quotes{}
	for _, symbol := range symbols {
		quote, err := GetTiingoPrices(symbol, from, to, realTime, period, token)
		if err == nil {
			quotes = append(quotes, quote)
		} else {
			log.Info("Crypto: error downloading " + symbol)
		}
	}
	return quotes, nil
}

// getJSON via http request and decodes it using NewDecoder. Sets target interface to decoded json
func getJSON(url string, target interface{}) error {
	var myClient = &http.Client{Timeout: 10 * time.Second}
	r, err := myClient.Get(url)
	if err != nil {
		return err
	}
	defer r.Body.Close()

	return json.NewDecoder(r.Body).Decode(target)
}

// FetcherConfig is a structure of binancefeeder's parameters
type FetcherConfig struct {
	Symbols        []string `json:"symbols"`
    ApiKey         string   `json:"api_key"`
	QueryStart     string   `json:"query_start"`
	BaseTimeframe  string   `json:"base_timeframe"`
}

// TiingoCryptoFetcher is the main worker for TiingoCrypto
type TiingoCryptoFetcher struct {
	config         map[string]interface{}
	symbols        []string
    apiKey         string
	queryStart     time.Time
	baseTimeframe  *utils.Timeframe
}

// recast changes parsed JSON-encoded data represented as an interface to FetcherConfig structure
func recast(config map[string]interface{}) *FetcherConfig {
	data, _ := json.Marshal(config)
	ret := FetcherConfig{}
	json.Unmarshal(data, &ret)

	return &ret
}

//Checks time string and returns correct time format
func queryTime(query string) time.Time {
	trials := []string{
		"2006-01-02 03:04:05",
		"2006-01-02T03:04:05",
		"2006-01-02 03:04",
		"2006-01-02T03:04",
		"2006-01-02",
	}
	for _, layout := range trials {
		qs, err := time.Parse(layout, query)
		if err == nil {
			//Returns time in correct time.Time object once it matches correct time format
			return qs.In(utils.InstanceConfig.Timezone)
		}
	}
	//Return null if no time matches time format
	return time.Time{}
}

//Convert time from milliseconds to Unix
func convertMillToTime(originalTime int64) time.Time {
	i := time.Unix(0, originalTime*int64(time.Millisecond))
	return i
}

func findLastTimestamp(tbk *io.TimeBucketKey) time.Time {
	cDir := executor.ThisInstance.CatalogDir
	query := planner.NewQuery(cDir)
	query.AddTargetKey(tbk)
	start := time.Unix(0, 0).In(utils.InstanceConfig.Timezone)
	end := time.Unix(math.MaxInt64, 0).In(utils.InstanceConfig.Timezone)
	query.SetRange(start.Unix(), end.Unix())
	query.SetRowLimit(io.LAST, 1)
	parsed, err := query.Parse()
	if err != nil {
		return time.Time{}
	}
	reader, err := executor.NewReader(parsed)
	csm, err := reader.Read()
	cs := csm[*tbk]
	if cs == nil || cs.Len() == 0 {
		return time.Time{}
	}
	ts := cs.GetTime()
	return ts[0]
}

// NewBgWorker registers a new background worker
func NewBgWorker(conf map[string]interface{}) (bgworker.BgWorker, error) {
	config := recast(conf)
	var queryStart time.Time
	timeframeStr := "1Min"
	var symbols []string

	if config.BaseTimeframe != "" {
		timeframeStr = config.BaseTimeframe
	}

	if config.QueryStart != "" {
		queryStart = queryTime(config.QueryStart)
	}

	if len(config.Symbols) > 0 {
		symbols = config.Symbols
	}
    
	return &TiingoCryptoFetcher{
		config:         conf,
		symbols:        symbols,
        apiKey:         config.ApiKey,
		queryStart:     queryStart,
		baseTimeframe:  utils.NewTimeframe(timeframeStr),
	}, nil
}

// Run grabs data in intervals from starting time to ending time.
// If query_end is not set, it will run forever.
func (tiicc *TiingoCryptoFetcher) Run() {
    
	realTime := false    
	timeStart := time.Time{}
	
    // Get last timestamp collected
	for _, symbol := range tiicc.symbols {
        tbk := io.NewTimeBucketKey(symbol + "/" + tiicc.baseTimeframe.String + "/OHLC")
        lastTimestamp := findLastTimestamp(tbk)
        log.Info("Crypto: lastTimestamp for %s = %v", symbol, lastTimestamp)
        if timeStart.IsZero() || (!lastTimestamp.IsZero() && lastTimestamp.Before(timeStart)) {
            timeStart = lastTimestamp.UTC()
        }
	}
    
	// Set start time if not given.
	if !tiicc.queryStart.IsZero() {
		timeStart = tiicc.queryStart.UTC()
	} else {
		timeStart = time.Now().UTC().Add(-tiicc.baseTimeframe.Duration)
	}

	// For loop for collecting candlestick data forever
	var timeEnd time.Time
	var waitTill time.Time
	firstLoop := true
    
	for {
        
        if realTime {
            timeStart = timeEnd
            timeEnd = time.Now().UTC()
        } else {
            if firstLoop {
                firstLoop = false
            } else {
                timeStart = timeEnd
            }
            timeEnd = timeStart.Add(tiicc.baseTimeframe.Duration * 4900) // Under Tiingo's limit of 5000 records per request
            if timeEnd.After(time.Now().UTC()) {
                realTime = true
                timeEnd = time.Now().UTC()
            }
        }
        /*
        To prevent gaps (ex: querying between 1:31 PM and 2:32 PM (hourly)would not be ideal)
        But we still want to wait 1 candle afterwards (ex: 1:01 PM (hourly))
        If it is like 1:59 PM, the first wait sleep time will be 1:59, but afterwards would be 1 hour.
        Main goal is to ensure it runs every 1 <time duration> at :00
        Tiingo returns data by the day, regardless of granularity
        year := timeEnd.Year()
        month := timeEnd.Month()
        day := timeEnd.Day()
        hour := timeEnd.Hour()
        minute := timeEnd.Minute()
        if strings.HasSuffix(tiicc.baseTimeframe.String, "Min") {
            timeEnd = time.Date(year, month, day, hour, minute, 0, 0, time.UTC)
        } else if strings.HasSuffix(tiicc.baseTimeframe.String, "H") {
            timeEnd = time.Date(year, month, day, hour, 0, 0, 0, time.UTC)
        } else if strings.HasSuffix(tiicc.baseTimeframe.String, "D") {
            timeEnd = time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
        }
        */
        
        quotes, _ := GetTiingoPricesFromSymbols(tiicc.symbols, timeStart, timeEnd, realTime, tiicc.baseTimeframe.String, tiicc.apiKey)
        
        for _, quote := range quotes {
            // Check if there are entries to write
            if len(quote.Epoch) < 1 {
                continue
            }
            if realTime {
                // Check if realTime entry already exists or is still the latest to prevent overwriting and retriggering stream
                log.Info("Crypto: timeEnd %v First %v Last %v", timeEnd, time.Unix(quote.Epoch[0], 0).UTC(), time.Unix(quote.Epoch[len(quote.Epoch)-1], 0).UTC())
                log.Info("2 len(Epochs) %v", len(quote.Epoch))
                if timeEnd.Unix() > quote.Epoch[0] && timeEnd.Unix() > quote.Epoch[len(quote.Epoch)-1] {
                    // We assume that the head or tail of the slice is the earliest/latest entry received from data provider; and
                    // compare it against the timeEnd, which is the timestamp we want to write to the bucket; and
                    // if this is insufficient, we can always query the lastTimestamp from tbk
                    log.Info("Crypto: Row dated %v is still the latest in %s/%s/OHLC", time.Unix(quote.Epoch[0], 0).UTC(), quote.Symbol, tiicc.baseTimeframe.String)
                    continue
                } else {
                    // Write only the latest
                    rtQuote := NewQuote(quote.Symbol, 1)
                    rtQuote.Epoch[0] = quote.Epoch[len(quote.Epoch)-1]
                    rtQuote.Open[0] = quote.Open[len(quote.Open)-1]
                    rtQuote.High[0] = quote.High[len(quote.High)-1]
                    rtQuote.Low[0] = quote.Low[len(quote.Low)-1]
                    rtQuote.Close[0] = quote.Close[len(quote.Close)-1]
                    rtQuote.Volume[0] = quote.Volume[len(quote.Volume)-1]
                    quote = rtQuote
                    log.Info("Crypto: Writing row dated %v to %s/%s/OHLC", time.Unix(quote.Epoch[0], 0).UTC(), quote.Symbol, tiicc.baseTimeframe.String)
                }
            } else {
                log.Info("Crypto: Writing %v rows to %s/%s/OHLC from %v to %v", len(quote.Epoch), quote.Symbol, tiicc.baseTimeframe.String, timeStart, timeEnd)
            }
            
            //for _, epoch := range quote.Epoch {
            //    log.Info("Crypto: Adding %v", time.Unix(epoch, 0).UTC())
            //}
            
            // write to csm
            cs := io.NewColumnSeries()
            cs.AddColumn("Epoch", quote.Epoch)
            cs.AddColumn("Open", quote.Open)
            cs.AddColumn("High", quote.High)
            cs.AddColumn("Low", quote.Low)
            cs.AddColumn("Close", quote.Close)
            // cs.AddColumn("Volume", quote.Volume)
            csm := io.NewColumnSeriesMap()
            tbk := io.NewTimeBucketKey(quote.Symbol + "/" + tiicc.baseTimeframe.String + "/OHLC")
            csm.AddColumnSeries(*tbk, cs)
            executor.WriteCSM(csm, false)
        }
        
		if realTime {
			// Sleep till next :00 time
            // This function ensures that we will always get full candles
			waitTill = time.Now().UTC().Add(tiicc.baseTimeframe.Duration)
            waitTill = time.Date(waitTill.Year(), waitTill.Month(), waitTill.Day(), waitTill.Hour(), waitTill.Minute(), 0, 0, time.UTC)
            log.Info("Crypto: Next request at %v", waitTill)
			time.Sleep(waitTill.Sub(time.Now().UTC()))
		} else {
			time.Sleep(time.Second*99)
		}
	}
}

func main() {
}
