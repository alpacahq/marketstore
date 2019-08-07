package main

import (
	"encoding/json"
	"fmt"
	"math"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"
    "strings"
    
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
	//Volume    []float64   `json:"volume"`
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
		//Volume: make([]float64, bars),
	}
}

func GetTiingoPrices(symbol string, from, to time.Time, period string, token string) (Quote, error) {

	resampleFreq := "1day"
	switch period {
	case "1Min":
		resampleFreq = "1min"
	case "3Min":
		resampleFreq = "3min"
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
	case "12H":
		resampleFreq = "12hour"
	case "1D":
		resampleFreq = "1day"
	}

	type priceData struct {
		Date           string  `json:"date"` // "2017-12-19T00:00:00Z"
		Ticker         string  `json:"ticker"`
		Open           float64 `json:"open"`
		Low            float64 `json:"low"`
		High           float64 `json:"high"`
		Close          float64 `json:"close"`
		//Volume         float64 `json:"volume"`
	}
    
	var iexData []priceData

	url := fmt.Sprintf(
		"https://api.tiingo.com/iex/%s/prices?startDate=%s&endDate=%s&resampleFreq=%s&afterHours=false&forceFill=true",
		symbol,
		url.QueryEscape(from.Format("2006-1-2")),
		url.QueryEscape(to.Format("2006-1-2")),
		resampleFreq)
        
	client := &http.Client{Timeout: ClientTimeout}
	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("Authorization", fmt.Sprintf("Token %s", token))
	resp, err := client.Do(req)

	if err != nil {
		log.Info("TiingoIEX symbol '%s' not found\n", symbol)
		return NewQuote("", 0), err
	}
	defer resp.Body.Close()

	contents, _ := ioutil.ReadAll(resp.Body)
	err = json.Unmarshal(contents, &iexData)
	if err != nil {
		log.Info("TiingoIEX symbol '%s' error: %v\n", symbol, err)
		return NewQuote("", 0), err
	}
    
	if len(iexData) < 1 {
		log.Info("TiingoIEX symbol '%s' No data returned %v", symbol)        
		return NewQuote("", 0), err
	}
    
	numrows := len(iexData)
	quote := NewQuote(symbol, numrows)

	for bar := 0; bar < numrows; bar++ {
        dt, _ := time.Parse(time.RFC3339, iexData[bar].Date)
        quote.Epoch[bar] = dt.Unix()
        quote.Open[bar] = iexData[bar].Open
        quote.High[bar] = iexData[bar].High
        quote.Low[bar] = iexData[bar].Low
        quote.Close[bar] = iexData[bar].Close
        //quote.Volume[bar] = float64(iexData[bar].Volume)
	}

	return quote, nil
}

// GetTiingoPricesFromSymbols - create a list of prices from symbols in string array
func GetTiingoPricesFromSymbols(symbols []string, from, to time.Time, period string, token string) (Quotes, error) {

	quotes := Quotes{}
	for _, symbol := range symbols {
		quote, err := GetTiingoPrices(symbol, from, to, period, token)
		if err == nil {
			quotes = append(quotes, quote)
		} else {
			log.Info("TiingoIEX error downloading " + symbol)
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

// TiingoIEXFetcher is the main worker for TiingoIEX
type TiingoIEXFetcher struct {
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
    
	return &TiingoIEXFetcher{
		config:         conf,
		symbols:        symbols,
        apiKey:         config.ApiKey,
		queryStart:     queryStart,
		baseTimeframe:  utils.NewTimeframe(timeframeStr),
	}, nil
}

// Run grabs data in intervals from starting time to ending time.
// If query_end is not set, it will run forever.
func (tiiex *TiingoIEXFetcher) Run() {
    
	realTime := false    
	timeStart := time.Time{}
	
    // Get last timestamp collected
	for _, symbol := range tiiex.symbols {
        tbk := io.NewTimeBucketKey(symbol + "/" + tiiex.baseTimeframe.String + "/OHLC")
        lastTimestamp := findLastTimestamp(tbk)
        log.Info("TiingoIEX: lastTimestamp for %s = %v", symbol, lastTimestamp)
        if timeStart.IsZero() || (!lastTimestamp.IsZero() && lastTimestamp.Before(timeStart)) {
            timeStart = lastTimestamp.UTC()
        }
	}
    
	// Set start time if not given.
	if !tiiex.queryStart.IsZero() {
		timeStart = tiiex.queryStart.UTC()
	} else {
		timeStart = time.Now().UTC().Add(-tiiex.baseTimeframe.Duration)
	}

	// For loop for collecting candlestick data forever
	var timeEnd time.Time
	var waitTill time.Time
	firstLoop := true
    
	for {
        
        if !firstLoop {
            if !realTime {
                // If next batch of backfill goes into the future, switch to realTime
                if timeEnd.Add(tiiex.baseTimeframe.Duration * 1440 * 30).After(time.Now().UTC()) {
                    realTime = true
                    timeStart = timeEnd
                    timeEnd = time.Now().UTC()
                // If still backfilling
                } else {
                    timeStart = timeEnd
                    timeEnd = timeEnd.Add(tiiex.baseTimeframe.Duration * 1440 * 30)
                }
            // if realTime
            } else {
                timeStart = timeEnd
                timeEnd = time.Now().UTC()
            }
        // firstLoop, we use this if we get timed out as well
        } else {
            firstLoop = false
            // Keep timeStart as original value
            timeEnd = timeStart.Add(tiiex.baseTimeframe.Duration * 1440 * 30)
        }
        
        year := timeEnd.Year()
        month := timeEnd.Month()
        day := timeEnd.Day()
        hour := timeEnd.Hour()
        minute := timeEnd.Minute()

        // To prevent gaps (ex: querying between 1:31 PM and 2:32 PM (hourly)would not be ideal)
        // But we still want to wait 1 candle afterwards (ex: 1:01 PM (hourly))
        // If it is like 1:59 PM, the first wait sleep time will be 1:59, but afterwards would be 1 hour.
        // Main goal is to ensure it runs every 1 <time duration> at :00
        if strings.HasSuffix(tiiex.baseTimeframe.String, "Min") {
            timeEnd = time.Date(year, month, day, hour, minute, 0, 0, time.UTC)
        } else if strings.HasSuffix(tiiex.baseTimeframe.String, "H") {
            timeEnd = time.Date(year, month, day, hour, 0, 0, 0, time.UTC)
        } else if strings.HasSuffix(tiiex.baseTimeframe.String, "D") {
            timeEnd = time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
        }
        
        quotes, _ := GetTiingoPricesFromSymbols(tiiex.symbols, timeStart, timeEnd, tiiex.baseTimeframe.String, tiiex.apiKey)
        
        for _, quote := range quotes {
            log.Info("TiingoIEX: Writing to %s/%s/OHLC from %v to %v", quote.Symbol, tiiex.baseTimeframe.String, timeStart, timeEnd)
            // write to csm
            cs := io.NewColumnSeries()
            cs.AddColumn("Epoch", quote.Epoch)
            cs.AddColumn("Open", quote.Open)
            cs.AddColumn("High", quote.High)
            cs.AddColumn("Low", quote.Low)
            cs.AddColumn("Close", quote.Close)
            //cs.AddColumn("Volume", quote.Volume)
            csm := io.NewColumnSeriesMap()
            tbk := io.NewTimeBucketKey(quote.Symbol + "/" + tiiex.baseTimeframe.String + "/OHLC")
            csm.AddColumnSeries(*tbk, cs)
            executor.WriteCSM(csm, false)
        }
        
		if realTime {
			// Sleep till next :00 time
            // This function ensures that we will always get full candles
			waitTill = time.Now().UTC().Add(tiiex.baseTimeframe.Duration)
            waitTill = time.Date(waitTill.Year(), waitTill.Month(), waitTill.Day(), waitTill.Hour(), waitTill.Minute(), 0, 0, time.UTC)
            log.Info("TiingoIEX: Next request at %v", waitTill)
			time.Sleep(waitTill.Sub(time.Now().UTC()))
		} else {
			time.Sleep(time.Second*60)
		}
	}
}

func main() {
}
