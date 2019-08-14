package main

import (
	"encoding/json"
	"fmt"
	"math"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"
    "strconv"
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
	}
}

func GetIntrinioPrices(symbol string, from, to time.Time, realTime bool, period string, token string) (Quote, error) {
    
	resampleFreq := "H1"
	switch period {
	case "1Min":
		resampleFreq = "m1"
	case "5Min":
		resampleFreq = "m5"
	case "15Min":
		resampleFreq = "m15"
	case "30Min":
		resampleFreq = "m30"
	case "1H":
		resampleFreq = "H1"
	case "2H":
		resampleFreq = "H2"
	case "4H":
		resampleFreq = "H4"
	case "6H":
		resampleFreq = "H6"
	case "8H":
		resampleFreq = "H8"
	}

	type pairData struct {
		Symbol         string  `json:"code"`
		BaseCurrency   string  `json:"base_currency"`
		QuoteCurrency  string  `json:"quote_currency"`
	}
    
	type priceData struct {
		Date             string `json:"occurred_at"` // "2017-12-19T00:00:00Z"
		OpenBid          string `json:"open_bid"`
		HighBid          string `json:"high_bid"`
		LowBid           string `json:"low_bid"`
		CloseBid         string `json:"close_bid"`
 		OpenAsk          string `json:"open_ask"`
		HighAsk          string `json:"high_ask"`
		LowAsk           string `json:"low_ask"`
		CloseAsk         string `json:"close_ask"`
		TotalTicks       int64   `json:"total_ticks"`
	}
    
	type intrinioData struct {
		PriceData     []priceData `json:"prices"`
		PairData      pairData    `json:"pair"`
		Page          string      `json:"next_page"`
	}
    
	var forexData intrinioData

    api_url := fmt.Sprintf(
                        "https://api-v2.intrinio.com/forex/prices/%s/%s?api_key=%s&start_date=%s&start_time=%s&end_date=%s&end_time=%s",
                        symbol,
                        resampleFreq,
                        token,
                        url.QueryEscape(from.Format("2006-01-02")),
                        url.QueryEscape(from.Format("15:04:05")),
                        url.QueryEscape(to.Format("2006-01-02")),
                        url.QueryEscape(to.Format("15:04:05")))
    
	client := &http.Client{Timeout: ClientTimeout}
	req, _ := http.NewRequest("GET", api_url, nil)
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
	resp, err := client.Do(req)

	if err != nil {
		log.Info("Forex: Intrinio symbol '%s' not found\n", symbol)
		return NewQuote(symbol, 0), err
	}
	defer resp.Body.Close()

	contents, _ := ioutil.ReadAll(resp.Body)
	err = json.Unmarshal(contents, &forexData)
	if err != nil {
		log.Info(": Intrinio symbol '%s' error: %v\n contents: %s", symbol, err, contents)
		return NewQuote(symbol, 0), err
	}
    
	if len(forexData.PriceData) < 1 {
		log.Info("Forex: Intrinio symbol '%s' No data returned from %v-%v", symbol, from, to)
        log.Info("%s, %s, %s", api_url, url.QueryEscape(from.Format("15:04:05")), url.QueryEscape(to.Format("15:04:05")))
        log.Info("%v", forexData)
		return NewQuote(symbol, 0), err
	}
    
	numrows := len(forexData.PriceData)
	quote := NewQuote(symbol, numrows)
    // Pointers to help slice into just the relevent datas
    startOfSlice := -1
    endOfSlice := -1
    
	for bar := 0; bar < numrows; bar++ {
        dt, _ := time.Parse(time.RFC3339, forexData.PriceData[bar].Date)
        log.Info("%v, %v, %v", from, dt, to)
        // Only add data collected between from (timeStart) and to (timeEnd) range to prevent overwriting or confusion when aggregating data
        if dt.UTC().Unix() >= from.UTC().Unix() && dt.UTC().Unix() <= to.UTC().Unix() {
            if startOfSlice == -1 {
                startOfSlice = bar
            }
            endOfSlice = bar
            quote.Epoch[bar] = dt.UTC().Unix()
            open_bid, _ := strconv.ParseFloat(forexData.PriceData[bar].OpenBid, 64) 
            open_ask, _ := strconv.ParseFloat(forexData.PriceData[bar].OpenAsk, 64)
            high_bid, _ := strconv.ParseFloat(forexData.PriceData[bar].HighBid, 64) 
            high_ask, _ := strconv.ParseFloat(forexData.PriceData[bar].HighAsk, 64)
            low_bid, _ := strconv.ParseFloat(forexData.PriceData[bar].LowBid, 64) 
            low_ask, _ := strconv.ParseFloat(forexData.PriceData[bar].LowAsk, 64)
            close_bid, _ := strconv.ParseFloat(forexData.PriceData[bar].CloseBid, 64) 
            close_ask, _ := strconv.ParseFloat(forexData.PriceData[bar].CloseAsk, 64)
            
            quote.Open[bar] = (open_bid + open_ask) / 2
            quote.High[bar] = (high_bid + high_ask) / 2
            quote.Low[bar] = (low_bid + low_ask) / 2
            quote.Close[bar] = (close_bid + close_ask) / 2
        }
	}
    
    quote.Epoch = quote.Epoch[startOfSlice:endOfSlice]
    quote.Open = quote.Open[startOfSlice:endOfSlice]
    quote.High = quote.High[startOfSlice:endOfSlice]
    quote.Low = quote.Low[startOfSlice:endOfSlice]
    quote.Close = quote.Close[startOfSlice:endOfSlice]
    
	return quote, nil
}

// GetIntrinioPricesFromSymbols - create a list of prices from symbols in string array
func GetIntrinioPricesFromSymbols(symbols []string, from, to time.Time, realTime bool, period string, token string) (Quotes, error) {
    
	quotes := Quotes{}
	for _, symbol := range symbols {
		quote, err := GetIntrinioPrices(symbol, from, to, realTime, period, token)
		if err == nil {
			quotes = append(quotes, quote)
		} else {
			log.Info("Forex: Intrinio error downloading " + symbol)
		}
	}
	return quotes, nil
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
		Date           string  `json:"date"` // "2017-12-19T00:00:00Z"
		Ticker         string  `json:"ticker"`
		Open           float64 `json:"open"`
		Low            float64 `json:"low"`
		High           float64 `json:"high"`
		Close          float64 `json:"close"`
	}
    
	var forexData []priceData
    
    api_url := fmt.Sprintf(
                        "https://api.tiingo.com/tiingo/fx/%s/prices?resampleFreq=%s&afterHours=false&forceFill=true&startDate=%s",
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
		log.Info("Forex: Tiingo symbol '%s' not found\n", symbol)
		return NewQuote(symbol, 0), err
	}
	defer resp.Body.Close()

	contents, _ := ioutil.ReadAll(resp.Body)
	err = json.Unmarshal(contents, &forexData)
	if err != nil {
		log.Info("Forex: Tiingo symbol '%s' error: %v\n contents: %s", symbol, err, contents)
		return NewQuote(symbol, 0), err
	}
    
	if len(forexData) < 1 {
		log.Info("Forex: Tiingo symbol '%s' No data returned from %v-%v", symbol, from, to)  
		return NewQuote(symbol, 0), err
	}
    
	numrows := len(forexData)
	quote := NewQuote(symbol, numrows)
    // Pointers to help slice into just the relevent datas
    startOfSlice := -1
    endOfSlice := -1
    
	for bar := 0; bar < numrows; bar++ {
        dt, _ := time.Parse(time.RFC3339, forexData[bar].Date)
        // Only add data collected between from (timeStart) and to (timeEnd) range to prevent overwriting or confusion when aggregating data
        if dt.UTC().Unix() >= from.UTC().Unix() && dt.UTC().Unix() <= to.UTC().Unix() {
            if startOfSlice == -1 {
                startOfSlice = bar
            }
            endOfSlice = bar
            quote.Epoch[bar] = dt.UTC().Unix()
            quote.Open[bar] = forexData[bar].Open
            quote.High[bar] = forexData[bar].High
            quote.Low[bar] = forexData[bar].Low
            quote.Close[bar] = forexData[bar].Close
        }
	}
    
    quote.Epoch = quote.Epoch[startOfSlice:endOfSlice]
    quote.Open = quote.Open[startOfSlice:endOfSlice]
    quote.High = quote.High[startOfSlice:endOfSlice]
    quote.Low = quote.Low[startOfSlice:endOfSlice]
    quote.Close = quote.Close[startOfSlice:endOfSlice]
    
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
			log.Info("Forex: Tiingo error downloading " + symbol)
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
    ApiKey2        string   `json:"api_key2"`
	QueryStart     string   `json:"query_start"`
	BaseTimeframe  string   `json:"base_timeframe"`
}

// TiingoForexFetcher is the main worker for TiingoForex
type TiingoForexFetcher struct {
	config         map[string]interface{}
	symbols        []string
    apiKey         string
    apiKey2         string
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
    
	return &TiingoForexFetcher{
		config:         conf,
		symbols:        symbols,
        apiKey:         config.ApiKey,
        apiKey2:        config.ApiKey2,
		queryStart:     queryStart,
		baseTimeframe:  utils.NewTimeframe(timeframeStr),
	}, nil
}

// Run grabs data in intervals from starting time to ending time.
// If query_end is not set, it will run forever.
func (tiifx *TiingoForexFetcher) Run() {
    
	realTime := false    
	timeStart := time.Time{}
	
    // Get last timestamp collected
	for _, symbol := range tiifx.symbols {
        tbk := io.NewTimeBucketKey(symbol + "/" + tiifx.baseTimeframe.String + "/OHLC")
        lastTimestamp := findLastTimestamp(tbk)
        log.Info("Forex: lastTimestamp for %s = %v", symbol, lastTimestamp)
        if timeStart.IsZero() || (!lastTimestamp.IsZero() && lastTimestamp.Before(timeStart)) {
            timeStart = lastTimestamp.UTC()
        }
	}
    
	// Set start time if not given.
	if !tiifx.queryStart.IsZero() {
		timeStart = tiifx.queryStart.UTC()
	} else {
		timeStart = time.Now().UTC().Add(-tiifx.baseTimeframe.Duration)
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
            timeEnd = timeStart.Add(tiifx.baseTimeframe.Duration * 99) // Under Intrinio's limit of 100 records per request
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
        */
        year := timeEnd.Year()
        month := timeEnd.Month()
        day := timeEnd.Day()
        hour := timeEnd.Hour()
        minute := timeEnd.Minute()
        timeEnd = time.Date(year, month, day, hour, minute, 0, 0, time.UTC)
        
        log.Info("%v-%v", timeStart, timeEnd)
        tiingoQuotes, _ := GetTiingoPricesFromSymbols(tiifx.symbols, timeStart, timeEnd, realTime, tiifx.baseTimeframe.String, tiifx.apiKey)
        intrinioQuotes, _ := GetIntrinioPricesFromSymbols(tiifx.symbols, timeStart, timeEnd, realTime, tiifx.baseTimeframe.String, tiifx.apiKey2)
        quotes := Quotes{}
        
        // Aggregate Tiingo and Intrinio quotes
        for _, symbol := range tiifx.symbols {
            for _, tiingoQuote := range tiingoQuotes {
                if tiingoQuote.Symbol == symbol {
                    for _, intrinioQuote := range intrinioQuotes {
                        if intrinioQuote.Symbol == symbol {
                            // Both Quotes are for the same symbol here
                            // Combine, or add to quotes
                            // They should have the same len since we only add the requested time range to quotes
                            if len(tiingoQuote.Epoch) > 0 && len(intrinioQuote.Epoch) > 0 {
                                numrows := len(intrinioQuote.Epoch)
                                quote := NewQuote(symbol, numrows)
                                for bar := 0; bar < numrows; bar++ {
                                    if tiingoQuote.Epoch[bar] != intrinioQuote.Epoch[bar] {
                                        log.Info("Forex: Tiingo and Intrinio do not match in Epochs! Tiingo: %v, Intrinio %v", tiingoQuote.Epoch[bar], intrinioQuote.Epoch[bar])
                                        // If flagged, the records are probably sorted in opposing orders
                                    } else {
                                        quote.Epoch[bar] = tiingoQuote.Epoch[bar]
                                        quote.Open[bar] = (tiingoQuote.Open[bar] + intrinioQuote.Open[bar]) / 2
                                        quote.High[bar] = (tiingoQuote.High[bar] + intrinioQuote.High[bar]) / 2
                                        quote.Low[bar] = (tiingoQuote.Low[bar] + intrinioQuote.Low[bar]) / 2
                                        quote.Close[bar] = (tiingoQuote.Close[bar] + intrinioQuote.Close[bar]) / 2
                                    }
                                }
                                quotes = append(quotes, quote)   
                            } else if len(tiingoQuote.Epoch) > 0 {
                                quotes = append(quotes, tiingoQuote)
                            } else if len(intrinioQuote.Epoch) > 0 {
                                quotes = append(quotes, intrinioQuote)
                            } else {
                                log.Info("Forex: Fringe case where all data providers returned no data")
                            }
                        }
                    }
                }
            }
        }
        
        for _, quote := range quotes {
            // Check if there are entries to write
            if len(quote.Epoch) < 1 {
                continue
            }
            if realTime {
                // Check if realTime entry already exists or is still the latest to prevent overwriting and retriggering stream
                if timeEnd.Unix() > quote.Epoch[0] && timeEnd.Unix() > quote.Epoch[len(quote.Epoch)-1] {
                    // We assume that the head or tail of the slice is the earliest/latest entry received from data provider; and
                    // compare it against the timeEnd, which is the timestamp we want to write to the bucket; and
                    // if this is insufficient, we can always query the lastTimestamp from tbk
                    log.Info("Forex: Row dated %v is still the latest in %s/%s/OHLC", time.Unix(quote.Epoch[len(quote.Epoch)-1], 0).UTC(), quote.Symbol, tiifx.baseTimeframe.String)
                    continue
                } else {
                    log.Info("Forex: Realtiming %v row(s) to %s/%s/OHLC from %v to %v", len(quote.Epoch), quote.Symbol, tiifx.baseTimeframe.String, timeStart, timeEnd)
                }
            } else {
                log.Info("Forex: Backfilling %v rows to %s/%s/OHLC from %v to %v", len(quote.Epoch), quote.Symbol, tiifx.baseTimeframe.String, timeStart, timeEnd)
            }
            // write to csm
            cs := io.NewColumnSeries()
            cs.AddColumn("Epoch", quote.Epoch)
            cs.AddColumn("Open", quote.Open)
            cs.AddColumn("High", quote.High)
            cs.AddColumn("Low", quote.Low)
            cs.AddColumn("Close", quote.Close)
            csm := io.NewColumnSeriesMap()
            tbk := io.NewTimeBucketKey(quote.Symbol + "/" + tiifx.baseTimeframe.String + "/OHLC")
            csm.AddColumnSeries(*tbk, cs)
            executor.WriteCSM(csm, false)
            
            // Add flipped pairs
            revSymbol := ""
            if strings.HasPrefix(quote.Symbol, "USD") {
                revSymbol = strings.Replace(quote.Symbol, "USD", "", -1) + "USD"
            } else if strings.HasPrefix(quote.Symbol, "EUR") {
                revSymbol = strings.Replace(quote.Symbol, "EUR", "", -1) + "EUR"
            } else if strings.HasPrefix(quote.Symbol, "JPY") {
                revSymbol = strings.Replace(quote.Symbol, "JPY", "", -1) + "JPY"
            }
            if revSymbol != "" {
                numrows := len(quote.Epoch)
                revQuote := NewQuote(revSymbol, numrows)
                for bar := 0; bar < numrows; bar++ {
                    revQuote.Epoch[bar] = 1/quote.Epoch[bar]
                    revQuote.Open[bar] = 1/quote.Open[bar]
                    revQuote.High[bar] = 1/quote.High[bar]
                    revQuote.Low[bar] = 1/quote.Low[bar]
                    revQuote.Close[bar] = 1/quote.Close[bar]
                }
                if numrows > 1 {
                    log.Info("Forex: Writing %v rows to %s/%s/OHLC from %v to %v", len(revQuote.Epoch), revQuote.Symbol, tiifx.baseTimeframe.String, timeStart, timeEnd)
                } else {
                    log.Info("Forex: Writing to %s/%s/OHLC from %v to %v", revQuote.Symbol, tiifx.baseTimeframe.String, timeStart, timeEnd)
                }
                // write to csm
                cs := io.NewColumnSeries()
                cs.AddColumn("Epoch", revQuote.Epoch)
                cs.AddColumn("Open", revQuote.Open)
                cs.AddColumn("High", revQuote.High)
                cs.AddColumn("Low", revQuote.Low)
                cs.AddColumn("Close", revQuote.Close)
                csm := io.NewColumnSeriesMap()
                tbk := io.NewTimeBucketKey(revQuote.Symbol + "/" + tiifx.baseTimeframe.String + "/OHLC")
                csm.AddColumnSeries(*tbk, cs)
                executor.WriteCSM(csm, false)
            }
        }
        
		if realTime {
			// Sleep till next :00 time
            // This function ensures that we will always get full candles
			waitTill = time.Now().UTC().Add(tiifx.baseTimeframe.Duration)
            waitTill = time.Date(waitTill.Year(), waitTill.Month(), waitTill.Day(), waitTill.Hour(), waitTill.Minute(), 0, 0, time.UTC)
            log.Info("Forex: Next request at %v", waitTill)
			time.Sleep(waitTill.Sub(time.Now().UTC()))
		} else {
			time.Sleep(time.Second*33)
		}
	}
}

func main() {
}
