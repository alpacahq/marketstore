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
    "math/rand"
    
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
                        "https://api-v2.intrinio.com/forex/prices/%s/%s?api_key=%s&start_date=%s&start_time=%s",
                        symbol,
                        resampleFreq,
                        token,
                        url.QueryEscape(from.Format("2006-01-02")),
                        url.QueryEscape(from.Format("15:04:05")))
    
    if !realTime {
        api_url = api_url + "&end_date=" + url.QueryEscape(to.Format("2006-01-02")) + "&end_time=" + url.QueryEscape(to.Format("15:04:05"))
    }
    
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
        log.Warn("Forex: Intrinio symbol '%s' No data returned from %v-%v, url %s", symbol, from, to, api_url)
		return NewQuote(symbol, 0), err
	}
    
	numrows := len(forexData.PriceData)
	quote := NewQuote(symbol, numrows)
    // Pointers to help slice into just the relevent datas
    startOfSlice := -1
    endOfSlice := -1
    
	for bar := 0; bar < numrows; bar++ {
        dt, _ := time.Parse(time.RFC3339, forexData.PriceData[bar].Date)
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
    
    if startOfSlice > -1 && endOfSlice > -1 {
        quote.Epoch = quote.Epoch[startOfSlice:endOfSlice]
        quote.Open = quote.Open[startOfSlice:endOfSlice]
        quote.High = quote.High[startOfSlice:endOfSlice]
        quote.Low = quote.Low[startOfSlice:endOfSlice]
        quote.Close = quote.Close[startOfSlice:endOfSlice]
    } else {
        quote = NewQuote(symbol, 0)
    }
    
    // Reverse the order of slice in Intrinio because data is returned in descending (latest to earliest) whereas Tiingo does it from ascending (earliest to latest)
    for i, j := 0, len(quote.Epoch)-1; i < j; i, j = i+1, j-1 {
        quote.Epoch[i], quote.Epoch[j] = quote.Epoch[j], quote.Epoch[i]
        quote.Open[i], quote.Open[j] = quote.Open[j], quote.Open[i]
        quote.High[i], quote.High[j] = quote.High[j], quote.High[i]
        quote.Low[i], quote.Low[j] = quote.Low[j], quote.Low[i]
        quote.Close[i], quote.Close[j] = quote.Close[j], quote.Close[i]
    }

	return quote, nil
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
                        "https://api.tiingo.com/tiingo/fx/%s/prices?resampleFreq=%s&startDate=%s",
                        symbol,
                        resampleFreq,
                        url.QueryEscape(from.Format("2006-1-2")))
    
    // Pad to with an extra day if backfilling to ensure that start_date and end_date is different
    if !realTime && to.AddDate(0, 0, 1).After(time.Now().UTC()) {
        api_url = api_url + "&endDate=" + url.QueryEscape(to.AddDate(0, 0, 1).Format("2006-1-2"))
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
        log.Warn("Forex: Tiingo symbol '%s' No data returned from %v-%v, url %s", symbol, from, to, api_url)
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
    
    if startOfSlice > -1 && endOfSlice > -1 {
        quote.Epoch = quote.Epoch[startOfSlice+1:endOfSlice+1]
        quote.Open = quote.Open[startOfSlice+1:endOfSlice+1]
        quote.High = quote.High[startOfSlice+1:endOfSlice+1]
        quote.Low = quote.Low[startOfSlice+1:endOfSlice+1]
        quote.Close = quote.Close[startOfSlice+1:endOfSlice+1]
    } else {
        quote = NewQuote(symbol, 0)
    }
    
	return quote, nil
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
	USDXSymbols    []string `json:"usdx_symbols"`
	EURXSymbols    []string `json:"eurx_symbols"`
	JPYXSymbols    []string `json:"jpyx_symbols"`
}

// ForexFetcher is the main worker for TiingoForex
type ForexFetcher struct {
	config         map[string]interface{}
	symbols        []string
    apiKey         string
    apiKey2        string
	queryStart     time.Time
	baseTimeframe  *utils.Timeframe
	usdxSymbols    []string
	eurxSymbols    []string
	jpyxSymbols    []string
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

func alignTimeToQuanateeHours(timeCheck time.Time, opening bool) time.Time {
    
    // Quanatee Opening = Monday 1200 UTC is the first data we will consume in the week
    // Quanatee Closing = Friday 2100 UTC is the last data we will consume in the week
    
    if opening == true {
        // Set to nearest open hours time if timeCheck is over Quanatee Hours
        if ( int(timeCheck.Weekday()) == 5 && timeCheck.Hour() == 21 && timeCheck.Minute() > 0 ) || ( int(timeCheck.Weekday()) == 5 && timeCheck.Hour() > 21 ) || ( int(timeCheck.Weekday()) > 5 && int(timeCheck.Weekday()) < 1 ) || ( int(timeCheck.Weekday()) == 1 && timeCheck.Hour() < 12 ) {
            if int(timeCheck.Weekday()) >= 5 {
                // timeCheck is Friday or Saturday, set to Monday
                timeCheck = timeCheck.AddDate(0, 0, (8 - int(timeCheck.Weekday())))
            } else if int(timeCheck.Weekday()) == 0 {
                // timeCheck is Sunday, set to Monday
                timeCheck = timeCheck.AddDate(0, 0, 1)
            }
            // Set the Hour and Minutes
            timeCheck = time.Date(timeCheck.Year(), timeCheck.Month(), timeCheck.Day(), 12, 0, 0, 0, time.UTC)
        }
    } else {
        // Set to nearest closing hours time if timeCheck is over Quanatee Hours
        if ( int(timeCheck.Weekday()) == 5 && timeCheck.Hour() == 21 && timeCheck.Minute() > 0 ) || ( int(timeCheck.Weekday()) == 5 && timeCheck.Hour() > 21 ) || ( int(timeCheck.Weekday()) > 5 && int(timeCheck.Weekday()) < 1 ) || ( int(timeCheck.Weekday()) == 1 && timeCheck.Hour() < 12 ) {
            if int(timeCheck.Weekday()) == 6 {
                // timeCheck is Saturday, Sub 1 Day to Friday
                timeCheck = timeCheck.AddDate(0, 0, -1)
            } else if int(timeCheck.Weekday()) == 0 {
                // timeCheck is Sunday, Sub 2 Days to Friday
                timeCheck = timeCheck.AddDate(0, 0, -2)
            }
            // Set the Hour and Minutes
            timeCheck = time.Date(timeCheck.Year(), timeCheck.Month(), timeCheck.Day(), 21, 0, 0, 0, time.UTC)
        }
    }
    
    return timeCheck
}

// NewBgWorker registers a new background worker
func NewBgWorker(conf map[string]interface{}) (bgworker.BgWorker, error) {
	config := recast(conf)
	var queryStart time.Time
	timeframeStr := "1Min"
	var symbols []string
	var usdxSymbols []string
	var eurxSymbols []string
	var jpyxSymbols []string

	if config.BaseTimeframe != "" {
		timeframeStr = config.BaseTimeframe
	}

	if config.QueryStart != "" {
		queryStart = queryTime(config.QueryStart)
	}
    
	if len(config.Symbols) > 0 {
		symbols = config.Symbols
	}
    
	if len(config.USDXSymbols) > 0 {
		usdxSymbols = config.USDXSymbols
	}
    
	if len(config.EURXSymbols) > 0 {
		eurxSymbols = config.EURXSymbols
	}
    
	if len(config.JPYXSymbols) > 0 {
		jpyxSymbols = config.JPYXSymbols
	}
    
	return &ForexFetcher{
		config:         conf,
		symbols:        symbols,
        apiKey:         config.ApiKey,
        apiKey2:        config.ApiKey2,
		queryStart:     queryStart,
		baseTimeframe:  utils.NewTimeframe(timeframeStr),
        usdxSymbols:    usdxSymbols,
        eurxSymbols:    eurxSymbols,
        jpyxSymbols:    jpyxSymbols,
	}, nil
}

// Run grabs data in intervals from starting time to ending time.
// If query_end is not set, it will run forever.
func (tiifx *ForexFetcher) Run() {
    
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
                timeEnd = timeStart.Add(tiifx.baseTimeframe.Duration * 95) // Under Intrinio's limit of 100 records per request
                // If timeEnd is backfilling up to after Quanatee Hours, set to the nearest closing time
                log.Info("Forex timeEnd 1: %v", timeEnd)
                timeEnd = alignTimeToQuanateeHours(timeEnd, false)
                log.Info("Forex timeEnd 2: %v", timeEnd)
                if timeEnd.After(time.Now().UTC()) {
                    realTime = true
                    timeEnd = time.Now().UTC()
                }
            }
        }
        // If timeStart is after Quanatee Hours, set to the next opening time
        timeStart = alignTimeToQuanateeHours(timeStart, true)
        if timeStart == timeEnd {
            // If timeStart is set to the next opening hours, timeEnd will be the same as timeStart. Minus timeStart by 1 interval to get the opening data (e.g. 1159 UTC to 1200 UTC)
            log.Info("Forex timeStart 1: %v", timeStart)
            timeStart = timeStart.Add(-tiifx.baseTimeframe.Duration)
            log.Info("Forex timeStart 1: %v", timeStart)
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

        quotes := Quotes{}
        symbols := tiifx.symbols
        rand.Shuffle(len(symbols), func(i, j int) { symbols[i], symbols[j] = symbols[j], symbols[i] })
        // Data for symbols are retrieved in random order for fairness
        // Data for symbols are written immediately for asynchronous-like processing
        for _, symbol := range symbols {
            time.Sleep(333 * time.Millisecond)
            tiingoQuote, _ := GetTiingoPrices(symbol, timeStart, timeEnd, realTime, tiifx.baseTimeframe.String, tiifx.apiKey)
            intrinioQuote, _ := GetIntrinioPrices(symbol, timeStart, timeEnd, realTime, tiifx.baseTimeframe.String, tiifx.apiKey2)
            quote := NewQuote(symbol, 0)
            // If both Quotes have valid datas, combine them
            // If not, serve only the quote with valid datas
            // Both Quotes would have the same length since we only add datas according to the requested period range
            if len(tiingoQuote.Epoch) < 1 && len(intrinioQuote.Epoch) < 1 {
                // Both quotes are invalid
                continue
            } else if len(tiingoQuote.Epoch) == len(intrinioQuote.Epoch) && tiingoQuote.Epoch[0] > 0 && intrinioQuote.Epoch[0] > 0 && tiingoQuote.Epoch[len(tiingoQuote.Epoch)-1] > 0 && intrinioQuote.Epoch[len(intrinioQuote.Epoch)-1] > 0 {
                // Both quotes are valid
                if tiingoQuote.Epoch[0] != intrinioQuote.Epoch[0] || tiingoQuote.Epoch[len(tiingoQuote.Epoch)-1] != intrinioQuote.Epoch[len(intrinioQuote.Epoch)-1] {
                    // First and last epochs do not match
                    // This could be either datas returned are in different orders; or
                    // Datas returned have missing data rows (likely from Tiingo); or
                    // Improper slicing of periods
                    log.Info("Forex: %s Tiingo and Intrinio do not match in Epochs!", symbol)
                    quote = intrinioQuote
                } else {
                    // First and last epochs match, we assume that the rows are lined up
                    numrows := len(intrinioQuote.Epoch)
                    quote = NewQuote(symbol, numrows)
                    for bar := 0; bar < numrows; bar++ {
                        if tiingoQuote.Epoch[bar] != intrinioQuote.Epoch[bar] {
                            // If the rows are not lined up, we fallback to Intrinio only
                            quote.Epoch[bar] = intrinioQuote.Epoch[bar]
                            quote.Open[bar] = intrinioQuote.Open[bar]
                            quote.High[bar] = intrinioQuote.High[bar]
                            quote.Low[bar] = intrinioQuote.Low[bar]
                            quote.Close[bar] = intrinioQuote.Close[bar]
                        } else {
                            quote.Epoch[bar] = tiingoQuote.Epoch[bar]
                            quote.Open[bar] = (tiingoQuote.Open[bar] + intrinioQuote.Open[bar]) / 2
                            quote.High[bar] = (tiingoQuote.High[bar] + intrinioQuote.High[bar]) / 2
                            quote.Low[bar] = (tiingoQuote.Low[bar] + intrinioQuote.Low[bar]) / 2
                            quote.Close[bar] = (tiingoQuote.Close[bar] + intrinioQuote.Close[bar]) / 2
                        }
                    }
                }
            } else if len(tiingoQuote.Epoch) > 0 && tiingoQuote.Epoch[0] > 0 && tiingoQuote.Epoch[len(tiingoQuote.Epoch)-1] > 0 {
                // Only one quote is valid
                quote = tiingoQuote
            } else if len(intrinioQuote.Epoch) > 0 && intrinioQuote.Epoch[0] > 0 && intrinioQuote.Epoch[len(intrinioQuote.Epoch)-1] > 0 {
                // Only one quote is valid
                quote = intrinioQuote
            } else {
                continue
            }
            
            if len(quote.Epoch) < 1 {
                // Check if there is data to add
                continue
            } else if realTime && timeEnd.Unix() >= quote.Epoch[0] && timeEnd.Unix() >= quote.Epoch[len(quote.Epoch)-1] {
                // Check if realTime is adding the most recent data
                log.Info("IEX: Row dated %v is still the latest in %s/%s/OHLC", time.Unix(quote.Epoch[len(quote.Epoch)-1], 0).UTC(), quote.Symbol, tiifx.baseTimeframe.String)
                continue
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
            
            log.Info("Forex: %v row(s) to %s/%s/OHLC from %v to %v", len(quote.Epoch), quote.Symbol, tiifx.baseTimeframe.String, timeStart, timeEnd)
            quotes = append(quotes, quote)
        }
        
        // Add reversed pairs
        for _, quote := range quotes {
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
                    revQuote.Epoch[bar] = quote.Epoch[bar]
                    revQuote.Open[bar] = 1/quote.Open[bar]
                    revQuote.High[bar] = 1/quote.High[bar]
                    revQuote.Low[bar] = 1/quote.Low[bar]
                    revQuote.Close[bar] = 1/quote.Close[bar]
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
                
                log.Info("Forex: %v row(s) to %s/%s/OHLC from %v to %v", len(revQuote.Epoch), revQuote.Symbol, tiifx.baseTimeframe.String, timeStart, timeEnd)
                quotes = append(quotes, revQuote)
            }
        }
        
        // Combine original quotes with mirrored quotes for aggregation into index currencies (USDX, EURX, JPYX)
        aggQuotes := Quotes{}
        
        // Add USDX
        if len(tiifx.usdxSymbols) > 0 {
            usdx_quote := NewQuote("USDX", 0)
            for _, quote := range quotes {
                for _, symbol := range tiifx.usdxSymbols {
                    if quote.Symbol == symbol {
                        if len(quote.Epoch) > 0 {
                            if len(usdx_quote.Epoch) == 0 {
                                usdx_quote.Epoch = quote.Epoch
                                usdx_quote.Open = quote.Open
                                usdx_quote.High = quote.High
                                usdx_quote.Low = quote.Low
                                usdx_quote.Close = quote.Close
                            } else if len(usdx_quote.Epoch) == len(quote.Epoch) {
                                numrows := len(usdx_quote.Epoch)
                                for bar := 0; bar < numrows; bar++ {
                                    usdx_quote.Open[bar] = (quote.Open[bar] + usdx_quote.Open[bar]) / 2
                                    usdx_quote.High[bar] = (quote.High[bar] + usdx_quote.High[bar]) / 2
                                    usdx_quote.Low[bar] = (quote.Low[bar] + usdx_quote.Low[bar]) / 2
                                    usdx_quote.Close[bar] = (quote.Close[bar] + usdx_quote.Close[bar]) / 2
                                }
                            }
                        }
                    }
                }
            }
            if len(usdx_quote.Epoch) > 0 {
                aggQuotes = append(aggQuotes, usdx_quote)
            }
        }
        // Add EURX
        if len(tiifx.eurxSymbols) > 0 {
            eurx_quote := NewQuote("EURX", 0)
            for _, quote := range quotes {
                for _, symbol := range tiifx.eurxSymbols {
                    if quote.Symbol == symbol {
                        if len(quote.Epoch) > 0 {
                            if len(eurx_quote.Epoch) == 0 {
                                eurx_quote.Epoch = quote.Epoch
                                eurx_quote.Open = quote.Open
                                eurx_quote.High = quote.High
                                eurx_quote.Low = quote.Low
                                eurx_quote.Close = quote.Close
                            } else if len(eurx_quote.Epoch) == len(quote.Epoch) {
                                numrows := len(eurx_quote.Epoch)
                                for bar := 0; bar < numrows; bar++ {
                                    eurx_quote.Open[bar] = (quote.Open[bar] + eurx_quote.Open[bar]) / 2
                                    eurx_quote.High[bar] = (quote.High[bar] + eurx_quote.High[bar]) / 2
                                    eurx_quote.Low[bar] = (quote.Low[bar] + eurx_quote.Low[bar]) / 2
                                    eurx_quote.Close[bar] = (quote.Close[bar] + eurx_quote.Close[bar]) / 2
                                }
                            }
                        }
                    }
                }
            }
            if len(eurx_quote.Epoch) > 0 {
                aggQuotes = append(aggQuotes, eurx_quote)
            }
        }
        // Add JPYX
        if len(tiifx.jpyxSymbols) > 0 {
            jpyx_quote := NewQuote("JPYX", 0)
            for _, quote := range quotes {
                for _, symbol := range tiifx.jpyxSymbols {
                    if quote.Symbol == symbol {
                        if len(quote.Epoch) > 0 {
                            if len(jpyx_quote.Epoch) == 0 {
                                jpyx_quote.Epoch = quote.Epoch
                                jpyx_quote.Open = quote.Open
                                jpyx_quote.High = quote.High
                                jpyx_quote.Low = quote.Low
                                jpyx_quote.Close = quote.Close
                            } else if len(jpyx_quote.Epoch) == len(quote.Epoch) {
                                numrows := len(jpyx_quote.Epoch)
                                for bar := 0; bar < numrows; bar++ {
                                    jpyx_quote.Open[bar] = (quote.Open[bar] + jpyx_quote.Open[bar]) / 2
                                    jpyx_quote.High[bar] = (quote.High[bar] + jpyx_quote.High[bar]) / 2
                                    jpyx_quote.Low[bar] = (quote.Low[bar] + jpyx_quote.Low[bar]) / 2
                                    jpyx_quote.Close[bar] = (quote.Close[bar] + jpyx_quote.Close[bar]) / 2
                                }
                            }
                        }
                    }
                }
            }
            if len(jpyx_quote.Epoch) > 0 {
                aggQuotes = append(aggQuotes, jpyx_quote)
            }
        }
        
        for _, quote := range aggQuotes {
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
            
            log.Info("Forex: %v row(s) to %s/%s/OHLC from %v to %v", len(quote.Epoch), quote.Symbol, tiifx.baseTimeframe.String, timeStart, timeEnd)
        }
        
		if realTime {
			// Sleep till next :00 time
            // This function ensures that we will always get full candles
			waitTill = time.Now().UTC().Add(tiifx.baseTimeframe.Duration)
            waitTill = time.Date(waitTill.Year(), waitTill.Month(), waitTill.Day(), waitTill.Hour(), waitTill.Minute(), 0, 0, time.UTC)
            waitTill = alignTimeToQuanateeHours(waitTill, true)
            log.Info("Forex: Next request at %v", waitTill)
			time.Sleep(waitTill.Sub(time.Now().UTC()))
		} else {
			time.Sleep(time.Second*5)
		}
	}
}

func main() {
}
