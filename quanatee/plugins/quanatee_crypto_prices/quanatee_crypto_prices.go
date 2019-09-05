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
	Volume    []float64   `json:"volume"`
}

// Quotes - an array of historical price data
type Quotes []Quote

// ClientTimeout - connect/read timeout for client requests
const ClientTimeout = 15 * time.Second

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

func GetTiingoPrices(symbol string, from, to, last time.Time, realTime bool, period *utils.Timeframe, token string) (Quote, error) {

	resampleFreq := "1hour"
	switch period.String {
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

    // Try again if fail
	if err != nil {
        time.Sleep(100 * time.Millisecond)
        resp, err = client.Do(req)
    }
    
	if err != nil {
		log.Info("Crypto: symbol '%s' error: %s \n %s", symbol, err, api_url)
		return NewQuote(symbol, 0), err
	}
	defer resp.Body.Close()

	contents, _ := ioutil.ReadAll(resp.Body)
	err = json.Unmarshal(contents, &cryptoData)
	if err != nil {
		log.Info("Crypto: Tiingo symbol '%s' error: %v\n contents: %s", symbol, err, contents)
		return NewQuote(symbol, 0), err
	}
	if len(cryptoData) < 1 {
		log.Warn("Crypto: Tiingo symbol '%s' No data returned from %v-%v, url %s", symbol, from, to, api_url)
		return NewQuote(symbol, 0), err
	}
    
	numrows := len(cryptoData[0].PriceData)
	quote := NewQuote(symbol, numrows)
    // Pointers to help slice into just the relevent datas
    startOfSlice := -1
    endOfSlice := -1
    
	for bar := 0; bar < numrows; bar++ {
        dt, _ := time.Parse(time.RFC3339, cryptoData[0].PriceData[bar].Date)
        // Only add data collected between from (timeStart) and to (timeEnd) range to prevent overwriting or confusion when aggregating data
        if dt.UTC().Unix() > last.UTC().Unix() && dt.UTC().Unix() >= from.UTC().Unix() && dt.UTC().Unix() <= to.UTC().Unix() {
            if startOfSlice == -1 {
                startOfSlice = bar
            }
            endOfSlice = bar
            quote.Epoch[bar] = dt.UTC().Unix()
            quote.Open[bar] = cryptoData[0].PriceData[bar].Open
            quote.High[bar] = cryptoData[0].PriceData[bar].High
            quote.Low[bar] = cryptoData[0].PriceData[bar].Low
            quote.Close[bar] = cryptoData[0].PriceData[bar].Close
            quote.Volume[bar] = float64(cryptoData[0].PriceData[bar].Volume)
        }
	}
    
    if startOfSlice > -1 && endOfSlice > -1 {
        quote.Epoch = quote.Epoch[startOfSlice:endOfSlice+1]
        quote.Open = quote.Open[startOfSlice:endOfSlice+1]
        quote.High = quote.High[startOfSlice:endOfSlice+1]
        quote.Low = quote.Low[startOfSlice:endOfSlice+1]
        quote.Close = quote.Close[startOfSlice:endOfSlice+1]
        quote.Volume = quote.Volume[startOfSlice:endOfSlice+1]
    } else {
        quote = NewQuote(symbol, 0)
    }
    
	return quote, nil
}

// FetcherConfig is a structure of binancefeeder's parameters
type FetcherConfig struct {
	Symbols        []string `json:"symbols"`
    Indices        map[string][]interface{} `json:"indices"`
    ApiKey         string   `json:"api_key"`
	QueryStart     string   `json:"query_start"`
	BaseTimeframe  string   `json:"base_timeframe"`
}

// CryptoFetcher is the main worker for TiingoCrypto
type CryptoFetcher struct {
	config         map[string]interface{}
	symbols        []string
	indices        map[string][]string
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
	var indices map[string][]string

	if config.BaseTimeframe != "" {
		timeframeStr = config.BaseTimeframe
	}

	if config.QueryStart != "" {
		queryStart = queryTime(config.QueryStart)
	}

    log.Info("%v", config.Symbols)
	if len(config.Symbols) > 0 {
		symbols = config.Symbols
	}
    
    for key, value := range config.Indices {
        indexSymbols := make([]string, 0)
        for _, value2 := range value {
            indexSymbols = append(indexSymbols, value2.(string))
        indices[key] = indexSymbols
    }
    
    log.Info("%v", config.Indices)
    log.Info("%v", indices)
	
	return &CryptoFetcher{
		config:         conf,
		symbols:        symbols,
		indices:        indices,
        apiKey:         config.ApiKey,
		queryStart:     queryStart,
		baseTimeframe:  utils.NewTimeframe(timeframeStr),
	}, nil
}

// Run grabs data in intervals from starting time to ending time.
// If query_end is not set, it will run forever.
func (tiicc *CryptoFetcher) Run() {
    
	realTime := false    
	timeStart := time.Time{}
	lastTimestamp := time.Time{}
	
    // Get last timestamp collected
	for _, symbol := range tiicc.symbols {
        tbk := io.NewTimeBucketKey(symbol + "/" + tiicc.baseTimeframe.String + "/OHLCV")
        lastTimestamp = findLastTimestamp(tbk)
        log.Info("Crypto: lastTimestamp for %s = %v", symbol, lastTimestamp)
        if timeStart.IsZero() || (!lastTimestamp.IsZero() && lastTimestamp.Before(timeStart)) {
            timeStart = lastTimestamp.UTC()
        }
	}
    
	// Set start time if not given.
	if !tiicc.queryStart.IsZero() {
		timeStart = tiicc.queryStart.UTC()
	} else {
		timeStart = time.Now().UTC()
	}
    
	// For loop for collecting candlestick data forever
	var timeEnd time.Time
	var waitTill time.Time
	firstLoop := true
    
	for {
        
        if firstLoop {
            firstLoop = false
        } else {
            timeStart = timeEnd
        }
        if realTime {
            // Add timeEnd by a tick
            timeEnd = timeStart.Add(tiicc.baseTimeframe.Duration)
        } else {
            // Add timeEnd by a range
            timeEnd = timeStart.AddDate(0, 0, 1)
            if timeEnd.After(time.Now().UTC()) {
                // timeEnd is after current time
                realTime = true
                timeEnd = time.Now().UTC()
            }
        }
        
        log.Info("Crypto: %v-%v", timeStart, timeEnd)
        
        if !firstLoop {
            
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
            symbols := tiicc.symbols
            rand.Shuffle(len(symbols), func(i, j int) { symbols[i], symbols[j] = symbols[j], symbols[i] })
            // Data for symbols are retrieved in random order for fairness
            // Data for symbols are written immediately for asynchronous-like processing
            for _, symbol := range symbols {
                time.Sleep(150 * time.Millisecond)
                time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
                quote, err := GetTiingoPrices(symbol, timeStart, timeEnd, lastTimestamp, realTime, tiicc.baseTimeframe, tiicc.apiKey)
                if err == nil {
                    if len(quote.Epoch) < 1 {
                        // Check if there is data to add
                        continue
                    } else if realTime && lastTimestamp.Unix() >= quote.Epoch[0] && lastTimestamp.Unix() >= quote.Epoch[len(quote.Epoch)-1] {
                        // Check if realTime is adding the most recent data
                        log.Info("Crypto: Previous row dated %v is still the latest in %s/%s/OHLCV", time.Unix(quote.Epoch[len(quote.Epoch)-1], 0).UTC(), quote.Symbol, tiicc.baseTimeframe.String)
                        continue
                    }
                    // write to csm
                    cs := io.NewColumnSeries()
                    cs.AddColumn("Epoch", quote.Epoch)
                    cs.AddColumn("Open", quote.Open)
                    cs.AddColumn("High", quote.High)
                    cs.AddColumn("Low", quote.Low)
                    cs.AddColumn("Close", quote.Close)
                    cs.AddColumn("Volume", quote.Volume)
                    csm := io.NewColumnSeriesMap()
                    tbk := io.NewTimeBucketKey(quote.Symbol + "/" + tiicc.baseTimeframe.String + "/OHLCV")
                    csm.AddColumnSeries(*tbk, cs)
                    executor.WriteCSM(csm, false)
                    
                    // Save the latest timestamp written
                    lastTimestamp = time.Unix(quote.Epoch[len(quote.Epoch)-1], 0)
                    log.Info("Crypto: %v row(s) to %s/%s/OHLCV from %v to %v", len(quote.Epoch), quote.Symbol, tiicc.baseTimeframe.String, time.Unix(quote.Epoch[0], 0).UTC(), time.Unix(quote.Epoch[len(quote.Epoch)-1], 0).UTC())
                    quotes = append(quotes, quote)
                } else {
                    log.Info("Crypto: error downloading " + symbol)
                }
            }
            
            // Add reversed pairs
            for _, quote := range quotes {
                revSymbol := ""
                if strings.HasSuffix(quote.Symbol, "USD") {
                    revSymbol = "USD" + strings.Replace(quote.Symbol, "USD", "", -1)
                } else if strings.HasSuffix(quote.Symbol, "EUR") {
                    revSymbol = "EUR" + strings.Replace(quote.Symbol, "EUR", "", -1)
                } else if strings.HasSuffix(quote.Symbol, "JPY") {
                    revSymbol = "JPY" + strings.Replace(quote.Symbol, "JPY", "", -1)
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
                        revQuote.Volume[bar] = (quote.Close[bar]*quote.Volume[bar]) / revQuote.Close[bar]
                    }
                    // write to csm
                    cs := io.NewColumnSeries()
                    cs.AddColumn("Epoch", revQuote.Epoch)
                    cs.AddColumn("Open", revQuote.Open)
                    cs.AddColumn("High", revQuote.High)
                    cs.AddColumn("Low", revQuote.Low)
                    cs.AddColumn("Close", revQuote.Close)
                    cs.AddColumn("Volume", revQuote.Volume)
                    csm := io.NewColumnSeriesMap()
                    tbk := io.NewTimeBucketKey(revQuote.Symbol + "/" + tiicc.baseTimeframe.String + "/OHLCV")
                    csm.AddColumnSeries(*tbk, cs)
                    executor.WriteCSM(csm, false)
                    
                    log.Info("Crypto: %v row(s) to %s/%s/OHLCV from %v to %v", len(revQuote.Epoch), revQuote.Symbol, tiicc.baseTimeframe.String, time.Unix(revQuote.Epoch[0], 0).UTC(), time.Unix(revQuote.Epoch[len(revQuote.Epoch)-1], 0).UTC())
                    quotes = append(quotes, revQuote)
                }
            }
            
            aggQuotes := Quotes{}
            for key, value := range tiicc.indices {
                aggQuote := NewQuote(key, 0)
                for _, quote := range quotes {
                    for _, symbol := range value {
                        if quote.Symbol == symbol {
                            if len(quote.Epoch) > 0 {
                                if len(aggQuote.Epoch) == 0 {
                                    aggQuote.Epoch = quote.Epoch
                                    aggQuote.Open = quote.Open
                                    aggQuote.High = quote.High
                                    aggQuote.Low = quote.Low
                                    aggQuote.Close = quote.Close
                                } else if len(aggQuote.Epoch) == len(quote.Epoch) {
                                    numrows := len(aggQuote.Epoch)
                                    for bar := 0; bar < numrows; bar++ {
                                        // Calculate the market capitalization
                                        quote_cap := (quote.Close[bar] * quote.Volume[bar])
                                        aggQuote_cap := (aggQuote.Close[bar] * aggQuote.Volume[bar])
                                        total_cap := quote_cap + aggQuote_cap
                                        // Calculate the weighted averages
                                        aggQuote.Open[bar] = ( quote.Open[bar] * ( quote_cap / total_cap ) ) + ( aggQuote.Open[bar] * ( aggQuote_cap / total_cap ) )
                                        aggQuote.High[bar] = ( quote.High[bar] * ( quote_cap / total_cap ) ) + ( aggQuote.High[bar] * ( aggQuote_cap / total_cap ) )
                                        aggQuote.Low[bar] = ( quote.Low[bar] * ( quote_cap / total_cap ) ) + ( aggQuote.Low[bar] * ( aggQuote_cap / total_cap ) )
                                        aggQuote.Close[bar] = ( quote.Close[bar] * ( quote_cap / total_cap ) ) + ( aggQuote.Close[bar] * ( aggQuote_cap / total_cap ) )
                                        aggQuote.Volume[bar] = total_cap / aggQuote.Close[bar]
                                    }
                                }
                            }
                        }
                    }
                }
                if len(aggQuote.Epoch) > 0 {
                    aggQuotes = append(aggQuotes, aggQuote)
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
                cs.AddColumn("Volume", quote.Volume)
                csm := io.NewColumnSeriesMap()
                tbk := io.NewTimeBucketKey(quote.Symbol + "/" + tiicc.baseTimeframe.String + "/OHLCV")
                csm.AddColumnSeries(*tbk, cs)
                executor.WriteCSM(csm, false)
                
                log.Info("Crypto: %v row(s) to %s/%s/OHLCV from %v to %v", len(quote.Epoch), quote.Symbol, tiicc.baseTimeframe.String, time.Unix(quote.Epoch[0], 0).UTC(), time.Unix(quote.Epoch[len(quote.Epoch)-1], 0).UTC())
            }
        }
		if realTime {
			// Sleep till the next minute
            // This function ensures that we will always get full candles
			waitTill = time.Now().UTC().Add(tiicc.baseTimeframe.Duration)
            waitTill = time.Date(waitTill.Year(), waitTill.Month(), waitTill.Day(), waitTill.Hour(), waitTill.Minute(), 3, 0, time.UTC)
            log.Info("Crypto: Next request at %v", waitTill)
			time.Sleep(waitTill.Sub(time.Now().UTC()))
		} else {
			time.Sleep(time.Second*60)
		}
	}
}

func main() {
}
