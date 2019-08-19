package main

import (
	"encoding/json"
	"fmt"
	"math"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"
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
		log.Info("Crypto: Tiingo symbol '%s' not found\n", symbol)
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
        if dt.UTC().Unix() >= from.UTC().Unix() && dt.UTC().Unix() <= to.UTC().Unix() {
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
        quote.Epoch = quote.Epoch[startOfSlice+1:endOfSlice+1]
        quote.Open = quote.Open[startOfSlice+1:endOfSlice+1]
        quote.High = quote.High[startOfSlice+1:endOfSlice+1]
        quote.Low = quote.Low[startOfSlice+1:endOfSlice+1]
        quote.Close = quote.Close[startOfSlice+1:endOfSlice+1]
        quote.Volume = quote.Volume[startOfSlice+1:endOfSlice+1]
        if !realTime && quote.Epoch < 300 {
            log.Info("Crypto: %v", quote.Epoch)
        }
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
	QueryStart     string   `json:"query_start"`
	BaseTimeframe  string   `json:"base_timeframe"`
	BTCZSymbols   []string  `json:"btcz_symbols"`
	USDZSymbols   []string  `json:"usdz_symbols"`
	EURZSymbols   []string  `json:"eurz_symbols"`
}

// CryptoFetcher is the main worker for TiingoCrypto
type CryptoFetcher struct {
	config         map[string]interface{}
	symbols        []string
    apiKey         string
	queryStart     time.Time
	baseTimeframe  *utils.Timeframe
	btczSymbols   []string
	usdzSymbols   []string
	eurzSymbols   []string
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
    // We do not account for holidays or disruptions in Marketstore
    // Aligning time series datas is done in Quanatee functions
    
    if opening == true {
        // Set to nearest open hours time if timeCheck is over Quanatee Hours
        if ( int(timeCheck.Weekday()) == 5 && timeCheck.Hour() >= 21 ) || ( int(timeCheck.Weekday()) > 5 && int(timeCheck.Weekday()) < 1 ) || ( int(timeCheck.Weekday()) == 1 && timeCheck.Hour() < 12 ) {
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
        if ( int(timeCheck.Weekday()) == 5 && timeCheck.Hour() >= 21 ) || ( int(timeCheck.Weekday()) > 5 && int(timeCheck.Weekday()) < 1 ) || ( int(timeCheck.Weekday()) == 1 && timeCheck.Hour() < 12 ) {
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
	var btczSymbols []string
	var usdzSymbols []string
	var eurzSymbols []string

	if config.BaseTimeframe != "" {
		timeframeStr = config.BaseTimeframe
	}

	if config.QueryStart != "" {
		queryStart = queryTime(config.QueryStart)
	}

	if len(config.Symbols) > 0 {
		symbols = config.Symbols
	}
    
	if len(config.BTCZSymbols) > 0 {
		btczSymbols = config.BTCZSymbols
	}
    
	if len(config.USDZSymbols) > 0 {
		usdzSymbols = config.USDZSymbols
	}
    
	if len(config.EURZSymbols) > 0 {
		eurzSymbols = config.EURZSymbols
	}
    
	return &CryptoFetcher{
		config:         conf,
		symbols:        symbols,
        apiKey:         config.ApiKey,
		queryStart:     queryStart,
		baseTimeframe:  utils.NewTimeframe(timeframeStr),
        btczSymbols:    btczSymbols,  
        usdzSymbols:    usdzSymbols,
        eurzSymbols:    eurzSymbols,
	}, nil
}

// Run grabs data in intervals from starting time to ending time.
// If query_end is not set, it will run forever.
func (tiicc *CryptoFetcher) Run() {
    
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
    timeStart = alignTimeToQuanateeHours(timeStart, true)
    
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
            timeEnd = timeEnd.Add(tiicc.baseTimeframe.Duration)
        } else {
            // Add timeEnd by a range
            timeEnd = timeStart.AddDate(0, 0, 1)
            // If timeEnd is outside of Closing, set it to the closing time
            timeEnd = alignTimeToQuanateeHours(timeEnd, false)
            if alignTimeToQuanateeHours(timeStart, true).After(time.Now().UTC()) {
                // timeStart is at Closing and new timeStart (next Opening) is after current time
                firstLoop = true
                realTime = true
                timeStart = alignTimeToQuanateeHours(timeStart, true).Add(-tiicc.baseTimeframe.Duration)
                // do not run bool
            } else if timeEnd.After(time.Now().UTC()) {
                // timeEnd is after current time
                realTime = true
                timeEnd = time.Now().UTC()
            }
        }
        
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
                time.Sleep(333 * time.Millisecond)
                quote, err := GetTiingoPrices(symbol, timeStart, timeEnd, realTime, tiicc.baseTimeframe.String, tiicc.apiKey)
                if err == nil {
                    if len(quote.Epoch) < 1 {
                        // Check if there is data to add
                        continue
                    } else if realTime && timeEnd.Unix() >= quote.Epoch[0] && timeEnd.Unix() >= quote.Epoch[len(quote.Epoch)-1] {
                        // Check if realTime is adding the most recent data
                        log.Info("Crypto: Row dated %v is still the latest in %s/%s/OHLC", time.Unix(quote.Epoch[len(quote.Epoch)-1], 0).UTC(), quote.Symbol, tiicc.baseTimeframe.String)
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
                    tbk := io.NewTimeBucketKey(quote.Symbol + "/" + tiicc.baseTimeframe.String + "/OHLC")
                    csm.AddColumnSeries(*tbk, cs)
                    executor.WriteCSM(csm, false)
                    
                    log.Info("Crypto: %v row(s) to %s/%s/OHLC from %v to %v", len(quote.Epoch), quote.Symbol, tiicc.baseTimeframe.String, timeStart, timeEnd)
                    quotes = append(quotes, quote)
                } else {
                    log.Info("Crypto: error downloading " + symbol)
                }
            }
            
            aggQuotes := Quotes{}
            
            // Add BTCZ
            if len(tiicc.btczSymbols) > 0 {
                btcz_quote := NewQuote("BTCZ", 0)
                for _, quote := range quotes {
                    for _, symbol := range tiicc.btczSymbols {
                        if quote.Symbol == symbol {
                            if len(quote.Epoch) > 0 {
                                if len(btcz_quote.Epoch) == 0 {
                                    btcz_quote.Epoch = quote.Epoch
                                    btcz_quote.Open = quote.Open
                                    btcz_quote.High = quote.High
                                    btcz_quote.Low = quote.Low
                                    btcz_quote.Close = quote.Close
                                } else if len(btcz_quote.Epoch) == len(quote.Epoch) {
                                    numrows := len(btcz_quote.Epoch)
                                    for bar := 0; bar < numrows; bar++ {
                                        btcz_quote.Open[bar] = (quote.Open[bar] + btcz_quote.Open[bar]) / 2
                                        btcz_quote.High[bar] = (quote.High[bar] + btcz_quote.High[bar]) / 2
                                        btcz_quote.Low[bar] = (quote.Low[bar] + btcz_quote.Low[bar]) / 2
                                        btcz_quote.Close[bar] = (quote.Close[bar] + btcz_quote.Close[bar]) / 2
                                    }
                                }
                            }
                        }
                    }
                }
                if len(btcz_quote.Epoch) > 0 {
                    aggQuotes = append(aggQuotes, btcz_quote)
                }
            }
            // Add USDZ
            if len(tiicc.usdzSymbols) > 0 {
                usdz_quote := NewQuote("USDZ", 0)
                for _, quote := range quotes {
                    for _, symbol := range tiicc.usdzSymbols {
                        if quote.Symbol == symbol {
                            if len(quote.Epoch) > 0 {
                                if len(usdz_quote.Epoch) == 0 {
                                    usdz_quote.Epoch = quote.Epoch
                                    usdz_quote.Open = quote.Open
                                    usdz_quote.High = quote.High
                                    usdz_quote.Low = quote.Low
                                    usdz_quote.Close = quote.Close
                                } else if len(usdz_quote.Epoch) == len(quote.Epoch) {
                                    numrows := len(usdz_quote.Epoch)
                                    for bar := 0; bar < numrows; bar++ {
                                        usdz_quote.Open[bar] = (quote.Open[bar] + usdz_quote.Open[bar]) / 2
                                        usdz_quote.High[bar] = (quote.High[bar] + usdz_quote.High[bar]) / 2
                                        usdz_quote.Low[bar] = (quote.Low[bar] + usdz_quote.Low[bar]) / 2
                                        usdz_quote.Close[bar] = (quote.Close[bar] + usdz_quote.Close[bar]) / 2
                                    }
                                }
                            }
                        }
                    }
                }
                if len(usdz_quote.Epoch) > 0 {
                    aggQuotes = append(aggQuotes, usdz_quote)
                }
            }
            // Add EURZ
            if len(tiicc.eurzSymbols) > 0 {
                eurz_quote := NewQuote("EURZ", 0)
                for _, quote := range quotes {
                    for _, symbol := range tiicc.eurzSymbols {
                        if quote.Symbol == symbol {
                            if len(quote.Epoch) > 0 {
                                if len(eurz_quote.Epoch) == 0 {
                                    eurz_quote.Epoch = quote.Epoch
                                    eurz_quote.Open = quote.Open
                                    eurz_quote.High = quote.High
                                    eurz_quote.Low = quote.Low
                                    eurz_quote.Close = quote.Close
                                } else if len(eurz_quote.Epoch) == len(quote.Epoch) {
                                    numrows := len(eurz_quote.Epoch)
                                    for bar := 0; bar < numrows; bar++ {
                                        eurz_quote.Open[bar] = (quote.Open[bar] + eurz_quote.Open[bar]) / 2
                                        eurz_quote.High[bar] = (quote.High[bar] + eurz_quote.High[bar]) / 2
                                        eurz_quote.Low[bar] = (quote.Low[bar] + eurz_quote.Low[bar]) / 2
                                        eurz_quote.Close[bar] = (quote.Close[bar] + eurz_quote.Close[bar]) / 2
                                    }
                                }
                            }
                        }
                    }
                }
                if len(eurz_quote.Epoch) > 0 {
                    aggQuotes = append(aggQuotes, eurz_quote)
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
                tbk := io.NewTimeBucketKey(quote.Symbol + "/" + tiicc.baseTimeframe.String + "/OHLC")
                csm.AddColumnSeries(*tbk, cs)
                executor.WriteCSM(csm, false)
                
                log.Info("Crypto: %v row(s) to %s/%s/OHLC from %v to %v", len(quote.Epoch), quote.Symbol, tiicc.baseTimeframe.String, timeStart, timeEnd)
            }
        }
		if realTime {
			// Sleep till the next minute
            // This function ensures that we will always get full candles
			waitTill = time.Now().UTC().Add(tiicc.baseTimeframe.Duration)
            waitTill = time.Date(waitTill.Year(), waitTill.Month(), waitTill.Day(), waitTill.Hour(), waitTill.Minute(), 0, 0, time.UTC)
            // Check if timeEnd is Closing, will return Opening if so
            openTime := alignTimeToQuanateeHours(timeEnd, true)
            if openTime != timeEnd {
                // Set to wait till Opening
                waitTill = openTime
            }
            log.Info("Crypto: Next request at %v", waitTill)
			time.Sleep(waitTill.Sub(time.Now().UTC()))
		} else {
			time.Sleep(time.Second*60)
		}
	}
}

func main() {
}
