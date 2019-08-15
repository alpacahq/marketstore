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
		log.Warn("Crypto: symbol '%s' No data returned from %v-%v", symbol, from, to)
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
        quote.Epoch = quote.Epoch[startOfSlice:endOfSlice]
        quote.Open = quote.Open[startOfSlice:endOfSlice]
        quote.High = quote.High[startOfSlice:endOfSlice]
        quote.Low = quote.Low[startOfSlice:endOfSlice]
        quote.Close = quote.Close[startOfSlice:endOfSlice]
        quote.Volume = quote.Volume[startOfSlice:endOfSlice]
    } else {
        quote = NewQuote(symbol, 0)
    }
    
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
            timeEnd = timeStart.Add(tiicc.baseTimeframe.Duration * 999) // Under Tiingo's limit of 5000 records per request
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
        
        quotes, _ := GetTiingoPricesFromSymbols(tiicc.symbols, timeStart, timeEnd, realTime, tiicc.baseTimeframe.String, tiicc.apiKey)
        
        // Combine original quotes with aggregated quotes for aggregation into index currencies (BTCZ, USDZ, EURZ)
        finalQuotes := Quotes{}
        
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
                    log.Info("Crypto: Row dated %v is still the latest in %s/%s/OHLC", time.Unix(quote.Epoch[len(quote.Epoch)-1], 0).UTC(), quote.Symbol, tiicc.baseTimeframe.String)
                    continue
                }
            }
            // Add to finalQuotes
            finalQuotes = append(finalQuotes, quote)
        }
        
        // Add BTCZ
        if len(tiicc.btczSymbols) > 0 {
            btcz_quote := NewQuote("BTCZ", 0)
            for _, quote := range finalQuotes {
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
            finalQuotes = append(finalQuotes, btcz_quote)
        }
        // Add USDZ
        if len(tiicc.usdzSymbols) > 0 {
            usdz_quote := NewQuote("USDZ", 0)
            for _, quote := range finalQuotes {
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
            finalQuotes = append(finalQuotes, usdz_quote)
        }
        // Add EURZ
        if len(tiicc.eurzSymbols) > 0 {
            eurz_quote := NewQuote("EURZ", 0)
            for _, quote := range finalQuotes {
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
            finalQuotes = append(finalQuotes, eurz_quote)
        }
        
        for _, quote := range finalQuotes {
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
            
            log.Info("Crypto: Writing %v row(s) to %s/%s/OHLC from %v to %v", len(quote.Epoch), quote.Symbol, tiicc.baseTimeframe.String, timeStart, timeEnd)
        }
        
		if realTime {
			// Sleep till next interval for data provider to update candles
            // This function ensures that we will always get full candles
			waitTill = time.Now().UTC().Add(tiicc.baseTimeframe.Duration)
            waitTill = time.Date(waitTill.Year(), waitTill.Month(), waitTill.Day(), waitTill.Hour(), waitTill.Minute(), 1, 0, time.UTC)
            log.Info("Crypto: Next request at %v", waitTill)
			time.Sleep(waitTill.Sub(time.Now().UTC()))
		} else {
			time.Sleep(time.Second*99)
		}
	}
}

func main() {
}
