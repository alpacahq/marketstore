package main

import (
	"flag"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"strings"
	"time"

	"github.com/alpacahq/marketstore/contrib/calendar"
	"github.com/alpacahq/marketstore/contrib/ondiskagg/aggtrigger"
	"github.com/alpacahq/marketstore/executor"
	"github.com/alpacahq/marketstore/plugins/trigger"
	"github.com/alpacahq/marketstore/utils"
	. "github.com/alpacahq/marketstore/utils/io"
	"github.com/alpacahq/marketstore/utils/log"
	iex "github.com/timpalpant/go-iex"
	"github.com/timpalpant/go-iex/consolidator"
	"github.com/timpalpant/go-iex/iextp/tops"
)

var (
	dir       string
	from      string
	to        string
	csSymbols string

	// NY timezone
	NY, _  = time.LoadLocation("America/New_York")
	format = "2006-01-02"

	symbolMask = map[string]struct{}{}
)

func init() {
	flag.StringVar(&dir, "dir", "/project/data", "mktsdb directory to backfill to")
	flag.StringVar(&from, "from", time.Now().Add(-365*24*time.Hour).Format(format), "backfill from date (YYYY-MM-DD)")
	flag.StringVar(&to, "to", time.Now().Format(format), "backfill from date (YYYY-MM-DD)")
	flag.StringVar(&csSymbols, "symbols", "", "comma-separated symbols to backfill")

	flag.Parse()

	if csSymbols != "" {
		for _, symbol := range strings.Split(csSymbols, ",") {
			symbolMask[symbol] = struct{}{}
		}
	}
}

func main() {
	initWriter()

	start, err := time.Parse(format, from)
	if err != nil {
		log.Fatal(err.Error())
	}

	end, err := time.Parse(format, to)
	if err != nil {
		log.Fatal(err.Error())
	}

	log.Info("backfilling from %v to %v", start.Format(format), end.Format(format))

	sem := make(chan struct{}, runtime.NumCPU())

	for end.After(start) {
		if calendar.Nasdaq.IsMarketDay(end) {
			sem <- struct{}{}
			go func(t time.Time) {
				defer func() { <-sem }()
				log.Info("backfilling %v...", t.Format("2006-01-02"))
				s := time.Now()
				pullDate(t)
				log.Info("Done %v (in %s)", t.Format("2006-01-02"), time.Now().Sub(s).String())
			}(end)
		}

		end = end.Add(-24 * time.Hour)
	}

	// make sure all goroutines finish
	for i := 0; i < cap(sem); i++ {
		sem <- struct{}{}
	}
}

func pullDate(t time.Time) {
	client := iex.NewClient(http.DefaultClient)

	histData, err := client.GetHIST(t)
	if err != nil {
		// no file for this day
		if strings.Contains(err.Error(), "404") {
			log.Warn("404 for date: %v", t)
			return
		}
		panic(err)
	} else if len(histData) == 0 {
		panic(fmt.Errorf("Found %v available data feeds", len(histData)))
	}

	var topsLink string
	for _, hist := range histData {
		if hist.Feed == "TOPS" {
			topsLink = hist.Link
			break
		}
	}

	// Fetch the pcap dump for that date and iterate through its messages.
	resp, err := http.Get(topsLink)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	packetSource, err := iex.NewPacketDataSource(resp.Body)
	if err != nil {
		log.Fatal(err.Error())
	}

	scanner := iex.NewPcapScanner(packetSource)

	var (
		trades    []*tops.TradeReportMessage
		openTime  time.Time
		closeTime time.Time

		quotes []*tops.QuoteUpdateMessage
	)

	const QuoteBufferSize = 1024 * 512
	quotes = make([]*tops.QuoteUpdateMessage, 0, QuoteBufferSize)

	for {
		msg, err := scanner.NextMessage()
		if err != nil {
			if err == io.EOF {
				break
			}

			log.Fatal(err.Error())
		}

		switch tick := msg.(type) {
		case *tops.TradeReportMessage:
			trade := tick
			if len(symbolMask) > 0 {
				if _, ok := symbolMask[trade.Symbol]; !ok {
					continue
				}
			}
			if openTime.IsZero() {
				openTime = trade.Timestamp.Truncate(time.Minute)
				closeTime = openTime.Add(time.Minute)
			}

			if (trade.Timestamp.Equal(closeTime) || trade.Timestamp.After(closeTime)) && len(trades) > 0 {
				symBars := makeSymBars(trades, openTime, closeTime)

				if err := writeSymBars(symBars); err != nil {
					log.Fatal(err.Error())
				}
				if err := writeTrades(trades); err != nil {
					log.Fatal(err.Error())
				}

				trades = trades[:0]
				openTime = trade.Timestamp.Truncate(time.Minute)
				closeTime = openTime.Add(time.Minute)
			}

			trades = append(trades, trade)
		case *tops.QuoteUpdateMessage:
			quote := tick
			if len(quotes) == cap(quotes)-1 {
				if err := writeQuotes(quotes); err != nil {
					log.Fatal(err.Error())
				}
				quotes = quotes[:0]

			}
			quotes = append(quotes, quote)
		default:
		}
	}

	if len(trades) > 0 {
		symBars := makeSymBars(trades, openTime, closeTime)

		if err := writeSymBars(symBars); err != nil {
			log.Fatal(err.Error())
		}
		if err := writeTrades(trades); err != nil {
			log.Fatal(err.Error())
		}
		if err := writeQuotes(quotes); err != nil {
			log.Fatal(err.Error())
		}
	}
}

func makeSymBars(trades []*tops.TradeReportMessage, openTime, closeTime time.Time) map[string]*consolidator.Bar {
	symBars := map[string]*consolidator.Bar{}

	for _, trade := range trades {
		symbol := trade.Symbol
		price := trade.Price
		if _, ok := symBars[symbol]; !ok {
			symBars[symbol] = &consolidator.Bar{
				Symbol:    symbol,
				Open:      price,
				High:      price,
				Low:       price,
				Close:     price,
				Volume:    int64(trade.Size),
				OpenTime:  openTime,
				CloseTime: closeTime,
			}
		} else {
			bar := symBars[symbol]
			if bar.High < price {
				bar.High = price
			}
			if bar.Low > price {
				bar.Low = price
			}
			bar.Close = price
			bar.Volume += int64(trade.Size)
		}
	}
	return symBars
}

func writeSymBars(symBars map[string]*consolidator.Bar) error {
	csm := NewColumnSeriesMap()
	for symbol, bar := range symBars {
		tbk := NewTimeBucketKeyFromString(fmt.Sprintf("%s/1Min/OHLCV", symbol))

		cs := NewColumnSeries()
		cs.AddColumn("Epoch", []int64{bar.OpenTime.Unix()})
		cs.AddColumn("Open", []float32{float32(bar.Open)})
		cs.AddColumn("High", []float32{float32(bar.High)})
		cs.AddColumn("Low", []float32{float32(bar.Low)})
		cs.AddColumn("Close", []float32{float32(bar.Close)})
		cs.AddColumn("Volume", []int32{int32(bar.Volume)})
		csm.AddColumnSeries(*tbk, cs)
	}

	return executor.WriteCSM(csm, false)
}

func writeBars(bars []*consolidator.Bar) error {
	csm := NewColumnSeriesMap()

	for i := range bars {
		batch, index := nextBatch(bars, i)

		if len(batch) > 0 {
			tbk := NewTimeBucketKeyFromString(fmt.Sprintf("%s/1Min/OHLCV", batch[0].Symbol))

			epoch := make([]int64, len(batch))
			open := make([]float32, len(batch))
			high := make([]float32, len(batch))
			low := make([]float32, len(batch))
			close := make([]float32, len(batch))
			volume := make([]int32, len(batch))

			for j, bar := range batch {
				epoch[j] = bar.OpenTime.Unix()
				open[j] = float32(bar.Open)
				high[j] = float32(bar.High)
				low[j] = float32(bar.Low)
				close[j] = float32(bar.Close)
				volume[j] = int32(bar.Volume)
			}

			cs := NewColumnSeries()
			cs.AddColumn("Epoch", epoch)
			cs.AddColumn("Open", open)
			cs.AddColumn("High", high)
			cs.AddColumn("Low", low)
			cs.AddColumn("Close", close)
			cs.AddColumn("Volume", volume)
			csm.AddColumnSeries(*tbk, cs)
		}

		if index == len(bars) {
			break
		}
	}

	return executor.WriteCSM(csm, false)
}

func writeTrades(trades []*tops.TradeReportMessage) error {
	csm := NewColumnSeriesMap()

	type schema struct {
		epoch []int64
		nanos []int32
		px    []float32
		sz    []int32
		cond  []int32
	}

	mapSchema := map[string]*schema{}

	for _, trade := range trades {
		symbol := trade.Symbol

		if _, ok := mapSchema[symbol]; !ok {
			mapSchema[symbol] = &schema{}
		}
		cols := mapSchema[symbol]

		cols.epoch = append(cols.epoch, trade.Timestamp.Unix())
		cols.nanos = append(cols.nanos, int32(trade.Timestamp.Nanosecond()))
		cols.px = append(cols.px, float32(trade.Price))
		cols.sz = append(cols.sz, int32(trade.Size))
		cols.cond = append(cols.cond, int32(trade.SaleConditionFlags))
	}

	for symbol, cols := range mapSchema {
		tbk := NewTimeBucketKey(symbol + "/1Min/TRADE")

		csm.AddColumn(*tbk, "Epoch", cols.epoch)
		csm.AddColumn(*tbk, "Nanoseconds", cols.nanos)
		csm.AddColumn(*tbk, "Price", cols.px)
		csm.AddColumn(*tbk, "Size", cols.sz)
		//csm.AddColumn(*tbk, "Condition", cols.cond)
	}

	return executor.WriteCSM(csm, true)
}

func writeQuotes(quotes []*tops.QuoteUpdateMessage) error {
	csm := NewColumnSeriesMap()

	type schema struct {
		epoch        []int64
		nanos        []int32
		bidPx, askPx []float32
		bidSz, askSz []int32
		flags        []int32
	}

	mapSchema := map[string]*schema{}

	for _, quote := range quotes {
		symbol := quote.Symbol

		if _, ok := mapSchema[symbol]; !ok {
			mapSchema[symbol] = &schema{}
		}
		cols := mapSchema[symbol]

		cols.epoch = append(cols.epoch, quote.Timestamp.Unix())
		cols.nanos = append(cols.nanos, int32(quote.Timestamp.Nanosecond()))
		cols.bidPx = append(cols.bidPx, float32(quote.BidPrice))
		cols.askPx = append(cols.askPx, float32(quote.AskPrice))
		cols.bidSz = append(cols.bidSz, int32(quote.BidSize))
		cols.askSz = append(cols.askSz, int32(quote.AskSize))
		cols.flags = append(cols.flags, int32(quote.Flags))
	}

	for symbol, cols := range mapSchema {
		tbk := NewTimeBucketKey(symbol + "/1Min/QUOTE")

		csm.AddColumn(*tbk, "Epoch", cols.epoch)
		csm.AddColumn(*tbk, "Nanoseconds", cols.nanos)
		csm.AddColumn(*tbk, "BidPrice", cols.bidPx)
		csm.AddColumn(*tbk, "AskPrice", cols.askPx)
		csm.AddColumn(*tbk, "BidSize", cols.bidSz)
		csm.AddColumn(*tbk, "AskSize", cols.askSz)
		//csm.AddColumn(*tbk, "Flags", cols.flags)
	}

	return executor.WriteCSM(csm, true)
}

func nextBatch(bars []*consolidator.Bar, index int) ([]*consolidator.Bar, int) {
	batch := []*consolidator.Bar{}

	for i, bar := range bars[index:] {
		if i > 0 && !strings.EqualFold(bar.Symbol, bars[i-1].Symbol) {
			return batch, i
		}

		batch = append(batch, bar)
	}

	return batch, len(bars)
}

func initWriter() {
	utils.InstanceConfig.Timezone = NY
	utils.InstanceConfig.WALRotateInterval = 5

	executor.NewInstanceSetup(
		fmt.Sprintf("%v/mktsdb", dir),
		true, true, true, true)

	log.Info(
		"Initialized writer with InstanceID: %v - RootDir: %v\n",
		executor.ThisInstance.InstanceID,
		executor.ThisInstance.RootDir,
	)

	config := map[string]interface{}{
		"filter":       "nasdaq",
		"destinations": []string{"5Min", "15Min", "1H"},
	}

	trig, err := aggtrigger.NewTrigger(config)
	if err != nil {
		log.Fatal(err.Error())
	}

	executor.ThisInstance.TriggerMatchers = []*trigger.TriggerMatcher{
		trigger.NewMatcher(trig, "*/1Min/OHLCV"),
	}
}
