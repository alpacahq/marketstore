package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"sort"
	"strconv"
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
	dir  string
	from string
	to   string

	// NY timezone
	NY, _  = time.LoadLocation("America/New_York")
	format = "2006-01-02"
)

func init() {
	flag.StringVar(&dir, "dir", "/project/data", "mktsdb directory to backfill to")
	flag.StringVar(&from, "from", time.Now().Add(-365*24*time.Hour).Format(format), "backfill from date (YYYY-MM-DD)")
	flag.StringVar(&to, "to", time.Now().Format(format), "backfill from date (YYYY-MM-DD)")

	flag.Parse()
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
				log.Info("Done (in %s)", time.Now().Sub(s).String())
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

	// Fetch the pcap dump for that date and iterate through its messages.
	resp, err := http.Get(histData[0].Link)
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
	)

	for {
		msg, err := scanner.NextMessage()
		if err != nil {
			if err == io.EOF {
				break
			}

			log.Fatal(err.Error())
		}

		if msg, ok := msg.(*tops.TradeReportMessage); ok {
			if openTime.IsZero() {
				openTime = msg.Timestamp.Truncate(time.Minute)
				closeTime = openTime.Add(time.Minute)
			}

			if msg.Timestamp.After(closeTime) && len(trades) > 0 {
				bars := makeBars(trades, openTime, closeTime)

				if err := writeBars(bars); err != nil {
					log.Fatal(err.Error())
				}

				trades = trades[:0]
				openTime = msg.Timestamp.Truncate(time.Minute)
				closeTime = openTime.Add(time.Minute)
			}

			trades = append(trades, msg)
		}
	}
}

func makeBars(trades []*tops.TradeReportMessage, openTime, closeTime time.Time) []*consolidator.Bar {
	bars := consolidator.MakeBars(trades)
	for _, bar := range bars {
		bar.OpenTime = openTime
		bar.CloseTime = closeTime
	}

	sort.Slice(bars, func(i, j int) bool {
		return bars[i].Symbol < bars[j].Symbol
	})

	return bars
}

func writeBar(bar *consolidator.Bar, w *csv.Writer) error {
	row := []string{
		bar.Symbol,
		bar.OpenTime.Format(time.RFC3339),
		strconv.FormatFloat(bar.Open, 'f', 4, 64),
		strconv.FormatFloat(bar.High, 'f', 4, 64),
		strconv.FormatFloat(bar.Low, 'f', 4, 64),
		strconv.FormatFloat(bar.Close, 'f', 4, 64),
		strconv.FormatInt(bar.Volume, 10),
	}

	return w.Write(row)
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
