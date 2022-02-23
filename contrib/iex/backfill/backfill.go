package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"runtime"
	"sort"
	"strings"
	"time"

	iex "github.com/timpalpant/go-iex"
	"github.com/timpalpant/go-iex/consolidator"
	"github.com/timpalpant/go-iex/iextp/tops"

	"github.com/alpacahq/marketstore/v4/contrib/calendar"
	"github.com/alpacahq/marketstore/v4/contrib/ondiskagg/aggtrigger"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/plugins/trigger"
	"github.com/alpacahq/marketstore/v4/utils"
	utilsio "github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

var (
	dir  string
	from string
	to   string

	// NY timezone.
	NY, _  = time.LoadLocation("America/New_York")
	format = "2006-01-02"
)

// nolint:gochecknoinits // cobra's standard way to initialize flags
func init() {
	flag.StringVar(&dir, "dir", "/project/data", "mktsdb directory to backfill to")
	flag.StringVar(&from, "from", time.Now().Add(-365*24*time.Hour).Format(format), "backfill from date (YYYY-MM-DD)")
	flag.StringVar(&to, "to", time.Now().Format(format), "backfill from date (YYYY-MM-DD)")

	flag.Parse()
}

func main() {
	err := initWriter()
	if err != nil {
		log.Error(err.Error())
		os.Exit(1)
	}

	start, err := time.Parse(format, from)
	if err != nil {
		log.Error(err.Error())
		os.Exit(1)
	}

	end, err := time.Parse(format, to)
	if err != nil {
		log.Error(err.Error())
		os.Exit(1)
	}

	log.Info("backfilling from %v to %v", start.Format(format), end.Format(format))

	sem := make(chan struct{}, runtime.NumCPU())
	log.Info("Using %d threads", runtime.NumCPU())

	for end.After(start) {
		if calendar.Nasdaq.IsMarketDay(end) {
			sem <- struct{}{}
			go func(t time.Time) {
				defer func() { <-sem }()
				log.Info("backfilling %v...", t.Format("2006-01-02"))
				s := time.Now()
				err := pullDate(t)
				if err != nil {
					log.Error(fmt.Sprintf("failed to pullDate(%v). err=%v", t, err))
					return
				}
				log.Info("Done %v (in %s)", t.Format("2006-01-02"), time.Since(s).String())
			}(end)
		}

		end = end.Add(-24 * time.Hour)
	}

	// make sure all goroutines finish
	for i := 0; i < cap(sem); i++ {
		sem <- struct{}{}
	}
}

func pullDate(t time.Time) error {
	client := iex.NewClient(http.DefaultClient)

	histData, err := client.GetHIST(t)
	if err != nil {
		// no file for this day
		if strings.Contains(err.Error(), "404") {
			log.Warn("404 for date: %v", t)
			return nil
		}
		return fmt.Errorf("failed to getHIST: %w", err)
	} else if len(histData) == 0 {
		panic(fmt.Errorf("found %v available data feeds", len(histData)))
	}

	// Fetch the pcap dump for that date and iterate through its messages.
	log.Info("pcap url: %s", histData[0].Link)
	req, err := http.NewRequestWithContext(context.Background(), "GET", histData[0].Link, nil)
	if err != nil {
		return fmt.Errorf("create http req for %s: %w", histData[0].Link, err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	packetSource, err := iex.NewPacketDataSource(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to create NewPacketDataSource: %w", err)
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
			if errors.Is(err, io.EOF) {
				break
			}

			return fmt.Errorf("failed to scan next message: %w", err)
		}

		if msg, ok := msg.(*tops.TradeReportMessage); ok {
			if openTime.IsZero() {
				openTime = msg.Timestamp.Truncate(time.Minute)
				closeTime = openTime.Add(time.Minute)
			}

			if msg.Timestamp.After(closeTime) && len(trades) > 0 {
				bars := makeBars(trades, openTime, closeTime)

				if err := writeBars(bars); err != nil {
					return fmt.Errorf("failed to writeBars: %w", err)
				}

				trades = trades[:0]
				openTime = msg.Timestamp.Truncate(time.Minute)
				closeTime = openTime.Add(time.Minute)
			}

			trades = append(trades, msg)
		}
	}
	return nil
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

func writeBars(bars []*consolidator.Bar) error {
	csm := utilsio.NewColumnSeriesMap()

	for i := range bars {
		batch, index := nextBatch(bars, i)

		if len(batch) > 0 {
			tbk := utilsio.NewTimeBucketKeyFromString(fmt.Sprintf("%s/1Min/OHLCV", batch[0].Symbol))

			epoch := make([]int64, len(batch))
			open := make([]float32, len(batch))
			high := make([]float32, len(batch))
			low := make([]float32, len(batch))
			clos := make([]float32, len(batch))
			volume := make([]int32, len(batch))

			for j, bar := range batch {
				epoch[j] = bar.OpenTime.Unix()
				open[j] = float32(bar.Open)
				high[j] = float32(bar.High)
				low[j] = float32(bar.Low)
				clos[j] = float32(bar.Close)
				volume[j] = int32(bar.Volume)
			}

			cs := utilsio.NewColumnSeries()
			cs.AddColumn("Epoch", epoch)
			cs.AddColumn("Open", open)
			cs.AddColumn("High", high)
			cs.AddColumn("Low", low)
			cs.AddColumn("Close", clos)
			cs.AddColumn("Volume", volume)
			csm.AddColumnSeries(*tbk, cs)
		}

		if index == len(bars) {
			break
		}
	}

	return executor.WriteCSM(csm, false)
}

func nextBatch(bars []*consolidator.Bar, index int) (batchBars []*consolidator.Bar, idx int) {
	var batch []*consolidator.Bar

	for i, bar := range bars[index:] {
		if i > 0 && !strings.EqualFold(bar.Symbol, bars[i-1].Symbol) {
			return batch, i
		}

		batch = append(batch, bar)
	}

	return batch, len(bars)
}

func initWriter() error {
	utils.InstanceConfig.Timezone = NY
	walRotateInterval := 5
	instanceID := time.Now().UTC().UnixNano()
	relRootDir := fmt.Sprintf("%v/mktsdb", dir)

	config := map[string]interface{}{
		"filter":       "nasdaq",
		"destinations": []string{"5Min", "15Min", "1H"},
	}

	trig, err := aggtrigger.NewTrigger(config)
	if err != nil {
		return fmt.Errorf("failed to create a new aggtrigger: %w", err)
	}

	triggerMatchers := []*trigger.TriggerMatcher{
		trigger.NewMatcher(trig, "*/1Min/OHLCV"),
	}

	_, _, _, err = executor.NewInstanceSetup(
		relRootDir, nil, triggerMatchers,
		walRotateInterval, executor.WALBypass(true))
	if err != nil {
		return fmt.Errorf("failed to create new instance setup for iex/backfill: %w", err)
	}

	log.Info(
		"Initialized writer with InstanceID: %v - relRootDir: %v\n",
		instanceID,
		relRootDir,
	)
	return nil
}
