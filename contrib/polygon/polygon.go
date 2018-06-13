package main

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/alpacahq/marketstore/contrib/polygon/api"
	"github.com/alpacahq/marketstore/executor"
	"github.com/alpacahq/marketstore/planner"
	"github.com/alpacahq/marketstore/plugins/bgworker"
	"github.com/alpacahq/marketstore/utils/io"
	"github.com/buger/jsonparser"
	"github.com/golang/glog"
	nats "github.com/nats-io/go-nats"
)

type PolygonFetcher struct {
	config    FetcherConfig
	backfillM *sync.Map
}

type FetcherConfig struct {
	// polygon API key for authenticating with their APIs
	APIKey string `json:"api_key"`
	// polygon API base URL in case it is being proxied
	// (defaults to https://api.polygon.io/)
	BaseURL string `json:"base_url"`
	// list of symbols that are important
	Symbols []string `json:"symbols"`
}

// NewBgWorker returns a new instances of PolygonFetcher. See FetcherConfig
// for more details about configuring PolygonFetcher.
func NewBgWorker(conf map[string]interface{}) (bgworker.BgWorker, error) {
	data, _ := json.Marshal(conf)
	config := FetcherConfig{}
	json.Unmarshal(data, &config)

	return &PolygonFetcher{
		backfillM: &sync.Map{},
		config:    config,
	}, nil
}

// Run the PolygonFetcher. It starts the streaming API as well as the
// asynchronous backfilling routine.
func (pf *PolygonFetcher) Run() {
	api.SetAPIKey(pf.config.APIKey)

	go pf.workBackfill()

	if err := api.Stream(pf.streamHandler, pf.config.Symbols); err != nil {
		glog.Fatalf("nats streaming error (%v)", err)
	}

	select {}
}

func (pf *PolygonFetcher) streamHandler(msg *nats.Msg) {
	// quickly parse the json
	symbol, _ := jsonparser.GetString(msg.Data, "sym")

	open, _ := jsonparser.GetFloat(msg.Data, "o")
	high, _ := jsonparser.GetFloat(msg.Data, "h")
	low, _ := jsonparser.GetFloat(msg.Data, "l")
	close, _ := jsonparser.GetFloat(msg.Data, "c")
	volume, _ := jsonparser.GetInt(msg.Data, "v")
	epochMillis, _ := jsonparser.GetInt(msg.Data, "s")

	epoch := epochMillis / 1000

	pf.backfillM.LoadOrStore(symbol, &epoch)

	tbk := io.NewTimeBucketKeyFromString(fmt.Sprintf("%s/1Min/OHLCV", symbol))
	csm := io.NewColumnSeriesMap()

	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", []int64{epoch})
	cs.AddColumn("Open", []float32{float32(open)})
	cs.AddColumn("High", []float32{float32(high)})
	cs.AddColumn("Low", []float32{float32(low)})
	cs.AddColumn("Close", []float32{float32(close)})
	cs.AddColumn("Volume", []int32{int32(volume)})
	csm.AddColumnSeries(*tbk, cs)

	if err := executor.WriteCSM(csm, false); err != nil {
		glog.Errorf("csm write failed (%v)", err)
		return
	}
}

func (pf *PolygonFetcher) workBackfill() {
	ticker := time.NewTicker(30 * time.Second)

	for range ticker.C {
		// range over symbols that need backfilling, and
		// backfill them from the last written record
		pf.backfillM.Range(func(key, value interface{}) bool {
			symbol := key.(string)

			// make sure epoch value isn't nil (i.e. hasn't
			// been backfilled already)
			if value != nil {
				backfill(symbol, *value.(*int64))
				pf.backfillM.Store(key, nil)
			}

			return true
		})
	}
}

func backfill(symbol string, endEpoch int64) {
	var csm io.ColumnSeriesMap
	tbk := io.NewTimeBucketKey(fmt.Sprintf("%s/1Min/OHLCV", symbol))

	// query the latest entry prior to the streamed record
	{
		instance := executor.ThisInstance
		cDir := instance.CatalogDir
		q := planner.NewQuery(cDir)
		q.AddTargetKey(tbk)
		q.SetRowLimit(io.LAST, 1)
		q.SetEnd(endEpoch - int64(time.Minute.Seconds()))

		parsed, err := q.Parse()
		if err != nil {
			glog.Errorf("query parse error (%v)", err)
			return
		}

		scanner, err := executor.NewReader(parsed)
		if err != nil {
			glog.Errorf("new scanner error (%v)", err)
			return
		}

		csm, _, err = scanner.Read()
		if err != nil {
			glog.Errorf("scanner read error (%v)", err)
			return
		}
	}

	epoch := csm[*tbk].GetEpoch()

	// no gap to fill
	if len(epoch) == 0 {
		return
	}

	// request & write the missing bars
	{
		resp, err := api.GetAggregates(symbol, time.Unix(epoch[len(epoch)-1], 0))

		if err != nil {
			glog.Errorf("failed to backfill aggregates (%v)", err)
			return
		}

		csm = io.NewColumnSeriesMap()

		epoch = make([]int64, len(resp.Ticks))
		open := make([]float32, len(resp.Ticks))
		high := make([]float32, len(resp.Ticks))
		low := make([]float32, len(resp.Ticks))
		close := make([]float32, len(resp.Ticks))
		volume := make([]int32, len(resp.Ticks))

		for i, bar := range resp.Ticks {
			epoch[i] = bar.EpochMillis / 1000
			open[i] = float32(bar.Open)
			high[i] = float32(bar.High)
			low[i] = float32(bar.Low)
			close[i] = float32(bar.Close)
			volume[i] = int32(bar.Volume)
		}

		cs := io.NewColumnSeries()
		cs.AddColumn("Epoch", epoch)
		cs.AddColumn("Open", open)
		cs.AddColumn("High", high)
		cs.AddColumn("Low", low)
		cs.AddColumn("Close", close)
		cs.AddColumn("Volume", volume)
		csm.AddColumnSeries(*tbk, cs)

		if err := executor.WriteCSM(csm, false); err != nil {
			glog.Errorf("csm write failed (%v)", err)
			return
		}
	}
}

func main() {}
