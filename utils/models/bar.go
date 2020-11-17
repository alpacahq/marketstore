package models

import (
	"time"

	"github.com/alpacahq/marketstore/v4/contrib/polygon/worker"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

const BarSuffix = "OHLCV"

type Bar struct {
	Tbk                    *io.TimeBucketKey
	Csm                    io.ColumnSeriesMap
	Epoch                  []int64
	Open, High, Low, Close []float64
	Volume                 []uint64
	WriteTime              time.Duration
	limit                  int
	idx                    int
}

func NewBar(symbol, timeframe string, length int) *Bar {
	model := &Bar{
		Tbk:   io.NewTimeBucketKey(symbol + "/" + timeframe + "/" + BarSuffix),
		Csm:   io.NewColumnSeriesMap(),
		limit: 0,
	}
	model.Make(length)
	return model
}

func (model Bar) Key() *io.TimeBucketKey {
	return model.Tbk
}

func (model *Bar) Len() int {
	return len(model.Epoch)
}

func (model *Bar) Symbol() string {
	return model.Tbk.GetItemInCategory("Symbol")
}

func (model *Bar) SetLimit(limit int) {
	model.limit = limit
}

func (model *Bar) Make(length int) {
	model.Epoch = make([]int64, length)
	model.Open = make([]float64, length)
	model.High = make([]float64, length)
	model.Low = make([]float64, length)
	model.Close = make([]float64, length)
	model.Volume = make([]uint64, length)
}

func (model *Bar) Add(epoch int64, open, high, low, close float64, volume int) {
	idx := model.idx
	model.Epoch[idx] = epoch
	model.Open[idx] = open
	model.High[idx] = high
	model.Low[idx] = low
	model.Close[idx] = close
	model.Volume[idx] = uint64(volume)
	model.idx++
}

func (model *Bar) BuildCsm() *io.ColumnSeriesMap {
	limit := model.limit
	if model.idx > 0 {
		limit = model.idx
	}
	csm := io.NewColumnSeriesMap()
	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", model.Epoch[:limit])
	cs.AddColumn("Open", model.Open[:limit])
	cs.AddColumn("High", model.High[:limit])
	cs.AddColumn("Low", model.Low[:limit])
	cs.AddColumn("Close", model.Close[:limit])
	cs.AddColumn("Volume", model.Volume[:limit])
	csm.AddColumnSeries(*model.Tbk, cs)
	return &csm
}

func (model *Bar) Write() error {
	csm := model.BuildCsm()
	return executor.WriteCSM(*csm, false)
}

func (model *Bar) WriteAsync(workerPool *worker.WorkerPool) {
	workerPool.Do(func() {
		start := time.Now()
		if err := model.Write(); err != nil {
			log.Error("failed to write OHLCV bars for %s", model.Tbk.String())
		}
		model.WriteTime = time.Since(start)
	})
}
