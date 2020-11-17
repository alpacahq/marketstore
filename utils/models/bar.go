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
	csm                    io.ColumnSeriesMap
	cs                     *io.ColumnSeries
	Epoch                  []int64
	Open, High, Low, Close []float64
	Volume                 []int64
	WriteTime              time.Duration
	limit                  int
	idx                    int
}

func NewBar(symbol, timeframe string, length int) *Bar {
	model := &Bar{
		Tbk:   io.NewTimeBucketKey(symbol + "/" + timeframe + "/" + BarSuffix),
		limit: length,
	}
	model.Make(length)
	return model
}

func (model *Bar) Make(length int) {
	model.Epoch = make([]int64, length)
	model.Open = make([]float64, length)
	model.High = make([]float64, length)
	model.Low = make([]float64, length)
	model.Close = make([]float64, length)
	model.Volume = make([]int64, length)
}

func (model *Bar) Append(epoch int64, open, high, low, close float64, volume int64) {
	model.Epoch = append(model.Epoch, epoch)
	model.Open = append(model.Open, open)
	model.High = append(model.High, high)
	model.Low = append(model.Low, low)
	model.Close = append(model.Close, close)
	model.Volume = append(model.Volume, volume)
}

func (model *Bar) Add(epoch int64, open, high, low, close float64, volume int64) {
	idx := model.idx
	model.Set(idx, epoch, open, high, low, close, volume)
	model.idx++
}

func (model *Bar) Set(i int, epoch int64, open, high, low, close float64, volume int64) {
	model.Epoch[i] = epoch
	model.Open[i] = open
	model.High[i] = high
	model.Low[i] = low
	model.Close[i] = close
	model.Volume[i] = volume
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

func (model *Bar) buildCsm() *io.ColumnSeriesMap {
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
	csm := model.buildCsm()
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
