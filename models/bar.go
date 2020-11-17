package models

import (
	"time"

	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

const barSuffix = "OHLCV"

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

func BarBucketKey(symbol, timeframe string) string {
	return symbol + "/" + timeframe + "/" + barSuffix
}

func NewBar(symbol, timeframe string, length int) *Bar {
	model := &Bar{
		Tbk:   io.NewTimeBucketKey(BarBucketKey(symbol, timeframe)),
		Csm:   io.NewColumnSeriesMap(),
		limit: 0,
	}
	model.Make(length)
	return model
}

func (model Bar) Key() string {
	return model.Tbk.GetItemKey()
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
	if model.idx > 0 {
		model.limit = model.idx
	}
	limit := model.limit
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
	start := time.Now()
	csm := model.BuildCsm()
	err := executor.WriteCSM(*csm, false)
	model.WriteTime = time.Since(start)
	if err != nil {
		log.Error("Failed to write bars for %s (%+v)", model.Key(), err)
	} else {
		log.Debug("Wrote %d bars to %s", model.limit, model.Key())
	}
	return err
}
