package models

import (
	"time"

	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

const barSuffix = "OHLCV"

// Bar is a data model to persist arrays of Ask-Bid quotes
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

// BarBucketKey returns a string bucket key for a given symbol and timeframe
func BarBucketKey(symbol, timeframe string) string {
	return symbol + "/" + timeframe + "/" + barSuffix
}

// NewBar creates a new Bar object and initializes it's internal column buffers to the given length
func NewBar(symbol, timeframe string, length int) *Bar {
	model := &Bar{
		Tbk:   io.NewTimeBucketKey(BarBucketKey(symbol, timeframe)),
		Csm:   io.NewColumnSeriesMap(),
		limit: 0,
	}
	model.make(length)
	return model
}

// Key returns the key of the model's time bucket
func (model Bar) Key() string {
	return model.Tbk.GetItemKey()
}

// Len returns the length of the internal column buffers
func (model *Bar) Len() int {
	return len(model.Epoch)
}

// Symbol returns the Symbol part if the TimeBucketKey of this model
func (model *Bar) Symbol() string {
	return model.Tbk.GetItemInCategory("Symbol")
}

// SetLimit sets a limit on how many entries are actually used when .Write() is called
// It is useful if the model's buffers populated through the exported buffers directly (Open[i], Close[i], etc)
// and the actual amount of inserted data is less than the initailly specified length parameter.
func (model *Bar) SetLimit(limit int) {
	model.limit = limit
}

// make allocates buffers for this model.
func (model *Bar) make(length int) {
	model.Epoch = make([]int64, length)
	model.Open = make([]float64, length)
	model.High = make([]float64, length)
	model.Low = make([]float64, length)
	model.Close = make([]float64, length)
	model.Volume = make([]uint64, length)
}

// Add adds a new data point to the internal buffers, and increment the internal index by one
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

// BuildCsm prepares an io.ColumnSeriesMap object and populates it's columns with the contents of the internal buffers
// it is included in the .Write() method so use only when you need to work with the ColumnSeriesMap before writing it to disk
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

// Write persist the internal buffers to disk.
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
