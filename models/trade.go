package models

import (
	"time"

	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/models/enum"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

const (
	tradeSuffix    string = "TRADE"
	tradeTimeframe string = "1Sec"
)

// Trade defines schema and helper functions for storing trade data
type Trade struct {
	Tbk      *io.TimeBucketKey
	csm      io.ColumnSeriesMap
	cs       *io.ColumnSeries
	Epoch    []int64
	Nanos    []int32
	Price    []float64
	Size     []uint64
	Exchange []byte
	TapeID   []byte
	Cond1    []byte
	Cond2    []byte
	Cond3    []byte
	Cond4    []byte

	WriteTime time.Duration
	limit     int
	idx       int
}

// TradeBucketKey returns a string bucket key for a given symbol and timeframe
func TradeBucketKey(symbol string) string {
	return symbol + "/" + tradeTimeframe + "/" + tradeSuffix
}

// NewTrade creates a new Bar object and initializes it's internal column buffers to the given length
func NewTrade(symbol string, length int) *Trade {
	model := &Trade{
		Tbk:   io.NewTimeBucketKey(TradeBucketKey(symbol)),
		limit: length,
	}
	model.make(length)
	return model
}

// Key returns the key of the model's time bucket
func (model Trade) Key() string {
	return model.Tbk.GetItemKey()
}

// Len returns the length of the internal column buffers
func (model *Trade) Len() int {
	return len(model.Epoch)
}

// Symbol returns the Symbol part if the TimeBucketKey of this model
func (model *Trade) Symbol() string {
	return model.Tbk.GetItemInCategory("Symbol")
}

// SetLimit sets a limit on how many entries are actually used when .Write() is called
// It is useful if the model's buffers populated through the exported buffers directly (Open[i], Close[i], etc)
// and the actual amount of inserted data is less than the initailly specified length parameter.
func (model *Trade) SetLimit(limit int) {
	model.limit = limit
}

// make allocates buffers for this model.
func (model *Trade) make(length int) {
	model.Epoch = make([]int64, length)
	model.Nanos = make([]int32, length)
	model.Price = make([]float64, length)
	model.Size = make([]uint64, length)
	model.Exchange = make([]byte, length)
	model.TapeID = make([]byte, length)
	model.Cond1 = make([]byte, length)
	model.Cond2 = make([]byte, length)
	model.Cond3 = make([]byte, length)
	model.Cond4 = make([]byte, length)
}

// Add adds a new data point to the internal buffers, and increment the internal index by one
func (model *Trade) Add(epoch int64, nanos int, price float64, size int, exchange enum.Exchange, tapeid enum.Tape, conditions ...enum.TradeCondition) {
	idx := model.idx
	model.Epoch[idx] = epoch
	model.Nanos[idx] = int32(nanos)
	model.Price[idx] = price
	model.Size[idx] = uint64(size)
	model.Exchange[idx] = byte(exchange)
	model.TapeID[idx] = byte(tapeid)
	switch len(conditions) {
	case 4:
		model.Cond4[idx] = byte(conditions[3])
		fallthrough
	case 3:
		model.Cond3[idx] = byte(conditions[2])
		fallthrough
	case 2:
		model.Cond2[idx] = byte(conditions[1])
		fallthrough
	case 1:
		model.Cond1[idx] = byte(conditions[0])
	}
	model.idx++
}

// BuildCsm prepares an io.ColumnSeriesMap object and populates it's columns with the contents of the internal buffers
// it is included in the .Write() method so use only when you need to work with the ColumnSeriesMap before writing it to disk
func (model *Trade) buildCsm() *io.ColumnSeriesMap {
	if model.idx > 0 {
		model.limit = model.idx
	}
	limit := model.limit
	csm := io.NewColumnSeriesMap()
	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", model.Epoch[:limit])
	cs.AddColumn("Nanoseconds", model.Nanos[:limit])
	cs.AddColumn("Price", model.Price[:limit])
	cs.AddColumn("Size", model.Size[:limit])
	cs.AddColumn("Exchange", model.Exchange[:limit])
	cs.AddColumn("TapeID", model.TapeID[:limit])
	cs.AddColumn("Cond1", model.Cond1[:limit])
	cs.AddColumn("Cond2", model.Cond2[:limit])
	cs.AddColumn("Cond3", model.Cond3[:limit])
	cs.AddColumn("Cond4", model.Cond4[:limit])
	csm.AddColumnSeries(*model.Tbk, cs)
	return &csm
}

// Write persist the internal buffers to disk.
func (model *Trade) Write() error {
	start := time.Now()
	csm := model.buildCsm()
	err := executor.WriteCSM(*csm, true)
	model.WriteTime = time.Since(start)
	if err != nil {
		log.Error("Failed to write trades for %s (%+v)", model.Key(), err)
	} else {
		log.Debug("Wrote %d trades to %s", model.limit, model.Key())
	}
	return err
}
