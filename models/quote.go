package models

import (
	"time"

	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/models/enum"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

const (
	quoteSuffix    string = "QUOTE"
	quoteTimeframe string = "1Sec"
)

// Quote defines schema and helper functions for storing Ask-Bid quote data.
type Quote struct {
	Tbk         *io.TimeBucketKey
	csm         io.ColumnSeriesMap
	cs          *io.ColumnSeries
	Epoch       []int64
	Nanos       []int32
	BidPrice    []float64
	AskPrice    []float64
	BidSize     []uint64
	AskSize     []uint64
	BidExchange []byte
	AskExchange []byte
	Cond        []byte

	WriteTime time.Duration
	limit     int
	idx       int
}

// BarBucketKey returns a string bucket key for a given symbol and timeframe.
func QuoteBucketKey(symbol string) string {
	return symbol + "/" + quoteTimeframe + "/" + quoteSuffix
}

// NewBar creates a new Bar object and initializes it's internal column buffers to the given length.
func NewQuote(symbol string, length int) *Quote {
	model := &Quote{
		Tbk:   io.NewTimeBucketKey(QuoteBucketKey(symbol)),
		limit: length,
	}
	model.make(length)
	return model
}

// Key returns the key of the model's time bucket.
func (model Quote) Key() string {
	return model.Tbk.GetItemKey()
}

// Len returns the length of the internal column buffers.
func (model *Quote) Len() int {
	return len(model.Epoch)
}

// Symbol returns the Symbol part if the TimeBucketKey of this model.
func (model *Quote) Symbol() string {
	return model.Tbk.GetItemInCategory("Symbol")
}

// SetLimit sets a limit on how many entries are actually used when .Write() is called
// It is useful if the model's buffers populated through the exported buffers directly (Open[i], Close[i], etc)
// and the actual amount of inserted data is less than the initially specified length parameter.
func (model *Quote) SetLimit(limit int) {
	model.limit = limit
}

// make allocates buffers for this model.
func (model *Quote) make(length int) {
	model.Epoch = make([]int64, length)
	model.Nanos = make([]int32, length)
	model.BidPrice = make([]float64, length)
	model.AskPrice = make([]float64, length)
	model.BidSize = make([]uint64, length)
	model.AskSize = make([]uint64, length)
	model.BidExchange = make([]byte, length)
	model.AskExchange = make([]byte, length)
	model.Cond = make([]byte, length)
}

// Add adds a new data point to the internal buffers, and increment the internal index by one.
func (model *Quote) Add(epoch int64, nanos int, bidPrice, askPrice float64, bidSize, askSize int, bidExchange, askExchange enum.Exchange, cond enum.QuoteCondition) {
	idx := model.idx
	model.Epoch[idx] = epoch
	model.Nanos[idx] = int32(nanos)
	model.BidPrice[idx] = bidPrice
	model.AskPrice[idx] = askPrice
	model.BidSize[idx] = uint64(bidSize)
	model.AskSize[idx] = uint64(askSize)
	model.BidExchange[idx] = byte(bidExchange)
	model.AskExchange[idx] = byte(askExchange)
	model.Cond[idx] = byte(cond)
	model.idx++
}

// BuildCsm prepares an io.ColumnSeriesMap object and populates it's columns with the contents of the internal buffers
// it is included in the .Write() method so use only when you need to work with the ColumnSeriesMap before writing it to disk.
func (model *Quote) BuildCsm() *io.ColumnSeriesMap {
	if model.idx > 0 {
		model.limit = model.idx
	}
	limit := model.limit
	csm := io.NewColumnSeriesMap()
	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", model.Epoch[:limit])
	cs.AddColumn("Nanoseconds", model.Nanos[:limit])
	cs.AddColumn("AskPrice", model.AskPrice[:limit])
	cs.AddColumn("BidPrice", model.BidPrice[:limit])
	cs.AddColumn("AskSize", model.AskSize[:limit])
	cs.AddColumn("BidSize", model.BidSize[:limit])
	cs.AddColumn("BidExchange", model.BidExchange[:limit])
	cs.AddColumn("AskExchange", model.AskExchange[:limit])
	cs.AddColumn("Cond", model.Cond[:limit])
	csm.AddColumnSeries(*model.Tbk, cs)
	return &csm
}

// Write persist the internal buffers to disk.
func (model *Quote) Write() error {
	start := time.Now()
	csm := model.BuildCsm()
	err := executor.WriteCSM(*csm, true)
	model.WriteTime = time.Since(start)
	if err != nil {
		log.Error("Failed to write quotes for %s (%+v)", model.Key(), err)
	}
	return err
}
