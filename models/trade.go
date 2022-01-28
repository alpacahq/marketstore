package models

import (
	"time"

	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/models/enum"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

const (
	TradeSuffix    string = "TRADE"
	TradeTimeframe string = "1Sec"
)

// Trade defines schema and helper functions for storing trade data.
type Trade struct {
	Tbk      *io.TimeBucketKey
	csm      io.ColumnSeriesMap
	cs       *io.ColumnSeries
	Epoch    []int64
	Nanos    []int32
	Price    []enum.Price
	Size     []enum.Size
	Exchange []enum.Exchange
	TapeID   []enum.Tape
	Cond1    []enum.TradeCondition
	Cond2    []enum.TradeCondition
	Cond3    []enum.TradeCondition
	Cond4    []enum.TradeCondition

	WriteTime time.Duration
}

// TradeBucketKey returns a string bucket key for a given symbol and timeframe.
func TradeBucketKey(symbol string) string {
	return symbol + "/" + TradeTimeframe + "/" + TradeSuffix
}

// NewTrade creates a new Trade object and initializes it's internal column buffers to the given capacity.
func NewTrade(symbol string, capacity int) *Trade {
	model := &Trade{
		Tbk: io.NewTimeBucketKey(TradeBucketKey(symbol)),
	}
	model.make(capacity)
	return model
}

// Key returns the key of the model's time bucket.
func (model Trade) Key() string {
	return model.Tbk.GetItemKey()
}

// Len returns the length of the internal column buffers.
func (model *Trade) Len() int {
	return len(model.Epoch)
}

// Symbol returns the Symbol part if the TimeBucketKey of this model.
func (model *Trade) Symbol() string {
	return model.Tbk.GetItemInCategory("Symbol")
}

// make allocates buffers for this model.
func (model *Trade) make(capacity int) {
	model.Epoch = make([]int64, 0, capacity)
	model.Nanos = make([]int32, 0, capacity)
	model.Price = make([]enum.Price, 0, capacity)
	model.Size = make([]enum.Size, 0, capacity)
	model.Exchange = make([]enum.Exchange, 0, capacity)
	model.TapeID = make([]enum.Tape, 0, capacity)
	model.Cond1 = make([]enum.TradeCondition, 0, capacity)
	model.Cond2 = make([]enum.TradeCondition, 0, capacity)
	model.Cond3 = make([]enum.TradeCondition, 0, capacity)
	model.Cond4 = make([]enum.TradeCondition, 0, capacity)
}

// Add adds a new data point to the internal buffers, and increment the internal index by one.
func (model *Trade) Add(epoch int64, nanos int, price enum.Price, size enum.Size,
	exchange enum.Exchange, tapeid enum.Tape, conditions ...enum.TradeCondition,
) {
	model.Epoch = append(model.Epoch, epoch)
	model.Nanos = append(model.Nanos, int32(nanos))
	model.Price = append(model.Price, price)
	model.Size = append(model.Size, size)
	model.Exchange = append(model.Exchange, exchange)
	model.TapeID = append(model.TapeID, tapeid)

	cond1 := enum.NoTradeCondition
	cond2 := enum.NoTradeCondition
	cond3 := enum.NoTradeCondition
	cond4 := enum.NoTradeCondition

	switch len(conditions) {
	// nolint:gomnd
	case 4:
		cond4 = conditions[3]
		fallthrough
	// nolint:gomnd
	case 3:
		cond3 = conditions[2]
		fallthrough
	// nolint:gomnd
	case 2:
		cond2 = conditions[1]
		fallthrough
	
	case 1:
		cond1 = conditions[0]
	case 0:
		break
	default:
		log.Error("invalid length of conditions: %v", len(conditions))
	}

	model.Cond4 = append(model.Cond4, cond4)
	model.Cond3 = append(model.Cond3, cond3)
	model.Cond2 = append(model.Cond2, cond2)
	model.Cond1 = append(model.Cond1, cond1)
}

func (model *Trade) GetCs() *io.ColumnSeries {
	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", model.Epoch)
	cs.AddColumn("Nanoseconds", model.Nanos)
	cs.AddColumn("Price", model.Price)
	cs.AddColumn("Size", model.Size)
	cs.AddColumn("Exchange", model.Exchange)
	cs.AddColumn("TapeID", model.TapeID)
	cs.AddColumn("Cond1", model.Cond1)
	cs.AddColumn("Cond2", model.Cond2)
	cs.AddColumn("Cond3", model.Cond3)
	cs.AddColumn("Cond4", model.Cond4)

	return cs
}

// BuildCsm prepares an io.ColumnSeriesMap object and populates it's columns with the contents of the internal buffers
// it is included in the .Write() method
// so use only when you need to work with the ColumnSeriesMap before writing it to disk.
func (model *Trade) buildCsm() *io.ColumnSeriesMap {
	csm := io.NewColumnSeriesMap()
	csm.AddColumnSeries(*model.Tbk, model.GetCs())
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
		return err
	}

	return nil
}
