package models

import (
	"time"

	"github.com/alpacahq/marketstore/v4/utils/models/enum"

	"github.com/alpacahq/marketstore/v4/contrib/polygon/worker"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

const (
	quoteSuffix    string = "QUOTE"
	quoteTimeframe string = "1Sec"
)

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

func QuoteBucketKey(symbol string) string {
	return symbol + "/" + quoteTimeframe + "/" + quoteSuffix
}

func NewQuote(symbol string, length int) *Quote {
	model := &Quote{
		Tbk:   io.NewTimeBucketKey(QuoteBucketKey(symbol)),
		limit: length,
	}
	model.Make(length)
	return model
}

func (model Quote) Key() string {
	return model.Tbk.GetItemKey()
}

func (model *Quote) Len() int {
	return len(model.Epoch)
}

func (model *Quote) Symbol() string {
	return model.Tbk.GetItemInCategory("Symbol")
}

func (model *Quote) SetLimit(limit int) {
	model.limit = limit
}

func (model *Quote) Make(length int) {
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

func (model *Quote) Add(epoch int64, nanos int, bidPrice float64, askPrice float64, bidSize int, askSize int, bidExchange, askExchange enum.Exchange, cond enum.QuoteCondition) {
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

func (model *Quote) buildCsm() *io.ColumnSeriesMap {
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

func (model *Quote) Write() error {
	start := time.Now()
	csm := model.buildCsm()
	err := executor.WriteCSM(*csm, true)
	model.WriteTime = time.Since(start)
	if err != nil {
		log.Error("Failed to write quotes for %s (%+v)", model.Key(), err)
	} else {
		log.Debug("Wrote %d quotes to %s", model.limit, model.Key())
	}
	return err
}

func (model *Quote) WriteAsync(workerPool *worker.WorkerPool) {
	workerPool.Do(func() {
		model.Write()
	})
}
