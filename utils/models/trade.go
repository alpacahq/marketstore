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
	TradeSuffix    string = "OHLCV"
	tradeTimeframe string = "1Sec"
)

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

func NewTrade(symbol string, length int) *Trade {
	model := &Trade{
		Tbk:   io.NewTimeBucketKey(symbol + "/" + tradeTimeframe + "/" + TradeSuffix),
		limit: length,
	}
	model.Make(length)
	return model
}

func (model Trade) Key() *io.TimeBucketKey {
	return model.Tbk
}

func (model *Trade) Len() int {
	return len(model.Epoch)
}

func (model *Trade) Symbol() string {
	return model.Tbk.GetItemInCategory("Symbol")
}

func (model *Trade) SetLimit(limit int) {
	model.limit = limit
}

func (model *Trade) Make(length int) {
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

func (model *Trade) buildCsm() *io.ColumnSeriesMap {
	limit := model.limit
	if model.idx > 0 {
		limit = model.idx
	}
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

func (model *Trade) Write() error {
	csm := model.buildCsm()
	return executor.WriteCSM(*csm, true)
}

func (model *Trade) WriteAsync(workerPool *worker.WorkerPool) {
	workerPool.Do(func() {
		start := time.Now()
		if err := model.Write(); err != nil {
			log.Error("failed to write trades for %s", model.Tbk.String())
		}
		model.WriteTime = time.Since(start)
	})
}
