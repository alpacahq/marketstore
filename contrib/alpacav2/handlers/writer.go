package handlers

import (
	"fmt"

	"github.com/alpacahq/marketstore/v4/contrib/alpacav2/api"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/models"
	"github.com/alpacahq/marketstore/v4/models/enum"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

// writeTrade writes a Trade
func writeTrade(t *api.Trade) {
	model := models.NewTrade(t.Symbol, 1)

	// type conversions
	if t.Exchange == "" || t.Tape == "" {
		log.Error("[alpacav2] invalid trade %+v", t)
		return
	}
	exchange := enum.Exchange(t.Exchange[0])
	tape := enum.Tape(t.Tape[0])
	conditions := make([]enum.TradeCondition, len(t.Conditions))
	for i, cond := range t.Conditions {
		if cond == "" {
			log.Error("[alpacav2] invalid trade condition %q in trade %+v", cond, t)
			return
		}
		conditions[i] = enum.TradeCondition(cond[0])
	}

	// add record
	model.Add(t.Timestamp.Unix(), t.Timestamp.Nanosecond(),
		enum.Price(t.Price), enum.Size(t.Size), exchange, tape, conditions...)

	// save
	if err := model.Write(); err != nil {
		log.Error("[alpacav2] write failure for key: [%v] (%v)", model.Key(), err)
	}
}

// writeQuote writes a Quote
func writeQuote(q *api.Quote) {
	model := models.NewQuote(q.Symbol, 1)

	// type conversions
	if q.BidExchange == "" || q.AskExchange == "" {
		log.Error("[alpacav2] invalid quote %+v", q)
		return
	}
	bidExchange := enum.Exchange(q.BidExchange[0])
	askExchange := enum.Exchange(q.AskExchange[0])
	if len(q.Conditions) < 1 || q.Conditions[0] == "" {
		log.Error("[alpacav2] invalid quote conditions in quote %+v", q)
		return
	}
	condition := enum.QuoteCondition(q.Conditions[0][0])

	// add record
	model.Add(q.Timestamp.Unix(), q.Timestamp.Nanosecond(), q.BidPrice, q.AskPrice, q.BidSize, q.AskSize, bidExchange, askExchange, condition)

	// save
	if err := model.Write(); err != nil {
		log.Error("[alpacav2] write failure for key: [%v] (%v)", model.Key(), err)
	}
}

// writeAggregateToMinute writes an AggregateToMinute
func writeAggregateToMinute(agg *api.MinuteAggregate) {
	epoch := agg.Timestamp.Unix()

	tbk := io.NewTimeBucketKeyFromString(fmt.Sprintf("%s/1Min/OHLCV", agg.Symbol))
	csm := io.NewColumnSeriesMap()
	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", []int64{epoch})
	cs.AddColumn("Open", []float32{float32(agg.Open)})
	cs.AddColumn("High", []float32{float32(agg.High)})
	cs.AddColumn("Low", []float32{float32(agg.Low)})
	cs.AddColumn("Close", []float32{float32(agg.Close)})
	cs.AddColumn("Volume", []int32{int32(agg.Volume)})
	csm.AddColumnSeries(*tbk, cs)

	if err := executor.WriteCSM(csm, false); err != nil {
		log.Error("[alpacav2] csm write failure for key: [%v] (%v)", tbk.String(), err)
	}
}
