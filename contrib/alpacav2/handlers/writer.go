package handlers

import (
	"github.com/alpacahq/marketstore/v4/contrib/alpacav2/api"
	"github.com/alpacahq/marketstore/v4/models"
	"github.com/alpacahq/marketstore/v4/models/enum"
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
	model := models.NewBar(agg.Symbol, "1Min", 1)

	// add record
	model.Add(agg.Timestamp.Unix(),
		enum.Price(agg.Open), enum.Price(agg.High), enum.Price(agg.Low), enum.Price(agg.Close), enum.Size(agg.Volume))

	// save
	if err := model.Write(); err != nil {
		log.Error("[alpacav2] write failure for key: [%v] (%v)", model.Key(), err)
	}
}
