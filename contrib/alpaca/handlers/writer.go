package handlers

import (
	"strings"
	"time"

	"github.com/alpacahq/marketstore/v4/contrib/alpaca/api"
	polygon "github.com/alpacahq/marketstore/v4/contrib/polygon/api"
	"github.com/alpacahq/marketstore/v4/utils/log"
	"github.com/alpacahq/marketstore/v4/utils/models"
)

// writeTrade writes a Trade
func writeTrade(t *api.Trade) {
	symbol := strings.Replace(t.Symbol, "/", ".", 1)
	model := models.NewTrade(symbol, 1)

	// type conversions
	timestamp := time.Unix(0, t.Timestamp)
	nanosecond := int32(timestamp.Nanosecond())
	exchange := polygon.ConvertExchangeCode(t.Exchange)
	tape := polygon.ConvertTapeCode(t.TapeID)
	conditions := make([]byte, len(t.Conditions))
	for i, cond := range t.Conditions {
		conditions[i] = polygon.ConvertTradeCondition(cond)
	}

	// add record
	model.Add(timestamp.Unix(), nanosecond, t.Price, t.Size, exchange, tape, conditions...)

	// save
	if err := model.Write(); err != nil {
		log.Error("[alpaca] csm write failure for key: [%v] (%v)", model.Key().String(), err)
	}
}

// writeQuote writes a Quote
func writeQuote(q *api.Quote) {
	symbol := strings.Replace(q.Symbol, "/", ".", 1)
	model := models.NewQuote(symbol, 1)

	timestamp := time.Unix(0, q.Timestamp)
	bidExchange := polygon.ConvertExchangeCode(q.BidExchange)
	askExchange := polygon.ConvertExchangeCode(q.AskExchange)
	condition := polygon.ConvertQuoteCondition(byte(q.Conditions[0]))

	model.Add(timestamp.Unix(), int32(timestamp.Nanosecond()), q.BidPrice, q.AskPrice, q.BidSize, q.AskSize, bidExchange, askExchange, condition)
	if err := model.Write(); err != nil {
		log.Error("[alpaca] csm write failure for key: [%v] (%v)", model.Key().String(), err)
	}
}

// writeAggregateToMinute writes an AggregateToMinute
func writeAggregateToMinute(agg *api.AggregateToMinute) {

	model := models.NewBar(agg.Symbol, "1Min", 1)

	model.Add(agg.EpochMillis/1e3, agg.Open, agg.High, agg.Low, agg.Close, agg.Volume)

	if err := model.Write(); err != nil {
		log.Error("[alpaca] csm write failure for key: [%v] (%v)", model.Key().String(), err)
	}
}
