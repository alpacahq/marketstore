package consolidator

import (
	"sort"
	"time"

	"github.com/timpalpant/go-iex/iextp/tops"
)

// Bar represents trades aggregated over a time interval.
type Bar struct {
	Symbol    string
	OpenTime  time.Time
	CloseTime time.Time
	Open      float64
	High      float64
	Low       float64
	Close     float64
	Volume    int64
}

// Construct a Bar for each distinct symbol in the given list
// of trades.
func MakeBars(trades []*tops.TradeReportMessage) []*Bar {
	bySymbol := groupTradesBySymbol(trades)
	result := make([]*Bar, 0, len(bySymbol))
	for _, trades := range bySymbol {
		result = append(result, MakeBar(trades))
	}

	return result
}

// Construct a Bar from the given list of trades.
// NOTE: Assumes all ticks are from the same symbol.
func MakeBar(trades []*tops.TradeReportMessage) *Bar {
	sort.Slice(trades, func(i, j int) bool {
		return trades[i].Timestamp.Before(trades[j].Timestamp)
	})

	bar := &Bar{
		Symbol:   trades[0].Symbol,
		OpenTime: trades[0].Timestamp,
	}

	for _, trade := range trades {
		updateBar(bar, trade)
	}

	return bar
}

func groupTradesBySymbol(trades []*tops.TradeReportMessage) map[string][]*tops.TradeReportMessage {
	bySymbol := make(map[string][]*tops.TradeReportMessage)
	for _, trade := range trades {
		bySymbol[trade.Symbol] = append(bySymbol[trade.Symbol], trade)
	}

	return bySymbol
}

// Update the given bar to incorporate the trade.
// Note this function assumes the security and times are compatible.
func updateBar(bar *Bar, trade *tops.TradeReportMessage) {
	price := trade.Price
	if price > bar.High {
		bar.High = price
	}

	if bar.Low == 0 || price < bar.Low {
		bar.Low = price
	}

	if bar.Open == 0 {
		bar.Open = price
	}

	bar.CloseTime = trade.Timestamp
	bar.Close = price
	bar.Volume += int64(trade.Size)
}
