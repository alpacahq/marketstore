package handlers

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/alpacahq/gopaca/log"
	"github.com/alpacahq/polycache/enum"

	"github.com/alpacahq/polycache/utils"

	"github.com/alpacahq/polycache/raft"
	"github.com/alpacahq/polycache/structures"
)

const (
	ConditionExchangeSummary = 51
	OfficialConditionClosing = 15
	OfficialConditionOpening = 16
	ConditionClosing         = 17
	ConditionReOpening       = 18
	ConditionOpening         = 19
)

// TradeHandler handles a Polygon WS trade
// message and stores it to the cache
func TradeHandler(msg []byte) {
	if msg == nil {
		return
	}
	tt := make([]structures.PolyTrade, 0)
	err := json.Unmarshal(msg, &tt)
	if err != nil {
		log.Warn("error processing upstream message",
			"message", string(msg),
			"error", err.Error())
		return
	}

	for _, rt := range tt {
		t := structures.Trade{}
		timestamp := rt.T
		// parse timestamp - do this first to get the best lag measure
		t.Timestamp = time.Unix(0, int64(1000000*float64(timestamp)))
		lagOnReceipt := time.Now().Sub(t.Timestamp).Seconds()
		var (
			isOpen, isReOpen, isClose bool
			isOffOpen, isOffClose     bool
		)
		for _, c := range rt.C {
			switch c {
			case ConditionExchangeSummary:
				return
			case ConditionReOpening:
				isReOpen = true
			case ConditionOpening:
				isOpen = true
			case ConditionClosing:
				isClose = true
			case OfficialConditionOpening:
				isOffOpen = true
			case OfficialConditionClosing:
				isOffClose = true
			}
		}

		t.Size = int64(rt.S)
		t.Price = rt.P
		// skip empty trades
		if t.Size <= 0 || t.Price <= 0 {
			return
		}

		symbol := rt.Sym
		var format string
		switch {
		case isOpen:
			format = string(enum.OpeningPrice) + "%s"
		case isReOpen:
			format = string(enum.ReOpeningPrice) + "%s"
		case isClose:
			format = string(enum.ClosingPrice) + "%s"
		case isOffOpen:
			format = string(enum.OfficialOpeningPrice) + "%s"
		case isOffClose:
			format = string(enum.OfficialClosingPrice) + "%s"
		default:
			format = string(enum.Trade) + "%s"
		}
		symbol = fmt.Sprintf(format, strings.Replace(symbol, "/", ".", 1))

		if !isMoreRecent(symbol, t.Timestamp) {
			return
		}

		utils.TradeLagStats.Update(lagOnReceipt, symbol)

		// throw the trade on the queue
		raft.GetCache().Queue(symbol, t)
	}
}

// QuoteHandler handles a Polygon WS quote
// message and stores it to the cache
func QuoteHandler(msg []byte) {
	if msg == nil {
		return
	}
	qq := make([]structures.PolyQuote, 0)
	err := json.Unmarshal(msg, &qq)
	if err != nil {
		log.Warn("error processing upstream message",
			"message", string(msg),
			"error", err.Error())
		return
	}
	for _, rq := range qq {
		q := structures.Quote{}
		timestamp := rq.T
		q.Timestamp = time.Unix(0, int64(1000000*float64(timestamp)))
		lagOnReceipt := time.Now().Sub(q.Timestamp).Seconds()
		symbol := rq.Sym
		format := string(enum.Quote) + "%s"
		symbol = fmt.Sprintf(format, strings.Replace(symbol, "/", ".", 1))

		if !isMoreRecent(symbol, q.Timestamp) {
			return
		}

		// parse the quote fields
		q.BidPrice = rq.Bp
		q.AskPrice = rq.Ap
		q.BidSize = int64(rq.Bs)
		q.AskSize = int64(rq.As)

		utils.QuoteLagStats.Update(lagOnReceipt, symbol)

		// throw the quote on the queue
		raft.GetCache().Queue(symbol, q)
	}
}

func isMoreRecent(symbol string, t time.Time) bool {
	// if we have existing trade for this symbol, compare the incoming timestamp to see if this is newer
	ttI := raft.GetCache().GetFromQueue(symbol)
	var ts time.Time
	switch val := ttI.(type) {
	case structures.Quote:
		ts = val.Timestamp
	case structures.Trade:
		ts = val.Timestamp
	}
	if ts.IsZero() {
		return true
	} else {
		if ts.After(t) {
			return false
		}
	}
	return true
}
