package handlers_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/alpacahq/marketstore/v4/contrib/polygon/api"
	"github.com/alpacahq/marketstore/v4/contrib/polygon/handlers"
	"github.com/alpacahq/marketstore/v4/executor"
)

func setup(t *testing.T) {
	t.Helper()

	rootDir := t.TempDir()
	_, _, err := executor.NewInstanceSetup(rootDir, nil, nil, 5,
		executor.BackgroundSync(false), executor.WALBypass(true))
	assert.Nil(t, err)
}

func getTestTradeArray() []api.PolyTrade {
	return []api.PolyTrade{
		{
			Symbol:     "AAPL",
			Price:      100.11,
			Size:       10,
			Timestamp:  time.Now().Unix() * 1000,
			Conditions: []int{},
		},
	}
}

func getTestQuoteArray() []api.PolyQuote {
	return []api.PolyQuote{
		{
			Symbol:    "AAPL",
			BidPrice:  100.11,
			BidSize:   20,
			AskPrice:  100.12,
			AskSize:   10,
			Timestamp: time.Now().Unix() * 1000,
		},
	}
}

func TestHandlers(t *testing.T) {
	setup(t)

	// trade
	{
		buf, _ := json.Marshal(getTestTradeArray())
		handlers.TradeHandler(buf)

		a := getTestTradeArray()
		a[0].Conditions = []int{handlers.ConditionExchangeSummary}
		buf, _ = json.Marshal(a)
		handlers.TradeHandler(buf)
	}
	// quote
	{
		buf, _ := json.Marshal(getTestQuoteArray())
		handlers.QuoteHandler(buf)
	}
}
