package handlers_test

import (
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"testing"
	"time"

	"github.com/alpacahq/marketstore/v4/contrib/polygon/handlers"
	"github.com/alpacahq/marketstore/v4/utils/test"

	"github.com/alpacahq/marketstore/v4/executor"

	"github.com/alpacahq/marketstore/v4/contrib/polygon/api"
)

func setup(t *testing.T, testName string,
) (tearDown func()) {
	t.Helper()

	rootDir, _ := ioutil.TempDir("", fmt.Sprintf("handlers_test-%s", testName))
	_, _, _, err := executor.NewInstanceSetup(rootDir, nil, nil, 5, true, true, false, true)
	assert.Nil(t, err)

	return func() { test.CleanupDummyDataDir(rootDir) }
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
	tearDown := setup(t, "TestHandlers")
	defer tearDown()

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
