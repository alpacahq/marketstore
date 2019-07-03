package handlers

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/alpacahq/marketstore/catalog"

	"github.com/alpacahq/marketstore/executor"

	"github.com/alpacahq/marketstore/contrib/polygon/api"
	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

var _ = Suite(&HandlersTestSuite{})

type HandlersTestSuite struct {
	DataDirectory *catalog.Directory
	Rootdir       string
	WALFile       *executor.WALFileType
}

func (s *HandlersTestSuite) SetUpSuite(c *C) {
	s.Rootdir = c.MkDir()
	executor.NewInstanceSetup(s.Rootdir, true, true, false, true) // WAL Bypass
	s.DataDirectory = executor.ThisInstance.CatalogDir
	s.WALFile = executor.ThisInstance.WALFile
}

func (s *HandlersTestSuite) TearDownSuite(c *C) {}

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
func (s *HandlersTestSuite) TestHandlers(c *C) {
	// trade
	{
		buf, _ := json.Marshal(getTestTradeArray())
		TradeHandler(buf)

		a := getTestTradeArray()
		a[0].Conditions = []int{ConditionExchangeSummary}
		buf, _ = json.Marshal(a)
		TradeHandler(buf)
	}
	// quote
	{
		buf, _ := json.Marshal(getTestQuoteArray())
		QuoteHandler(buf)
	}
}
