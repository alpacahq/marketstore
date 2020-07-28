package handlers

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/alpacahq/gopaca/streaming/polygon"
	"github.com/alpacahq/marketstore/v4/catalog"

	"github.com/alpacahq/marketstore/v4/executor"

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

func getTestTrade() polygon.Trade {
	return polygon.Trade{
		Symbol:     "AAPL",
		Price:      100.11,
		Size:       10,
		Timestamp:  time.Now().Unix() * 1000,
		Conditions: []int32{},
	}
}
func getTestQuote() polygon.Quote {
	return polygon.Quote{
		Symbol:    "AAPL",
		BidPrice:  100.11,
		BidSize:   20,
		AskPrice:  100.12,
		AskSize:   10,
		Timestamp: time.Now().Unix() * 1000,
	}
}
func (s *HandlersTestSuite) TestHandlers(c *C) {
	// trade
	{
		buf, _ := json.Marshal(getTestTrade())
		TradeHandler(buf)

		a := getTestTrade()
		a.Conditions = []int32{ConditionExchangeSummary}
		buf, _ = json.Marshal(a)
		TradeHandler(buf)
	}
	// quote
	{
		buf, _ := json.Marshal(getTestQuote())
		QuoteHandler(buf)
	}
}
