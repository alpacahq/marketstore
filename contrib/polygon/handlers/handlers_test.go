package handlers

import (
	"encoding/json"
	"io/ioutil"
	"math"
	"os"
	"testing"
	"time"

	"github.com/alpacahq/polycache/raft"
	"github.com/alpacahq/polycache/structures"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type HandlersTestSuite struct {
	suite.Suite
}

func TestHandlersTestSuite(t *testing.T) {
	suite.Run(t, new(HandlersTestSuite))
}

func (s *HandlersTestSuite) TestHandlers() {
	// initialize cache
	tmpDir, _ := ioutil.TempDir("", "store_test")
	defer os.RemoveAll(tmpDir)

	raft.GetCache().RaftBind = "127.0.0.1:0"
	raft.GetCache().RaftDir = tmpDir

	require.NotNil(s.T(), raft.GetCache())
	require.Nil(s.T(), raft.GetCache().Open(true, "node0"))

	// wait to acquire leader
	for {
		if raft.GetCache().Leader() {
			break
		}
		time.Sleep(time.Millisecond)
	}

	// trade
	{
		buf, _ := json.Marshal([]structures.PolyTrade{
			{
				Sym: "AAPL",
				P:   100.11,
				S:   10,
				T:   int(float64(time.Now().UnixNano()) / 1000000),
			},
		})
		TradeHandler(buf)

		<-time.After(time.Second)

		v, err := raft.GetCache().Get("T.AAPL")
		assert.Nil(s.T(), err)
		assert.NotNil(s.T(), v)

		// skip
		buf, _ = json.Marshal([]structures.PolyTrade{
			{
				Sym: "AAPL",
				P:   0,
				S:   0,
				T:   int(float64(time.Now().UnixNano()) / 1000000),
				C:   []int{ConditionExchangeSummary},
			},
		})
		TradeHandler(buf)

		<-time.After(time.Second)

		v, err = raft.GetCache().Get("T.AAPL")
		assert.Nil(s.T(), err)
		assert.NotNil(s.T(), v)
		s.T().Log(v)
	}
	// quote
	{
		buf, _ := json.Marshal([]structures.PolyQuote{
			{
				Sym: "AAPL",
				Bp:  100.11,
				Ap:  100.12,
				Bs:  20,
				As:  10,
				T:   int(float64(time.Now().UnixNano()) / math.Pow10(6)),
			},
		})
		QuoteHandler(buf)

		<-time.After(time.Second)

		v, err := raft.GetCache().Get("Q.AAPL")
		assert.Nil(s.T(), err)
		assert.NotNil(s.T(), v)
	}
}
