package polygon

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type PolygonTestSuite struct {
	suite.Suite
}

func TestPolygonTestSuite(t *testing.T) {
	suite.Run(t, new(PolygonTestSuite))
}

func (s *PolygonTestSuite) TestPolygon() {
	// get historic aggregates
	{
		// successful
		get = func(u *url.URL) (*http.Response, error) {
			return &http.Response{
				Body: genBody([]byte(aggBody)),
			}, nil
		}

		now := time.Now()
		limit := 1

		resp, err := GetHistoricAggregates("APCA", Minute, &now, &now, &limit)
		assert.Nil(s.T(), err)
		assert.NotNil(s.T(), resp)

		// api failure
		get = func(u *url.URL) (*http.Response, error) {
			return &http.Response{}, fmt.Errorf("fail")
		}

		resp, err = GetHistoricAggregates("APCA", Minute, &now, &now, &limit)
		assert.NotNil(s.T(), err)
		assert.Nil(s.T(), resp)
	}

	// get historic trades
	{
		// successful
		get = func(u *url.URL) (*http.Response, error) {
			return &http.Response{
				Body: genBody([]byte(tradesBody)),
			}, nil
		}

		date := "2018-01-03"

		resp, err := GetHistoricTrades("APCA", date, nil)
		assert.Nil(s.T(), err)
		assert.NotNil(s.T(), resp)

		// api failure
		get = func(u *url.URL) (*http.Response, error) {
			return &http.Response{}, fmt.Errorf("fail")
		}

		resp, err = GetHistoricTrades("APCA", date, nil)
		assert.NotNil(s.T(), err)
		assert.Nil(s.T(), resp)
	}

	// get historic quotes
	{
		// successful
		get = func(u *url.URL) (*http.Response, error) {
			return &http.Response{
				Body: genBody([]byte(quotesBody)),
			}, nil
		}

		date := "2018-01-03"

		resp, err := GetHistoricQuotes("APCA", date)
		assert.Nil(s.T(), err)
		assert.NotNil(s.T(), resp)

		// api failure
		get = func(u *url.URL) (*http.Response, error) {
			return &http.Response{}, fmt.Errorf("fail")
		}

		resp, err = GetHistoricQuotes("APCA", date)
		assert.NotNil(s.T(), err)
		assert.Nil(s.T(), resp)
	}

	// get exchange data
	{
		// successful
		get = func(u *url.URL) (*http.Response, error) {
			return &http.Response{
				Body: genBody([]byte(exchangeBody)),
			}, nil
		}

		resp, err := GetStockExchanges()
		assert.Nil(s.T(), err)
		assert.NotNil(s.T(), resp)

		// api failure
		get = func(u *url.URL) (*http.Response, error) {
			return &http.Response{}, fmt.Errorf("fail")
		}

		resp, err = GetStockExchanges()
		assert.NotNil(s.T(), err)
		assert.Nil(s.T(), resp)
	}
}

type nopCloser struct {
	io.Reader
}

func (nopCloser) Close() error { return nil }

func genBody(buf []byte) io.ReadCloser {
	return nopCloser{bytes.NewBuffer(buf)}
}

const (
	aggBody = `{
		"symbol": "APCA",
		"aggType": "min",
		"map": {
		  "o": "open",
		  "c": "close",
		  "h": "high",
		  "l": "low",
		  "v": "volume",
		  "t": "timestamp"
		},
		"ticks": [
		  {
			"o": 47.53,
			"c": 47.53,
			"h": 47.53,
			"l": 47.53,
			"v": 16100,
			"t": 1199278800000
		  }
		]
	  }`
	quotesBody = `{
		"day": "2018-01-03",
		"map": {
		  "aE": "askexchange",
		  "aP": "askprice",
		  "aS": "asksize",
		  "bE": "bidexchange",
		  "bP": "bidprice",
		  "bS": "bidsize",
		  "c": "condition",
		  "t": "timestamp"
		},
		"msLatency": 7,
		"status": "success",
		"symbol": "APCA",
		"ticks": [
		  {
			"c": 0,
			"bE": "8",
			"aE": "11",
			"bP": 98.79,
			"aP": 98.89,
			"bS": 5,
			"aS": 1,
			"t": 1514938489451
		  }
		],
		"type": "quotes"
	  }`
	tradesBody = `{
		"day": "2018-01-03",
		"map": {
			"c1": "condition1",
			"c2": "condition2",
			"c3": "condition3",
			"c4": "condition4",
			"e": "exchange",
			"p": "price",
			"s": "size",
			"t": "timestamp"
		},
		"msLatency": 10,
		"status": "success",
		"symbol": "APCA",
		"ticks": [
			{
			"c1": 37,
			"c2": 12,
			"c3": 14,
			"c4": 0,
			"e": "8",
			"p": 98.82,
			"s": 61,
			"t": 1514938489451
			}
		],
		"type": "trades"
	}`
	exchangeBody = `[
		{
		  "id": 1,
		  "type": "exchange",
		  "market": "equities",
		  "mic": "XASE",
		  "name": "NYSE American (AMEX)",
		  "tape": "A"
		},
		{
		  "id": 2,
		  "type": "exchange",
		  "market": "equities",
		  "mic": "XBOS",
		  "name": "NASDAQ OMX BX",
		  "tape": "B"
		},
		{
		  "id": 15,
		  "type": "exchange",
		  "market": "equities",
		  "mic": "IEXG",
		  "name": "IEX",
		  "tape": "V"
		},
		{
		  "id": 16,
		  "type": "TRF",
		  "market": "equities",
		  "mic": "XCBO",
		  "name": "Chicago Board Options Exchange",
		  "tape": "W"
		}
	  ]`
)
