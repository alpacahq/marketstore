package alpaca

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/shopspring/decimal"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stretchr/testify/suite"
)

type AlpacaTestSuite struct {
	suite.Suite
}

func TestAlpacaTestSuite(t *testing.T) {
	suite.Run(t, new(AlpacaTestSuite))
}

func (s *AlpacaTestSuite) TestAlpaca() {
	// get account
	{
		// successful
		do = func(c *Client, req *http.Request) (*http.Response, error) {
			account := Account{
				ID: "some_id",
			}
			return &http.Response{
				Body: genBody(account),
			}, nil
		}

		acct, err := GetAccount()
		assert.Nil(s.T(), err)
		assert.NotNil(s.T(), acct)
		assert.Equal(s.T(), "some_id", acct.ID)

		// api failure
		do = func(c *Client, req *http.Request) (*http.Response, error) {
			return &http.Response{}, fmt.Errorf("fail")
		}

		acct, err = GetAccount()
		assert.NotNil(s.T(), err)
		assert.Nil(s.T(), acct)
	}

	// list positions
	{
		// successful
		do = func(c *Client, req *http.Request) (*http.Response, error) {
			positions := []Position{
				{Symbol: "APCA"},
			}
			return &http.Response{
				Body: genBody(positions),
			}, nil
		}

		positions, err := ListPositions()
		assert.Nil(s.T(), err)
		assert.Len(s.T(), positions, 1)

		// api failure
		do = func(c *Client, req *http.Request) (*http.Response, error) {
			return &http.Response{}, fmt.Errorf("fail")
		}

		positions, err = ListPositions()
		assert.NotNil(s.T(), err)
		assert.Nil(s.T(), positions)
	}

	// get clock
	{
		// successful
		do = func(c *Client, req *http.Request) (*http.Response, error) {
			clock := Clock{
				Timestamp: time.Now(),
				IsOpen:    true,
				NextOpen:  time.Now(),
				NextClose: time.Now(),
			}
			return &http.Response{
				Body: genBody(clock),
			}, nil
		}

		clock, err := GetClock()
		assert.Nil(s.T(), err)
		assert.NotNil(s.T(), clock)
		assert.True(s.T(), clock.IsOpen)

		// api failure
		do = func(c *Client, req *http.Request) (*http.Response, error) {
			return &http.Response{}, fmt.Errorf("fail")
		}

		clock, err = GetClock()
		assert.NotNil(s.T(), err)
		assert.Nil(s.T(), clock)
	}

	// get calendar
	{
		// successful
		do = func(c *Client, req *http.Request) (*http.Response, error) {
			calendar := []CalendarDay{
				{
					Date:  "2018-01-01",
					Open:  time.Now().Format(time.RFC3339),
					Close: time.Now().Format(time.RFC3339),
				},
			}
			return &http.Response{
				Body: genBody(calendar),
			}, nil
		}

		start := "2018-01-01"
		end := "2018-01-02"

		calendar, err := GetCalendar(&start, &end)
		assert.Nil(s.T(), err)
		assert.Len(s.T(), calendar, 1)

		// api failure
		do = func(c *Client, req *http.Request) (*http.Response, error) {
			return &http.Response{}, fmt.Errorf("fail")
		}

		calendar, err = GetCalendar(&start, &end)
		assert.NotNil(s.T(), err)
		assert.Nil(s.T(), calendar)
	}

	// list orders
	{
		// successful
		do = func(c *Client, req *http.Request) (*http.Response, error) {
			orders := []Order{
				{
					ID: "some_id",
				},
			}
			return &http.Response{
				Body: genBody(orders),
			}, nil
		}

		status := "new"
		until := time.Now()
		limit := 1

		orders, err := ListOrders(&status, &until, &limit)
		assert.Nil(s.T(), err)
		require.Len(s.T(), orders, 1)
		assert.Equal(s.T(), "some_id", orders[0].ID)

		// api failure
		do = func(c *Client, req *http.Request) (*http.Response, error) {
			return &http.Response{}, fmt.Errorf("fail")
		}

		orders, err = ListOrders(&status, &until, &limit)
		assert.NotNil(s.T(), err)
		assert.Nil(s.T(), orders)
	}

	// place order
	{
		// successful
		do = func(c *Client, req *http.Request) (*http.Response, error) {
			por := PlaceOrderRequest{}
			if err := json.NewDecoder(req.Body).Decode(&por); err != nil {
				return nil, err
			}
			return &http.Response{
				Body: genBody(Order{
					Qty:         por.Qty,
					Side:        por.Side,
					TimeInForce: por.TimeInForce,
					Type:        por.Type,
				}),
			}, nil
		}

		req := PlaceOrderRequest{
			AccountID:   "some_id",
			Qty:         decimal.New(1, 0),
			Side:        Buy,
			TimeInForce: GTC,
			Type:        Limit,
		}

		order, err := PlaceOrder(req)
		assert.Nil(s.T(), err)
		assert.NotNil(s.T(), order)
		assert.Equal(s.T(), req.Type, order.Type)

		// api failure
		do = func(c *Client, req *http.Request) (*http.Response, error) {
			return &http.Response{}, fmt.Errorf("fail")
		}

		order, err = PlaceOrder(req)
		assert.NotNil(s.T(), err)
		assert.Nil(s.T(), order)
	}

	// get order
	{
		// successful
		do = func(c *Client, req *http.Request) (*http.Response, error) {
			order := Order{
				ID: "some_order_id",
			}
			return &http.Response{
				Body: genBody(order),
			}, nil
		}

		order, err := GetOrder("some_order_id")
		assert.Nil(s.T(), err)
		assert.NotNil(s.T(), order)

		// api failure
		do = func(c *Client, req *http.Request) (*http.Response, error) {
			return &http.Response{}, fmt.Errorf("fail")
		}

		order, err = GetOrder("some_order_id")
		assert.NotNil(s.T(), err)
		assert.Nil(s.T(), order)
	}

	// cancel order
	{
		// successful
		do = func(c *Client, req *http.Request) (*http.Response, error) {
			return &http.Response{}, nil
		}

		assert.Nil(s.T(), CancelOrder("some_order_id"))

		// api failure
		do = func(c *Client, req *http.Request) (*http.Response, error) {
			return &http.Response{}, fmt.Errorf("fail")
		}

		assert.NotNil(s.T(), CancelOrder("some_order_id"))
	}

	// list assets
	{
		// successful
		do = func(c *Client, req *http.Request) (*http.Response, error) {
			assets := []Asset{
				{ID: "some_id"},
			}
			return &http.Response{
				Body: genBody(assets),
			}, nil
		}

		status := "active"

		assets, err := ListAssets(&status)
		assert.Nil(s.T(), err)
		require.Len(s.T(), assets, 1)
		assert.Equal(s.T(), "some_id", assets[0].ID)

		// api failure
		do = func(c *Client, req *http.Request) (*http.Response, error) {
			return &http.Response{}, fmt.Errorf("fail")
		}

		assets, err = ListAssets(&status)
		assert.NotNil(s.T(), err)
		assert.Nil(s.T(), assets)
	}

	// get asset
	{
		// successful
		do = func(c *Client, req *http.Request) (*http.Response, error) {
			asset := Asset{ID: "some_id"}
			return &http.Response{
				Body: genBody(asset),
			}, nil
		}

		asset, err := GetAsset("APCA")
		assert.Nil(s.T(), err)
		assert.NotNil(s.T(), asset)

		// api failure
		do = func(c *Client, req *http.Request) (*http.Response, error) {
			return &http.Response{}, fmt.Errorf("fail")
		}

		asset, err = GetAsset("APCA")
		assert.NotNil(s.T(), err)
		assert.Nil(s.T(), asset)
	}

	// list bar lists
	{
		// successful
		do = func(c *Client, req *http.Request) (*http.Response, error) {
			bars := []Bar{
				{
					Time:   1551157200,
					Open:   80.2,
					High:   80.86,
					Low:    80.02,
					Close:  80.51,
					Volume: 4283085,
				},
			}
			var barsMap = make(map[string][]Bar)
			barsMap["APCA"] = bars
			return &http.Response{
				Body: genBody(barsMap),
			}, nil
		}

		bars, err := ListBars([]string{"APCA"}, ListBarParams{Timeframe: "1D"})
		assert.Nil(s.T(), err)
		require.Len(s.T(), bars, 1)
		assert.Equal(s.T(), int64(1551157200), bars["APCA"][0].Time)

		// api failure
		do = func(c *Client, req *http.Request) (*http.Response, error) {
			return &http.Response{}, fmt.Errorf("fail")
		}

		bars, err = ListBars([]string{"APCA"}, ListBarParams{Timeframe: "1D"})
		assert.NotNil(s.T(), err)
		assert.Nil(s.T(), bars)
	}

	// get bar list
	{
		// successful
		do = func(c *Client, req *http.Request) (*http.Response, error) {
			bars := []Bar{
				{
					Time:   1551157200,
					Open:   80.2,
					High:   80.86,
					Low:    80.02,
					Close:  80.51,
					Volume: 4283085,
				},
			}
			var barsMap = make(map[string][]Bar)
			barsMap["APCA"] = bars
			return &http.Response{
				Body: genBody(barsMap),
			}, nil
		}

		bars, err := GetSymbolBars("APCA", ListBarParams{Timeframe: "1D"})
		assert.Nil(s.T(), err)
		assert.NotNil(s.T(), bars)

		// api failure
		do = func(c *Client, req *http.Request) (*http.Response, error) {
			return &http.Response{}, fmt.Errorf("fail")
		}

		bars, err = GetSymbolBars("APCA", ListBarParams{Timeframe: "1D"})
		assert.NotNil(s.T(), err)
		assert.Nil(s.T(), bars)
	}

	// test verify
	{
		// 200
		resp := &http.Response{
			StatusCode: http.StatusOK,
		}

		assert.Nil(s.T(), verify(resp))

		// 500
		resp = &http.Response{
			StatusCode: http.StatusInternalServerError,
			Body:       genBody(APIError{Code: 1010101, Message: "server is dead"}),
		}

		assert.NotNil(s.T(), verify(resp))
	}
}

type nopCloser struct {
	io.Reader
}

func (nopCloser) Close() error { return nil }

func genBody(data interface{}) io.ReadCloser {
	buf, _ := json.Marshal(data)
	return nopCloser{bytes.NewBuffer(buf)}
}
