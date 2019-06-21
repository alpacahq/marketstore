package binance

import (
	"context"
)

// DepthService show depth info
type DepthService struct {
	c      *Client
	symbol string
	limit  *int
}

// Symbol set symbol
func (s *DepthService) Symbol(symbol string) *DepthService {
	s.symbol = symbol
	return s
}

// Limit set limit
func (s *DepthService) Limit(limit int) *DepthService {
	s.limit = &limit
	return s
}

// Do send request
func (s *DepthService) Do(ctx context.Context, opts ...RequestOption) (res *DepthResponse, err error) {
	r := &request{
		method:   "GET",
		endpoint: "/api/v1/depth",
	}
	r.setParam("symbol", s.symbol)
	if s.limit != nil {
		r.setParam("limit", *s.limit)
	}
	data, err := s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return nil, err
	}
	j, err := newJSON(data)
	if err != nil {
		return nil, err
	}
	res = new(DepthResponse)
	res.LastUpdateID = j.Get("lastUpdateId").MustInt64()
	bidsLen := len(j.Get("bids").MustArray())
	res.Bids = make([]Bid, bidsLen)
	for i := 0; i < bidsLen; i++ {
		item := j.Get("bids").GetIndex(i)
		res.Bids[i] = Bid{
			Price:    item.GetIndex(0).MustString(),
			Quantity: item.GetIndex(1).MustString(),
		}
	}
	asksLen := len(j.Get("asks").MustArray())
	res.Asks = make([]Ask, asksLen)
	for i := 0; i < asksLen; i++ {
		item := j.Get("asks").GetIndex(i)
		res.Asks[i] = Ask{
			Price:    item.GetIndex(0).MustString(),
			Quantity: item.GetIndex(1).MustString(),
		}
	}
	return res, nil
}

// DepthResponse define depth info with bids and asks
type DepthResponse struct {
	LastUpdateID int64 `json:"lastUpdateId"`
	Bids         []Bid `json:"bids"`
	Asks         []Ask `json:"asks"`
}

// Bid define bid info with price and quantity
type Bid struct {
	Price    string
	Quantity string
}

// Ask define ask info with price and quantity
type Ask struct {
	Price    string
	Quantity string
}
