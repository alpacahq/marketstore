package binance

import (
	"context"
	"encoding/json"
)

// ListBookTickersService list all book tickers
type ListBookTickersService struct {
	c *Client
}

// Do send request
func (s *ListBookTickersService) Do(ctx context.Context, opts ...RequestOption) (res []*BookTicker, err error) {
	r := &request{
		method:   "GET",
		endpoint: "/api/v3/ticker/bookTicker",
	}
	data, err := s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return []*BookTicker{}, err
	}
	res = make([]*BookTicker, 0)
	err = json.Unmarshal(data, &res)
	if err != nil {
		return []*BookTicker{}, err
	}
	return res, nil
}

// BookTickerService list symbol's book ticker
type BookTickerService struct {
	c      *Client
	symbol string
}

// Symbol set symbol
func (s *BookTickerService) Symbol(symbol string) *BookTickerService {
	s.symbol = symbol
	return s
}

// Do send request
func (s *BookTickerService) Do(ctx context.Context, opts ...RequestOption) (res *BookTicker, err error) {
	r := &request{
		method:   "GET",
		endpoint: "/api/v3/ticker/bookTicker",
	}
	r.setParam("symbol", s.symbol)
	data, err := s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return nil, err
	}
	res = new(BookTicker)
	err = json.Unmarshal(data, &res)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// BookTicker define book ticker info
type BookTicker struct {
	Symbol      string `json:"symbol"`
	BidPrice    string `json:"bidPrice"`
	BidQuantity string `json:"bidQty"`
	AskPrice    string `json:"askPrice"`
	AskQuantity string `json:"askQty"`
}

// ListPricesService list all ticker prices
type ListPricesService struct {
	c *Client
}

// Do send request
func (s *ListPricesService) Do(ctx context.Context, opts ...RequestOption) (res []*SymbolPrice, err error) {
	r := &request{
		method:   "GET",
		endpoint: "/api/v1/ticker/allPrices",
	}
	data, err := s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return []*SymbolPrice{}, err
	}
	res = make([]*SymbolPrice, 0)
	err = json.Unmarshal(data, &res)
	if err != nil {
		return []*SymbolPrice{}, err
	}
	return res, nil
}

// SymbolPrice define symbol and price pair
type SymbolPrice struct {
	Symbol string `json:"symbol"`
	Price  string `json:"price"`
}

// PriceChangeStatsService show stats of price change in last 24 hours
type PriceChangeStatsService struct {
	c      *Client
	symbol string
}

// Symbol set symbol
func (s *PriceChangeStatsService) Symbol(symbol string) *PriceChangeStatsService {
	s.symbol = symbol
	return s
}

// Do send request
func (s *PriceChangeStatsService) Do(ctx context.Context, opts ...RequestOption) (res *PriceChangeStats, err error) {
	r := &request{
		method:   "GET",
		endpoint: "/api/v1/ticker/24hr",
	}
	r.setParam("symbol", s.symbol)
	data, err := s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return res, err
	}
	res = new(PriceChangeStats)
	err = json.Unmarshal(data, res)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// ListPriceChangeStatsService show stats of price change in last 24 hours for all symbols
type ListPriceChangeStatsService struct {
	c *Client
}

// Do send request
func (s *ListPriceChangeStatsService) Do(ctx context.Context, opts ...RequestOption) (res []*PriceChangeStats, err error) {
	r := &request{
		method:   "GET",
		endpoint: "/api/v1/ticker/24hr",
	}
	data, err := s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return res, err
	}
	res = make([]*PriceChangeStats, 0)
	err = json.Unmarshal(data, &res)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// PriceChangeStats define price change stats
type PriceChangeStats struct {
	Symbol             string `json:"symbol"`
	PriceChange        string `json:"priceChange"`
	PriceChangePercent string `json:"priceChangePercent"`
	WeightedAvgPrice   string `json:"weightedAvgPrice"`
	PrevClosePrice     string `json:"prevClosePrice"`
	LastPrice          string `json:"lastPrice"`
	BidPrice           string `json:"bidPrice"`
	AskPrice           string `json:"askPrice"`
	OpenPrice          string `json:"openPrice"`
	HighPrice          string `json:"highPrice"`
	LowPrice           string `json:"lowPrice"`
	Volume             string `json:"volume"`
	OpenTime           int64  `json:"openTime"`
	CloseTime          int64  `json:"closeTime"`
	FristID            int64  `json:"firstId"`
	LastID             int64  `json:"lastId"`
	Count              int64  `json:"count"`
}
