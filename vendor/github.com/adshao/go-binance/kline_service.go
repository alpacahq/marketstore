package binance

import (
	"context"
	"fmt"
)

// KlinesService list klines
type KlinesService struct {
	c         *Client
	symbol    string
	interval  string
	limit     *int
	startTime *int64
	endTime   *int64
}

// Symbol set symbol
func (s *KlinesService) Symbol(symbol string) *KlinesService {
	s.symbol = symbol
	return s
}

// Interval set interval
func (s *KlinesService) Interval(interval string) *KlinesService {
	s.interval = interval
	return s
}

// Limit set limit
func (s *KlinesService) Limit(limit int) *KlinesService {
	s.limit = &limit
	return s
}

// StartTime set startTime
func (s *KlinesService) StartTime(startTime int64) *KlinesService {
	s.startTime = &startTime
	return s
}

// EndTime set endTime
func (s *KlinesService) EndTime(endTime int64) *KlinesService {
	s.endTime = &endTime
	return s
}

// Do send request
func (s *KlinesService) Do(ctx context.Context, opts ...RequestOption) (res []*Kline, err error) {
	r := &request{
		method:   "GET",
		endpoint: "/api/v1/klines",
	}
	r.setParam("symbol", s.symbol)
	r.setParam("interval", s.interval)
	if s.limit != nil {
		r.setParam("limit", *s.limit)
	}
	if s.startTime != nil {
		r.setParam("startTime", *s.startTime)
	}
	if s.endTime != nil {
		r.setParam("endTime", *s.endTime)
	}
	data, err := s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return []*Kline{}, err
	}
	j, err := newJSON(data)
	if err != nil {
		return []*Kline{}, err
	}
	num := len(j.MustArray())
	res = make([]*Kline, num)
	for i := 0; i < num; i++ {
		item := j.GetIndex(i)
		if len(item.MustArray()) < 11 {
			err = fmt.Errorf("invalid kline response")
			return []*Kline{}, err
		}
		res[i] = &Kline{
			OpenTime:                 item.GetIndex(0).MustInt64(),
			Open:                     item.GetIndex(1).MustString(),
			High:                     item.GetIndex(2).MustString(),
			Low:                      item.GetIndex(3).MustString(),
			Close:                    item.GetIndex(4).MustString(),
			Volume:                   item.GetIndex(5).MustString(),
			CloseTime:                item.GetIndex(6).MustInt64(),
			QuoteAssetVolume:         item.GetIndex(7).MustString(),
			TradeNum:                 item.GetIndex(8).MustInt64(),
			TakerBuyBaseAssetVolume:  item.GetIndex(9).MustString(),
			TakerBuyQuoteAssetVolume: item.GetIndex(10).MustString(),
		}
	}
	return res, nil
}

// Kline define kline info
type Kline struct {
	OpenTime                 int64  `json:"openTime"`
	Open                     string `json:"open"`
	High                     string `json:"high"`
	Low                      string `json:"low"`
	Close                    string `json:"close"`
	Volume                   string `json:"volume"`
	CloseTime                int64  `json:"closeTime"`
	QuoteAssetVolume         string `json:"quoteAssetVolume"`
	TradeNum                 int64  `json:"tradeNum"`
	TakerBuyBaseAssetVolume  string `json:"takerBuyBaseAssetVolume"`
	TakerBuyQuoteAssetVolume string `json:"takerBuyQuoteAssetVolume"`
}
