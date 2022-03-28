package v1

import "time"

type Asset struct {
	ID           string `json:"id"`
	Name         string `json:"name"`
	Exchange     string `json:"exchange"`
	Class        string `json:"asset_class"`
	Symbol       string `json:"symbol"`
	Status       string `json:"status"`
	Tradable     bool   `json:"tradable"`
	Marginal   bool   `json:"marginal"`
	Shortable    bool   `json:"shortable"`
	EasyToBorrow bool   `json:"easy_to_borrow"`
}

type Bar struct {
	Time   int64   `json:"t"`
	Open   float32 `json:"o"`
	High   float32 `json:"h"`
	Low    float32 `json:"l"`
	Close  float32 `json:"c"`
	Volume int32   `json:"v"`
}

type ListBarParams struct {
	Timeframe string     `url:"timeframe,omitempty"`
	StartDt   *time.Time `url:"start_dt,omitempty"`
	EndDt     *time.Time `url:"end_dt,omitempty"`
	Limit     *int       `url:"limit,omitempty"`
}
