package alpaca

import (
	"time"

	"github.com/shopspring/decimal"
)

type Account struct {
	ID               string          `json:"id"`
	CreatedAt        time.Time       `json:"created_at"`
	UpdatedAt        time.Time       `json:"updated_at"`
	DeletedAt        *time.Time      `json:"deleted_at"`
	Status           string          `json:"status"`
	Currency         string          `json:"currency"`
	Cash             decimal.Decimal `json:"cash"`
	CashWithdrawable decimal.Decimal `json:"cash_withdrawable"`
	TradingBlocked   bool            `json:"trading_blocked"`
	TransfersBlocked bool            `json:"transfers_blocked"`
	AccountBlocked   bool            `json:"account_blocked"`
	BuyingPower      decimal.Decimal `json:"buying_power"`
}

type Order struct {
	ID             string           `json:"id"`
	ClientOrderID  string           `json:"client_order_id"`
	CreatedAt      time.Time        `json:"created_at"`
	UpdatedAt      time.Time        `json:"updated_at"`
	SubmittedAt    time.Time        `json:"submitted_at"`
	FilledAt       *time.Time       `json:"filled_at"`
	ExpiredAt      *time.Time       `json:"expired_at"`
	CanceledAt     *time.Time       `json:"canceled_at"`
	FailedAt       *time.Time       `json:"failed_at"`
	AssetID        string           `json:"asset_id"`
	Symbol         string           `json:"symbol"`
	Exchange       string           `json:"exchange"`
	Class          string           `json:"asset_class"`
	Qty            decimal.Decimal  `json:"qty"`
	FilledQty      decimal.Decimal  `json:"filled_qty"`
	Type           OrderType        `json:"order_type"`
	Side           Side             `json:"side"`
	TimeInForce    TimeInForce      `json:"time_in_force"`
	LimitPrice     *decimal.Decimal `json:"limit_price"`
	FilledAvgPrice *decimal.Decimal `json:"filled_avg_price"`
	StopPrice      *decimal.Decimal `json:"stop_price"`
	Status         string           `json:"status"`
}

type Position struct {
	AssetID        string          `json:"asset_id"`
	Symbol         string          `json:"symbol"`
	Exchange       string          `json:"exchange"`
	Class          string          `json:"asset_class"`
	AccountID      string          `json:"account_id"`
	EntryPrice     decimal.Decimal `json:"avg_entry_price"`
	Qty            decimal.Decimal `json:"qty"`
	Side           string          `json:"side"`
	MarketValue    decimal.Decimal `json:"market_value"`
	CostBasis      decimal.Decimal `json:"cost_basis"`
	UnrealizedPL   decimal.Decimal `json:"unrealized_pl"`
	UnrealizedPLPC decimal.Decimal `json:"unrealized_plpc"`
	CurrentPrice   decimal.Decimal `json:"current_price"`
	LastdayPrice   decimal.Decimal `json:"lastday_price"`
	ChangeToday    decimal.Decimal `json:"change_today"`
}

type Asset struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Exchange string `json:"exchange"`
	Class    string `json:"asset_class"`
	Symbol   string `json:"symbol"`
	Status   string `json:"status"`
	Tradable bool   `json:"tradable"`
}

type Fundamental struct {
	AssetID           string          `json:"asset_id"`
	Symbol            string          `json:"symbol"`
	FullName          string          `json:"full_name"`
	IndustryName      string          `json:"industry_name"`
	IndustryGroup     string          `json:"industry_group"`
	Sector            string          `json:"sector"`
	PERatio           float32         `json:"pe_ratio"`
	PEGRatio          float32         `json:"peg_ratio"`
	Beta              float32         `json:"beta"`
	EPS               float32         `json:"eps"`
	MarketCap         int64           `json:"market_cap"`
	SharesOutstanding int64           `json:"shares_outstanding"`
	AvgVol            int64           `json:"avg_vol"`
	DivRate           float32         `json:"div_rate"`
	ROE               float32         `json:"roe"`
	ROA               float32         `json:"roa"`
	PS                float32         `json:"ps"`
	PC                float32         `json:"pc"`
	GrossMargin       float32         `json:"gross_margin"`
	FiftyTwoWeekHigh  decimal.Decimal `json:"fifty_two_week_high"`
	FiftyTwoWeekLow   decimal.Decimal `json:"fifty_two_week_low"`
	ShortDescription  string          `json:"short_description"`
	LongDescription   string          `json:"long_description"`
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

type Quote struct {
	BidTimestamp  time.Time `json:"bid_timestamp"`
	Bid           float32   `json:"bid"`
	AskTimestamp  time.Time `json:"ask_timestamp"`
	Ask           float32   `json:"ask"`
	LastTimestamp time.Time `json:"last_timestamp"`
	Last          float32   `json:"last"`
	AssetID       string    `json:"asset_id"`
	Symbol        string    `json:"symbol"`
	Class         string    `json:"asset_class"`
}

type CalendarDay struct {
	Date  string `json:"date"`
	Open  string `json:"open"`
	Close string `json:"close"`
}

type Clock struct {
	Timestamp time.Time `json:"timestamp"`
	IsOpen    bool      `json:"is_open"`
	NextOpen  time.Time `json:"next_open"`
	NextClose time.Time `json:"next_close"`
}

type PlaceOrderRequest struct {
	AccountID     string           `json:"-"`
	AssetKey      *string          `json:"symbol"`
	Qty           decimal.Decimal  `json:"qty"`
	Side          Side             `json:"side"`
	Type          OrderType        `json:"type"`
	TimeInForce   TimeInForce      `json:"time_in_force"`
	LimitPrice    *decimal.Decimal `json:"limit_price"`
	StopPrice     *decimal.Decimal `json:"stop_price"`
	ClientOrderID string           `json:"client_order_id"`
}

type Side string

const (
	Buy  Side = "buy"
	Sell Side = "sell"
)

type OrderType string

const (
	Market        OrderType = "market"
	Limit         OrderType = "limit"
	Stop          OrderType = "stop"
	StopLimit     OrderType = "stop_limit"
	MarketOnClose OrderType = "market_on_close"
	LimitOnClose  OrderType = "limit_on_close"
)

type TimeInForce string

const (
	Day TimeInForce = "day"
	GTC TimeInForce = "gtc"
	OPG TimeInForce = "opg"
	IOC TimeInForce = "ioc"
	FOK TimeInForce = "fok"
	GTX TimeInForce = "gtx"
	GTD TimeInForce = "gtd"
	CLS TimeInForce = "cls"
)

// stream

// ClientMsg is the standard message sent by clients of the stream interface
type ClientMsg struct {
	Action string      `json:"action" msgpack:"action"`
	Data   interface{} `json:"data" msgpack:"data"`
}

// ServerMsg is the standard message sent by the server to update clients
// of the stream interface
type ServerMsg struct {
	Stream string      `json:"stream" msgpack:"stream"`
	Data   interface{} `json:"data"`
}

type TradeUpdate struct {
	Event string `json:"event"`
	Order Order  `json:"order"`
}
