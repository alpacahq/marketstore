package api

import (
	"fmt"
	"time"
)

// Trade is a stock trade that happened on the market.
type Trade struct {
	ID         int64     `json:"i"`
	Exchange   string    `json:"x"`
	Price      float64   `json:"p"`
	Size       uint32    `json:"s"`
	Timestamp  time.Time `json:"t"`
	Conditions []string  `json:"c"`
	Tape       string    `json:"z"`
}

// Quote is a stock quote from the market.
type Quote struct {
	BidExchange string    `json:"bx"`
	BidPrice    float64   `json:"bp"`
	BidSize     uint32    `json:"bs"`
	AskExchange string    `json:"ax"`
	AskPrice    float64   `json:"ap"`
	AskSize     uint32    `json:"as"`
	Timestamp   time.Time `json:"t"`
	Conditions  []string  `json:"c"`
	Tape        string    `json:"z"`
}

// Bar is an aggregate of trades
type Bar struct {
	Timestamp  time.Time `json:"t"`
	Open       float64   `json:"o"`
	High       float64   `json:"h"`
	Low        float64   `json:"l"`
	Close      float64   `json:"c"`
	Volume     uint64    `json:"v"`
	TradeCount uint64    `json:"n"`
	VWAP       float64   `json:"vw"`
}

// MultiBarItem contains a single bar for a symbol or an error
type MultiBarItem struct {
	Symbol string
	Bar    Bar
	Error  error
}

type multiBarResponse struct {
	NextPageToken *string          `json:"next_page_token"`
	Bars          map[string][]Bar `json:"bars"`
}

// Snapshot is a snapshot of a symbol.
type Snapshot struct {
	LatestTrade  *Trade `json:"latestTrade"`
	LatestQuote  *Quote `json:"latestQuote"`
	MinuteBar    *Bar   `json:"minuteBar"`
	DailyBar     *Bar   `json:"dailyBar"`
	PrevDailyBar *Bar   `json:"prevDailyBar"`
}

// GetBarsParams contains optional parameters for getting bars
type GetBarsParams struct {
	// TimeFrame is the aggregation size of the bars
	TimeFrame TimeFrame
	// Adjustment tells if the bars should be adjusted for corporate actions
	Adjustment Adjustment
	// Start is the inclusive beginning of the interval
	Start time.Time
	// End is the inclusive end of the interval
	End time.Time
	// TotalLimit is the limit of the total number of the returned bars.
	// If missing, all bars between start end end will be returned.
	TotalLimit int
	// PageLimit is the pagination size. If empty, the default page size will be used.
	PageLimit int
	// Feed is the source of the data: sip or iex.
	// If provided, it overrides the client's Feed option.
	Feed string
}

// TimeFrameUnite is the base unit of the timeframe.
type TimeFrameUnit string

// List of timeframe units
const (
	Min  TimeFrameUnit = "Min"
	Hour TimeFrameUnit = "Hour"
	Day  TimeFrameUnit = "Day"
)

// TimeFrame is the resolution of the bars
type TimeFrame struct {
	N    int
	Unit TimeFrameUnit
}

func NewTimeFrame(n int, unit TimeFrameUnit) TimeFrame {
	return TimeFrame{
		N:    n,
		Unit: unit,
	}
}

func (tf TimeFrame) String() string {
	return fmt.Sprintf("%d%s", tf.N, tf.Unit)
}

var (
	OneMin  TimeFrame = NewTimeFrame(1, Min)
	OneHour TimeFrame = NewTimeFrame(1, Hour)
	OneDay  TimeFrame = NewTimeFrame(1, Day)
)

// Adjustment specifies the corporate action adjustment(s) for the bars
type Adjustment string

// List of adjustments
const (
	Raw      Adjustment = "raw"
	Split    Adjustment = "split"
	Dividend Adjustment = "dividend"
	All      Adjustment = "all"
)
