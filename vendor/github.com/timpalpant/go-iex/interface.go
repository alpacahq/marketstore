package iex

import (
	"encoding/json"
	"fmt"
)

const IEXTP1 = "IEXTP1"

const (
	StartMessages    = "O"
	StartSystemHours = "S"
	StartMarketHours = "R"
	EndMarketHours   = "M"
	EndSystemHours   = "E"
	EndMessages      = "C"
)

const (
	// Trading halted across all US equity markets.
	TradingHalted = "H"
	// Trading halt released into an Order Acceptance Period
	// (IEX-listed securities only)
	TradingOrderAcceptancePeriod = "O"
	// Trading paused and Order Acceptance Period on IEX
	// (IEX-listed securities only)
	TradingPaused = "P"
	// Trading on IEX
	Trading = "T"
)

const (
	// Trading halt reasons.
	HaltNewsPending            = "T1"
	IPOIssueNotYetTrading      = "IPO1"
	IPOIssueDeferred           = "IPOD"
	MarketCircuitBreakerLevel3 = "MCB3"
	ReasonNotAvailable         = "NA"

	// Order Acceptance Period Reasons
	HaltNewsDisseminations           = "T2"
	IPONewIssueOrderAcceptancePeriod = "IPO2"
	IPOPreLaunchPeriod               = "IPO3"
	MarketCircuitBreakerLevel1       = "MCB1"
	MarketCircuitBreakerLevel2       = "MCB2"
)

const (
	MarketOpen  = "MarketOpen"
	MarketClose = "MarketClose"
)

type TOPS struct {
	// Refers to the stock ticker.
	Symbol string
	// Refers to IEX’s percentage of the market in the stock.
	MarketPercent float64
	// Refers to amount of shares on the bid on IEX.
	BidSize int
	// Refers to the best bid price on IEX.
	BidPrice float64
	// Refers to amount of shares on the ask on IEX.
	AskSize int
	// Refers to the best ask price on IEX.
	AskPrice float64
	// Refers to shares traded in the stock on IEX.
	Volume int
	// Refers to last sale price of the stock on IEX. (Refer to the attribution section above.)
	LastSalePrice float64
	// Refers to last sale size of the stock on IEX.
	LastSaleSize int
	// Refers to last sale time of the stock on IEX.
	LastSaleTime Time
	// Refers to the last update time of the data.
	// If the value is the zero Time, IEX has not quoted the symbol in
	// the trading day.
	LastUpdated Time
}

type Last struct {
	// Refers to the stock ticker.
	Symbol string
	// Refers to last sale price of the stock on IEX. (Refer to the attribution section above.)
	Price float64
	// Refers to last sale size of the stock on IEX.
	Size int
	// Refers to last sale time in epoch time of the stock on IEX.
	Time Time
}

type HIST struct {
	// URL to the available data file.
	Link string
	// Date of the data contained in this file.
	Date string
	// Which data feed is contained in this file.
	Feed string
	// The feed format specification version.
	Version string
	// The protocol version of the data.
	Protocol string
	// The size, in bytes, of the data file.
	Size int64 `json:",string"`
}

type DEEP struct {
	Symbol        string
	MarketPercent float64
	Volume        int
	LastSalePrice float64
	LastSaleSize  int
	LastSaleTime  Time
	LastUpdate    Time
	Bids          []*Quote
	Asks          []*Quote
	SystemEvent   *SystemEvent
	TradingStatus *TradingStatusMessage
	OpHaltStatus  *OpHaltStatus
	SSRStatus     *SSRStatus
	SecurityEvent *SecurityEventMessage
	Trades        []*Trade
	TradeBreaks   []*TradeBreak
}

type Quote struct {
	Price     float64
	Size      float64
	Timestamp Time
}

type SystemEvent struct {
	SystemEvent string
	Timestamp   Time
}

type TradingStatusMessage struct {
	Status    string
	Reason    string
	Timestamp Time
}

type OpHaltStatus struct {
	IsHalted  bool
	Timestamp Time
}

type SSRStatus struct {
	IsSSR     bool
	Detail    string
	Timestamp Time
}

type SecurityEventMessage struct {
	SecurityEvent string
	Timestamp     Time
}

type Trade struct {
	Price                 float64
	Size                  int
	TradeID               int64
	IsISO                 bool
	IsOddLot              bool
	IsOutsideRegularHours bool
	IsSinglePriceCross    bool
	IsTradeThroughExcempt bool
	Timestamp             Time
}

type TradeBreak struct {
	Price                 float64
	Size                  int
	TradeID               int64
	IsISO                 bool
	IsOddLot              bool
	IsOutsideRegularHours bool
	IsSinglePriceCross    bool
	IsTradeThroughExcempt bool
	Timestamp             Time
}

type Book struct {
	Bids []*Quote
	Asks []*Quote
}

type Market struct {
	// Refers to the Market Identifier Code (MIC).
	MIC string
	// Refers to the tape id of the venue.
	TapeID string
	// Refers to name of the venue defined by IEX.
	VenueName string
	// Refers to the amount of traded shares reported by the venue.
	Volume int
	// Refers to the amount of Tape A traded shares reported by the venue.
	TapeA int
	// Refers to the amount of Tape B traded shares reported by the venue.
	TapeB int
	// Refers to the amount of Tape C traded shares reported by the venue.
	TapeC int
	// Refers to the venue’s percentage of shares traded in the market.
	MarketPercent float64
	// Refers to the last update time of the data.
	LastUpdated Time
}

type Symbol struct {
	// Refers to the symbol represented in Nasdaq Integrated symbology (INET).
	Symbol string
	// Refers to the name of the company or security.
	Name string
	// Refers to the date the symbol reference data was generated.
	Date string
	// Will be true if the symbol is enabled for trading on IEX.
	IsEnabled bool
}

type IntradayStats struct {
	// Refers to single counted shares matched from executions on IEX.
	Volume struct {
		Value       int
		LastUpdated Time
	}
	// Refers to number of symbols traded on IEX.
	SymbolsTraded struct {
		Value       int
		LastUpdated Time
	}
	// Refers to executions received from order routed to away trading centers.
	RoutedVolume struct {
		Value       int
		LastUpdated Time
	}
	// Refers to sum of matched volume times execution price of those trades.
	Notional struct {
		Value       int
		LastUpdated Time
	}
	// Refers to IEX’s percentage of total US Equity market volume.
	MarketShare struct {
		Value       float64
		LastUpdated Time
	}
}

type Stats struct {
	// Refers to the trading day.
	Date string
	// Refers to executions received from order routed to away trading centers.
	Volume int
	// Refers to single counted shares matched from executions on IEX.
	RoutedVolume int
	// Refers to IEX’s percentage of total US Equity market volume.
	MarketShare float64
	// Will be true if the trading day is a half day.
	IsHalfDay bool
	// Refers to the number of lit shares traded on IEX (single-counted).
	LitVolume int
}

type intBool bool

func (bit *intBool) UnmarshalJSON(data []byte) error {
	asString := string(data)
	if asString == "1" || asString == "true" {
		*bit = true
	} else if asString == "0" || asString == "false" {
		*bit = false
	} else {
		return fmt.Errorf("boolean unmarshal error: invalid input %s", asString)
	}

	return nil
}

// UnmarshalJSON customizes JSON unmarshalling for the Stats
// type to be able to decode either 0/1 or true/false in the
// IsHalfDay field (see: https://github.com/timpalpant/go-iex/issues/21).
func (s *Stats) UnmarshalJSON(data []byte) error {
	type Alias Stats
	tmp := &struct {
		IsHalfDay intBool
		*Alias
	}{
		Alias: (*Alias)(s),
	}

	if err := json.Unmarshal(data, &tmp); err != nil {
		return err
	}

	s.IsHalfDay = bool(tmp.IsHalfDay)
	return nil
}

type Records struct {
	// Refers to single counted shares matched from executions on IEX.
	Volume *Record
	// Refers to number of symbols traded on IEX.
	SymbolsTraded *Record
	// Refers to executions received from order routed to away trading centers.
	RoutedVolume *Record
	// Refers to sum of matched volume times execution price of those trades.
	Notional *Record
}

type Record struct {
	Value            int    `json:"recordValue"`
	Date             string `json:"recordDate"`
	PreviousDayValue int
	Avg30Value       float64
}

type HistoricalSummary struct {
	AverageDailyVolume          float64
	AverageDailyRoutedVolume    float64
	AverageMarketShare          float64
	AverageOrderSize            float64
	AverageFillSize             float64
	Bin100Percent               float64
	Bin101Percent               float64
	Bin200Percent               float64
	Bin300Percent               float64
	Bin400Percent               float64
	Bin500Percent               float64
	Bin1000Percent              float64
	Bin5000Percent              float64
	Bin10000Percent             float64
	Bin10000Trades              float64
	Bin20000Trades              float64
	Bin50000Trades              float64
	UniqueSymbolsTraded         float64
	BlockPercent                float64
	SelfCrossPercent            float64
	ETFPercent                  float64
	LargeCapPercent             float64
	MidCapPercent               float64
	SmallCapPercent             float64
	VenueARCXFirstWaveWeight    float64
	VenueBATSFirstWaveWeight    float64
	VenueBATYFirstWaveWeight    float64
	VenueEDGAFirstWaveWeight    float64
	VenueEDGXFirstWaveWeight    float64
	VenueOverallFirstWaveWeight float64
	VenueXASEFirstWaveWeight    float64
	VenueXBOSFirstWaveWeight    float64
	VenueXCHIFirstWaveWeight    float64
	VenueXCISFirstWaveWeight    float64
	VenueXNGSFirstWaveWeight    float64
	VenueXNYSFirstWaveWeight    float64
	VenueXPHLFirstWaveWeight    float64
	VenueARCXFirstWaveRate      float64
	VenueBATSFirstWaveRate      float64
	VenueBATYFirstWaveRate      float64
	VenueEDGAFirstWaveRate      float64
	VenueEDGXFirstWaveRate      float64
	VenueOverallFirstWaveRate   float64
	VenueXASEFirstWaveRate      float64
	VenueXBOSFirstWaveRate      float64
	VenueXCHIFirstWaveRate      float64
	VenueXCISFirstWaveRate      float64
	VenueXNGSFirstWaveRate      float64
	VenueXNYSFirstWaveRate      float64
	VenueXPHLFirstWaveRate      float64
}

type KeyStats struct {
	CompanyName            string
	Marketcap              float64 // is not calculated in real time.
	Beta                   float64
	Week52high             float64
	Week52low              float64
	Week52change           float64
	ShortInterest          float64
	ShortDateJSON          interface{} `json:"shortDate"`
	ShortDate              string      // if not available, iex returns a number 0, see ShortDateJSON
	DividendRate           float64
	DividendYield          float64
	ExDividendDateJSON     interface{} `json:"exDividendDate"`
	ExDividendDate         string      // if not available, iex returns a number 0, see ExDividendDateJSON
	LatestEPS              float64     // (Most recent quarter)
	LatestEPSDate          string
	SharesOutstanding      float64
	Float                  float64
	ReturnOnEquity         float64 // (Trailing twelve months)
	ConsensusEPS           float64 // (Most recent quarter)
	NumberOfEstimates      float64 // (Most recent quarter)
	Symbol                 string
	EBITDA                 float64     // (Trailing twelve months)
	Revenue                float64     // (Trailing twelve months)
	GrossProfit            float64     // (Trailing twelve months)
	Cash                   float64     // refers to total cash. (Trailing twelve months)
	Debt                   float64     // refers to total debt. (Trailing twelve months)
	TtmEPS                 float64     // (Trailing twelve months)
	RevenuePerShare        float64     // (Trailing twelve months)
	RevenuePerEmployeeJSON interface{} `json:"revenuePerEmployee"`
	RevenuePerEmployee     float64     // (Trailing twelve months)
	PeRatioHigh            float64
	PeRatioLow             float64
	EPSSurpriseDollar      float64 // refers to the difference between actual EPS and consensus EPS in dollars.
	EPSSurprisePercent     float64 // refers to the percent difference between actual EPS and consensus EPS.
	ReturnOnAssets         float64 // (Trailing twelve months)
	ReturnOnCapital        float64 // (Trailing twelve months)
	ProfitMargin           float64
	PriceToSales           float64
	PriceToBook            float64
	Day200MovingAvg        float64
	Day50MovingAvg         float64
	InstitutionPercent     float64 // represents top 15 institutions
	InsiderPercent         float64
	ShortRatio             float64
	Year5ChangePercent     float64
	Year2ChangePercent     float64
	Year1ChangePercent     float64
	YtdChangePercent       float64
	Month6ChangePercent    float64
	Month3ChangePercent    float64
	Month1ChangePercent    float64
	Day5ChangePercent      float64
}

type News struct {
	Datetime string
	Headline string
	Source   string
	URL      string
	Summary  string
	Related  string
}

type StockQuote struct {
	Symbol           string  // refers to the stock ticker.
	CompanyName      string  // refers to the company name.
	PrimaryExchange  string  // refers to the primary listings exchange.
	Sector           string  // refers to the sector of the stock.
	CalculationPrice string  // refers to the source of the latest price. ("tops", "sip", "previousclose" or "close")
	Open             float64 // refers to the official open price
	OpenTime         int64   // refers to the official listing exchange time for the open
	Close            float64 // refers to the official close price
	CloseTime        int64   // refers to the official listing exchange time for the close
	High             float64 // refers to the market-wide highest price from the SIP. 15 minute delayed
	Low              float64 // refers to the market-wide lowest price from the SIP. 15 minute delayed
	LatestPrice      float64 // refers to the latest price being the IEX real time price, the 15 minute delayed market price, or the previous close price.
	LatestSource     string  // refers to the source of latestPrice. ("IEX real time price", "15 minute delayed price", "Close" or "Previous close")
	LatestTime       string  // refers to a human readable time of the latestPrice. The format will vary based on latestSource.
	LatestUpdate     int64   // refers to the update time of latestPrice in milliseconds since midnight Jan 1, 1970.
	LatestVolume     int64   // refers to the total market volume of the stock.
	IexRealtimePrice float64 // refers to last sale price of the stock on IEX. (Refer to the attribution section above.)
	IexRealtimeSize  int64   // refers to last sale size of the stock on IEX.
	IexLastUpdated   int64   // refers to the last update time of the data in milliseconds since midnight Jan 1, 1970 UTC or -1 or 0. If the value is -1 or 0, IEX has not quoted the symbol in the trading day.
	DelayedPrice     float64 // refers to the 15 minute delayed market price.
	DelayedPriceTime int64   // refers to the time of the delayed market price.
	PreviousClose    float64 // refers to the adjusted close price of the last trading day of the stock.
	Change           float64 // is calculated using calculationPrice from previousClose.
	ChangePercent    float64 // is calculated using calculationPrice from previousClose.
	IexMarketPercent float64 // refers to IEX’s percentage of the market in the stock.
	IexVolume        int64   // refers to shares traded in the stock on IEX.
	AvgTotalVolume   int64   // refers to the 30 day average volume on all markets.
	IexBidPrice      float64 // refers to the best bid price on IEX.
	IexBidSize       int64   // refers to amount of shares on the bid on IEX.
	IexAskPrice      float64 // refers to the best ask price on IEX.
	IexAskSize       int64   // refers to amount of shares on the ask on IEX.
	MarketCap        int64   // is calculated in real time using calculationPrice.
	PeRatio          float64 // is calculated in real time using calculationPrice.
	Week52High       float64 // refers to the adjusted 52 week high.
	Week52Low        float64 // refers to the adjusted 52 week low.
	YtdChange        float64 // refers to the price change percentage from start of year to previous close.
}

type Company struct {
	Symbol      string
	CompanyName string
	Exchange    string
	Industry    string
	Website     string
	Description string
	CEO         string
	Sector      string
	IssueType   string // refers to the common issue type of the stock.
	// ad – American Depository Receipt (ADR’s)
	// re – Real Estate Investment Trust (REIT’s)
	// ce – Closed end fund (Stock and Bond Fund)
	// si – Secondary Issue
	// lp – Limited Partnerships
	// cs – Common Stock
	// et – Exchange Traded Fund (ETF)
	// (blank) = Not Available, i.e., Warrant, Note, or (non-filing) Closed Ended Funds
}

type Dividends struct {
	ExDate       string      // refers to the dividend ex-date
	PaymentDate  string      // refers to the payment date
	RecordDate   string      // refers to the dividend record date
	DeclaredDate string      // refers to the dividend declaration date
	AmountJSON   interface{} `json:"amount"`
	Amount       float64     // refers to the payment amount
	Flag         string      // refers to the dividend flag (
	// FI = Final dividend, div ends or instrument ends,
	// LI = Liquidation, instrument liquidates,
	// PR = Proceeds of a sale of rights or shares,
	// RE = Redemption of rights,
	// AC = Accrued dividend,
	// AR = Payment in arrears,
	// AD = Additional payment,
	// EX = Extra payment,
	// SP = Special dividend,
	// YE = Year end,
	// UR = Unknown rate,
	// SU = Regular dividend is suspended)
	Type      string // refers to the dividend payment type (Dividend income, Interest income, Stock dividend, Short term capital gain, Medium term capital gain, Long term capital gain, Unspecified term capital gain)
	Qualified string // refers to the dividend income type
	// P = Partially qualified income
	// Q = Qualified income
	// N = Unqualified income
	// null = N/A or unknown
	IndicatedJSON interface{} `json:"Indicated"`
	Indicated     float64     // refers to the indicated rate of the dividend
}

type Chart struct {
	// Only available on 1d charts
	Minute         string
	Average        float64
	Notional       float64
	NumberOfTrades int
	MarketHigh     float64
	MarketLow      float64

	// only available on 1d charts, 15 minutes delayed
	MarketAverage        float64
	MarketVolume         int
	MarketNotional       float64
	MarketNumberOfTrades int
	MarketChangeOverTime float64

	// TODO: Only available on 1d charts when chartSimplify = true
	// simplifyFactor: array (of what?)

	// Not availabe on 1d charts
	Date             string
	Open             float64
	Close            float64
	UnadjustedVolume int
	Change           float64
	ChangePercent    float64
	VWAP             float64 // volume weitghted average price

	// Available on all charts
	High           float64
	Low            float64
	Volume         int
	Label          string
	ChangeOverTime float64
}
