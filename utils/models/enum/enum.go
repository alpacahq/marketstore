package enum

// TradeCondition is the type of trade transaction
type TradeCondition byte

// List of all trade conditions
const (
	Acquisition                TradeCondition = 'A'
	AutomaticExecution         TradeCondition = 'E'
	AveragePriceTrade          TradeCondition = 'W'
	BunchedSoldTrade           TradeCondition = 'G'
	BunchedTrade               TradeCondition = 'B'
	CashSale                   TradeCondition = 'C'
	ClosingPrints              TradeCondition = '6'
	ContingentTrade            TradeCondition = 'V'
	CorrectedConsolidatedClose TradeCondition = '9'
	CrossTrade                 TradeCondition = 'X'
	DerivativelyPriced         TradeCondition = '4'
	Distribution               TradeCondition = 'D'
	ExtendedHoursTrade         TradeCondition = 'T'
	ExtendedTradingHours       TradeCondition = 'U'
	FormT                      TradeCondition = 'T'
	IntermarketSweep           TradeCondition = 'F'
	MarketCenterOfficialClose  TradeCondition = 'M'
	MarketCenterOfficialOpen   TradeCondition = 'Q'
	NextDay                    TradeCondition = 'N'
	OddLotTrade                TradeCondition = 'I'
	OpeningPrints              TradeCondition = 'O'
	PlaceholderFor611Exempt    TradeCondition = '8'
	PriceVariationTrade        TradeCondition = 'H'
	PriorReferencePrice        TradeCondition = 'P'
	QualifiedContingentTrade   TradeCondition = '7'
	RegularSale                TradeCondition = '@'
	ReopeningPrints            TradeCondition = '5'
	Rule155Trade               TradeCondition = 'K'
	Seller                     TradeCondition = 'R'
	SoldLast                   TradeCondition = 'L'
	SoldOutOfSequence          TradeCondition = 'Z'
	SplitTrade                 TradeCondition = 'S'
	StoppedStock               TradeCondition = '1'
	YellowFlagRegularTrade     TradeCondition = 'Y'
	UnknownTradeCondition      TradeCondition = 0
)
