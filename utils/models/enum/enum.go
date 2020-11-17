package enum

// Exchange identifies the exchange
type Exchange byte

// List of exchanges
// http://www.utpplan.com/DOC/uqdfspecification.pdf - 3.6 Market Center Originator ID
// https://www.ctaplan.com/publicdocs/ctaplan/CQS_Pillar_Output_Specification.pdf - 4.3 Participant ID
const (
	NYSEAmerican      Exchange = 'A'
	NasdaqOMXBX       Exchange = 'B'
	NYSENational      Exchange = 'C'
	FinraADF          Exchange = 'D'
	MarketIndependent Exchange = 'E' // UTP only
	MIAX              Exchange = 'H' // CTA only
	ISE               Exchange = 'I'
	CboeEDGA          Exchange = 'J'
	CboeEDGX          Exchange = 'K'
	LTSE              Exchange = 'L' // CTA only
	NYSEChicago       Exchange = 'M'
	NYSE              Exchange = 'N'
	NYSEArca          Exchange = 'P'
	NasdaqOMX         Exchange = 'Q' // UTP only
	CQS               Exchange = 'S' // CTA only
	Nasdaq            Exchange = 'T' // CTA only
	MEMX              Exchange = 'U' // CTA only
	IEX               Exchange = 'V'
	CBSX              Exchange = 'W'
	NasdaqOMXPSX      Exchange = 'X'
	CboeBYX           Exchange = 'Y'
	CboeBZX           Exchange = 'Z'
	UndefinedExchange Exchange = 0
)

// Tape identifies the modern "ticker tape"
type Tape byte

// Tapes
const (
	TapeA         Tape = 'A' // NYSE
	TapeB         Tape = 'B' // NYSE
	TapeC         Tape = 'C' // NYSE
	UndefinedTape Tape = 0
)

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

// QuoteCondition is the type of the quote
type QuoteCondition byte

// Quote conditions UTP - Nasdaq
// http://www.utpplan.com/DOC/uqdfspecification.pdf
// 7.5.2.2. Quote Condition
const (
	ManualAskAutomatedBid    QuoteCondition = 'A'
	ManualBidAutomatedAsk    QuoteCondition = 'B'
	FastTrading              QuoteCondition = 'F'
	ManualBidAndAsk          QuoteCondition = 'H'
	OrderImbalance           QuoteCondition = 'I'
	ClosedQuote              QuoteCondition = 'L'
	NonFirmQuote             QuoteCondition = 'N'
	OpeningQuoteAutomated    QuoteCondition = 'O'
	RegularTwoSidedOpen      QuoteCondition = 'R'
	ManualBidAndAskNonFirm   QuoteCondition = 'U'
	NoOfferNoBidOneSidedOpen QuoteCondition = 'Y'
	OrderInflux              QuoteCondition = 'X'
	NoOpenNoResume           QuoteCondition = 'Z'
	OnDemandIntraDayAuction  QuoteCondition = '4'
	UnknownQuoteCondition    QuoteCondition = 0
)

// Quote conditions CTA - NYSE
// https://www.ctaplan.com/publicdocs/ctaplan/CQS_Pillar_Output_Specification.pdf
// Appendix G
const (
	SlowQuoteOfferSide       QuoteCondition = 'A'
	SlowQuoteBidSide         QuoteCondition = 'B'
	SlowQuoteLRPBidSide      QuoteCondition = 'E'
	SlowQuoteLRPOfferSide    QuoteCondition = 'F'
	SlowQuoteBidAndOfferSide QuoteCondition = 'H'
	OpeningQuote             QuoteCondition = 'O'
	RegularMarketMakerOpen   QuoteCondition = 'R'
	SlowQuoteSetSlowList     QuoteCondition = 'W'
	ClosingQuote             QuoteCondition = 'C'
	MarketMakerQuotesClosed  QuoteCondition = 'L'
	SlowQuoteLRPBidAndOffer  QuoteCondition = 'U'
	//NonFirmQuote            QuoteCondition = 'N' // Same as Nasdaq
	//OnDemandIntraDayAuction QuoteCondition = '4' // Same as Nasdaq

)
