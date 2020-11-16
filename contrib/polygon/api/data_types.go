package api

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

var exchangeCodes = map[byte]Exchange{
	1:  NYSEAmerican,
	2:  NasdaqOMXBX,
	3:  NYSENational,
	4:  FinraADF,
	5:  MarketIndependent,
	6:  ISE,
	7:  CboeEDGA,
	8:  CboeEDGX,
	9:  NYSEChicago,
	10: NYSE,
	11: NYSEArca,
	13: CQS,
	12: Nasdaq,
	14: MEMX,
	15: IEX,
	16: CBSX,
	17: NasdaqOMXPSX,
	18: CboeBYX,
	19: CboeBZX,
}

// ExchangeCode converts a Polygon exchange id to the internal representation
func ExchangeCode(polygonExchange byte) byte {
	val, ok := exchangeCodes[polygonExchange]
	if !ok {
		val = UndefinedExchange
	}
	return byte(val)
}

// PolygonTape represents
type PolygonTape byte

const (
	PolygonTape1 PolygonTape = 1 // CTA / NYSE
	PolygonTape2 PolygonTape = 2 // CTA / NYSE
	PolygonTape3 PolygonTape = 3 // UTP / NASDAQ
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

var tapeCodes = map[PolygonTape]Tape{
	PolygonTape1: TapeA,
	PolygonTape2: TapeB,
	PolygonTape3: TapeC,
}

func TapeCode(tape byte) byte {
	t, ok := tapeCodes[PolygonTape(tape)]
	if !ok {
		t = UndefinedTape
	}
	return byte(t)
}

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

var TradeConditionMapping = map[byte]TradeCondition{
	0:  RegularSale,
	1:  Acquisition,
	2:  AveragePriceTrade,
	3:  AutomaticExecution,
	4:  BunchedTrade,
	5:  BunchedSoldTrade,
	7:  CashSale,
	8:  ClosingPrints,
	9:  CrossTrade,
	10: DerivativelyPriced,
	11: Distribution,
	12: FormT,
	13: ExtendedHoursTrade,
	14: IntermarketSweep,
	15: MarketCenterOfficialClose,
	16: MarketCenterOfficialOpen,
	20: NextDay,
	21: PriceVariationTrade,
	22: PriorReferencePrice,
	23: Rule155Trade,
	25: OpeningPrints,
	27: StoppedStock,
	28: ReopeningPrints,
	29: Seller,
	30: SoldLast,
	32: SoldOutOfSequence,
	34: SplitTrade,
	37: OddLotTrade,
	38: CorrectedConsolidatedClose,
	52: ContingentTrade,
	53: QualifiedContingentTrade,
}

func TradeConditionCode(condition byte) byte {
	val, ok := TradeConditionMapping[condition]
	if !ok {
		val = UnknownTradeCondition
	}
	return byte(val)
}
