package api

import (
	"github.com/alpacahq/marketstore/v4/utils/models/enum"
)

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

// TradeConditionMapping provides a mapping from Polygon integer format to Marketstore's internal representation
var TradeConditionMapping = map[byte]enum.TradeCondition{
	0:  enum.RegularSale,
	1:  enum.Acquisition,
	2:  enum.AveragePriceTrade,
	3:  enum.AutomaticExecution,
	4:  enum.BunchedTrade,
	5:  enum.BunchedSoldTrade,
	7:  enum.CashSale,
	8:  enum.ClosingPrints,
	9:  enum.CrossTrade,
	10: enum.DerivativelyPriced,
	11: enum.Distribution,
	12: enum.FormT,
	13: enum.ExtendedHoursTrade,
	14: enum.IntermarketSweep,
	15: enum.MarketCenterOfficialClose,
	16: enum.MarketCenterOfficialOpen,
	20: enum.NextDay,
	21: enum.PriceVariationTrade,
	22: enum.PriorReferencePrice,
	23: enum.Rule155Trade,
	25: enum.OpeningPrints,
	27: enum.StoppedStock,
	28: enum.ReopeningPrints,
	29: enum.Seller,
	30: enum.SoldLast,
	32: enum.SoldOutOfSequence,
	34: enum.SplitTrade,
	37: enum.OddLotTrade,
	38: enum.CorrectedConsolidatedClose,
	52: enum.ContingentTrade,
	53: enum.QualifiedContingentTrade,
}

func ConvertTradeCondition(condition byte) byte {
	val, ok := TradeConditionMapping[condition]
	if !ok {
		val = enum.UnknownTradeCondition
	}
	return byte(val)
}
