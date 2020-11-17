package api

import (
	"github.com/alpacahq/marketstore/v4/models/enum"
)

// mapping between Polygon's integer exchange codes and Marketstore's internal representation
var exchangeCodeMapping = map[int]enum.Exchange{
	1:  enum.NYSEAmerican,
	2:  enum.NasdaqOMXBX,
	3:  enum.NYSENational,
	4:  enum.FinraADF,
	5:  enum.MarketIndependent,
	6:  enum.ISE,
	7:  enum.CboeEDGA,
	8:  enum.CboeEDGX,
	9:  enum.NYSEChicago,
	10: enum.NYSE,
	11: enum.NYSEArca,
	13: enum.CQS,
	12: enum.Nasdaq,
	14: enum.MEMX,
	15: enum.IEX,
	16: enum.CBSX,
	17: enum.NasdaqOMXPSX,
	18: enum.CboeBYX,
	19: enum.CboeBZX,
}

// ConvertExchangeCode converts a Polygon exchange id to the internal representation
func ConvertExchangeCode(exchange int) enum.Exchange {
	val, ok := exchangeCodeMapping[exchange]
	if !ok {
		val = enum.UndefinedExchange
	}
	return val
}

var tapeCodeMapping = map[int]enum.Tape{
	1: enum.TapeA,
	2: enum.TapeB,
	3: enum.TapeC,
}

// ConvertTapeCode converts between Polygons' TapeID and Marketstore's internal representation
func ConvertTapeCode(tape int) enum.Tape {
	t, ok := tapeCodeMapping[tape]
	if !ok {
		t = enum.UndefinedTape
	}
	return t
}

// TradeConditionMapping provides a mapping from Polygon integer format to Marketstore's internal representation
var TradeConditionMapping = map[int]enum.TradeCondition{
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

// ConvertTradeCondition converts between Polygon trade condition format and Marketstore's internal represention
func ConvertTradeCondition(condition int) enum.TradeCondition {
	val, ok := TradeConditionMapping[condition]
	if !ok {
		val = enum.UnknownTradeCondition
	}
	return val
}

var QuoteConditionMapping = map[int]enum.QuoteCondition{
	// 0:  enum.Regular,
	1: enum.RegularTwoSidedOpen,
	// 2:  enum.RegularOneSidedOpen,
	3: enum.SlowQuoteOfferSide,
	4: enum.SlowQuoteBidSide,
	5: enum.SlowQuoteBidAndOfferSide,
	6: enum.SlowQuoteLRPBidSide,
	7: enum.SlowQuoteLRPOfferSide,
	// 8:  enum.SlowDueNYSELRP,
	9:  enum.SlowQuoteSetSlowList,
	10: enum.ManualAskAutomatedBid,
	11: enum.ManualBidAutomatedAsk,
	12: enum.ManualBidAndAsk,
	13: enum.OpeningQuote,
	14: enum.ClosingQuote,
	15: enum.ClosedQuote,
	//16: enum.Resume,
	17: enum.FastTrading,
	//18: enum.TradingRangeIndication,
	19: enum.MarketMakerQuotesClosed,
	20: enum.NonFirmQuote,
	//21: enum.NewsDissemination,
	22: enum.OrderInflux,
	23: enum.OrderImbalance,
	//24: enum.DueToRelatedSecurityNewsDissemination,
	//25: enum.DueToRelatedSecurityNewsPending,
	//26: enum.AdditionalInformation,
	// 27: enum.NewsPending,
	// 28: enum.AdditionalInformationDueToRelatedSecurity,
	// 29: enum.DueToRelatedSecurity,
	// 30: enum.InViewOfCommon,
	// 31: enum.EquipmentChangeover,
	32: enum.NoOpenNoResume,
	// 33: enum.SubPennyTrading,
	// 34: enum.AutomatedBidNoOfferNoBid,
	// 35: enum.LuldPriceBand,
	// 36: enum.MarketWideCircuitBreakerLevel1,
	// 37: enum.MarketWideCircuitBreakerLevel2,
	// 38: enum.MarketWideCircuitBreakerLevel3,
	// 39: enum.RepublishedLuldPriceBand,
	// 40: enum.OnDemandAuction,
	// 41: enum.CashOnlySettlement,
	// 42: enum.NextDaySettlement,
	// 43: enum.LULDTradingPause,
	// 71: enum.SlowDueLRPBidAsk,
}

// ConvertQuoteCondition converts between Polygon trade condition format and Marketstore's internal represention
func ConvertQuoteCondition(condition int) enum.QuoteCondition {
	val, ok := QuoteConditionMapping[condition]
	if !ok {
		val = enum.UnknownQuoteCondition
	}
	return val
}
