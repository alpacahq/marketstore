package sirs

import (
	"github.com/alpacahq/marketstore/v4/contrib/ice/lib/date"
)

//SecurityMaster is model for database column mapping.
type SecurityMaster struct {
	Symbol                        string
	Cusip                         string
	ProcessDate                   *date.Date
	DateAdded                     *date.Date
	IdentifierTransactionCode     string
	IdentifierTransactionDate     *date.Date
	ChangeIndicator               string
	AssetCategory                 string
	PreviousIdentifier            string
	PreviousIdentifierMarker      string
	PreviousIdentifierDate        *date.Date
	NextIdentifier                string
	NextIdentifierDate            *date.Date
	IssueDescription              string
	CountryOfIssuer               string
	SecurityType                  string
	CollateralOfIssuer            string
	SicCodeOfIssuer               string
	ExchangeCode                  string
	PrimaryTickerSymbol           string
	TickerSymbolExt               string
	CurrentPaymentFrequency       string
	TaxStatus                     string
	DtcEligibility                string
	NsccEligibility               string
	GicsClassificationIndicator   string
	IssuerClassificationIndicator string
	MarginSecurityCode            string
	IssuerTypeCode                string
	NasdaqTierCode                string
	ActiveFlag                    string
	TradingStatus                 string
	IssueStatus                   string
	OriginalIdentifier            string
	OriginalIdentifierMarker      string
	SharesOutstanding             int64
	SharesOutstandingMarker       string
	SharesOutstandingDate         *date.Date
	SharesOutstandingChangeFlag   string
	IndicatedDividendFootnote     string
	IndicatedDividendDate         *date.Date
	IndicatedDividend             string
	EtfIndicator                  string
	IndicatedDividendScale        string
	DepositoryIndicator           string
	VotingRightsIndicator         string
	ExchangeCode1                 string
	EffectiveDate1                *date.Date
	StatusCode1                   string
	Ticker1                       string
	ExchangeCode2                 string
	EffectiveDate2                *date.Date
	StatusCode2                   string
	Ticker2                       string
	ListingExchangeCode           string
	ListingExchangeDate           *date.Date
	ListingExchangeStatusCode     string
	ListingExchangeTicker         string
	Country                       string
	CountryOfIssuerIncorporation  string
	Exchange                      string
}
