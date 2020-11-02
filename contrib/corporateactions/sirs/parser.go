package sirs

import (
	"bufio"
	"io"
	"strconv"
	"strings"

	"github.com/alpacahq/marketstore/v4/contrib/corporateactions/models"
	"github.com/alpacahq/marketstore/v4/contrib/corporateactions/lib/date"
	"github.com/alpacahq/marketstore/v4/utils/log"

	"github.com/pkg/errors"
)

const charactersPerLine = 80
const minLines = 6

type loader struct {
	stg         *models.SecurityMaster
	records 	[]*models.SecurityMaster
	processDate *date.Date
}


func Load(r io.Reader) ([]*models.SecurityMaster, error) {

	l := &loader{}

	//read file
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		l.lineReader(scanner.Text())
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	log.Info("security master was loaded")
	
	return l.records, nil
}

func (l *loader) lineReader(currentLine string) error {
	var err error
	line := string(currentLine)
	recType := line[13:15]

	// file header
	if recType == "H0" {
		if l.processDate, err = parseDate(line[18:26]); err != nil {
			return errors.Wrap(err, "cannot parse process date from header")
		}

		l.stg = &models.SecurityMaster{}
		return nil
	}

	cusip := strings.TrimSpace(line[0:9])

	// new record, when cusip in line changed means it is a new record
	if l.stg.Cusip != cusip {
		// save current record
		if l.stg.Cusip != "" {
			l.saveRecord()
		}

		// create new record
		l.stg = &models.SecurityMaster{
			Cusip: cusip,
		}
	}

	if err = l.parseEntry(recType, line); err != nil {
		return err
	}

	return nil
}

func (l *loader) saveRecord() error {
	//set listing exchange values
	if l.stg.ExchangeCode1 != "" && l.stg.ExchangeCode1 == l.stg.ExchangeCode {
		l.stg.ListingExchangeCode = l.stg.ExchangeCode1
		l.stg.ListingExchangeDate = l.stg.EffectiveDate1
		l.stg.ListingExchangeStatusCode = l.stg.StatusCode1
		l.stg.ListingExchangeTicker = l.stg.Ticker1
	} else if l.stg.ExchangeCode2 != "" && (l.stg.ExchangeCode2 == l.stg.ExchangeCode || l.stg.ExchangeCode2 == "29") {
		l.stg.ListingExchangeCode = l.stg.ExchangeCode2
		l.stg.ListingExchangeDate = l.stg.EffectiveDate2
		l.stg.ListingExchangeStatusCode = l.stg.StatusCode2
		l.stg.ListingExchangeTicker = l.stg.Ticker2
	} else if l.stg.ExchangeCode1 != "" {
		l.stg.ListingExchangeCode = l.stg.ExchangeCode1
		l.stg.ListingExchangeDate = l.stg.EffectiveDate1
		l.stg.ListingExchangeStatusCode = l.stg.StatusCode1
		l.stg.ListingExchangeTicker = l.stg.Ticker1
	} else {
		l.stg.ListingExchangeCode = l.stg.ExchangeCode2
		l.stg.ListingExchangeDate = l.stg.EffectiveDate2
		l.stg.ListingExchangeStatusCode = l.stg.StatusCode2
		l.stg.ListingExchangeTicker = strings.Replace(l.stg.Ticker2, "-", ".PR", 1)
	}

	//set symbol
	l.stg.Symbol = l.stg.ListingExchangeTicker
	if l.stg.TickerSymbolExt == "W" {
		l.stg.Symbol = l.stg.Symbol + ".W"
	}

	// Replace space with "."
	l.stg.Symbol = strings.Replace(l.stg.Symbol, " ", ".", -1)
	l.stg.Symbol = strings.Replace(l.stg.Symbol, "W.W", ".W", -1)

	// use cusip as original cusip; if orig cusip is blank, it means the symbol/cusip never change
	if l.stg.OriginalIdentifier == "" {
		l.stg.OriginalIdentifier = l.stg.Cusip
	}

	l.records = append(l.records, l.stg)

	return nil
}

func (l *loader) parseEntry(recType, line string) error {
	var err error

	switch recType {
	case "A0":
		l.stg.IdentifierTransactionCode = strings.TrimSpace(line[39:41])
		if l.stg.IdentifierTransactionDate, err = parseDate(line[41:49]); err != nil {
			return errors.Wrap(err, "cannot parse identifier transaction date from "+recType)
		}
		l.stg.ChangeIndicator = strings.TrimSpace(line[49:50])
		l.stg.AssetCategory = strings.TrimSpace(line[51:52])

	case "A1":
		l.stg.PreviousIdentifier = strings.TrimSpace(line[18:30])
		l.stg.PreviousIdentifierMarker = strings.TrimSpace(line[30:31])
		if l.stg.PreviousIdentifierDate, err = parseDate(line[31:39]); err != nil {
			return errors.Wrap(err, "cannot parse previous identifier date from "+recType)
		}
		l.stg.NextIdentifier = strings.TrimSpace(line[49:61])
		if l.stg.NextIdentifierDate, err = parseDate(line[62:70]); err != nil {
			return errors.Wrap(err, "cannot parse next identifier date from "+recType)
		}

	case "D0":
		l.stg.IssueDescription = strings.TrimSpace(line[18:80])

	case "D1":
		l.stg.CountryOfIssuer = strings.TrimSpace(line[18:21])
		l.stg.CountryOfIssuerIncorporation = strings.TrimSpace(line[71:73])
		l.stg.SecurityType = strings.TrimSpace(line[21:24])
		l.stg.CollateralOfIssuer = strings.TrimSpace(line[24:26])
		l.stg.SicCodeOfIssuer = strings.TrimSpace(line[26:30])
		l.stg.ExchangeCode = strings.TrimSpace(line[32:35])
		l.stg.PrimaryTickerSymbol = strings.TrimSpace(line[35:39])
		l.stg.TickerSymbolExt = strings.TrimSpace(line[39:43])
		l.stg.CurrentPaymentFrequency = strings.TrimSpace(line[43:45])
		l.stg.TaxStatus = strings.TrimSpace(line[65:66])
		l.stg.DtcEligibility = strings.TrimSpace(line[66:67])
		l.stg.NsccEligibility = strings.TrimSpace(line[67:68])
		l.stg.GicsClassificationIndicator = strings.TrimSpace(line[69:70])
		l.stg.IssuerClassificationIndicator = strings.TrimSpace(line[70:71])
		l.stg.MarginSecurityCode = strings.TrimSpace(line[76:77])
		l.stg.IssuerTypeCode = strings.TrimSpace(line[77:78])
		l.stg.NasdaqTierCode = strings.TrimSpace(line[78:79])

	case "D2":
		l.stg.ActiveFlag = strings.TrimSpace(line[58:59])
		l.stg.TradingStatus = strings.TrimSpace(line[59:60])
		l.stg.IssueStatus = strings.TrimSpace(line[61:62])
		l.stg.OriginalIdentifier = strings.TrimSpace(line[64:76])
		l.stg.OriginalIdentifierMarker = strings.TrimSpace(line[76:77])

	case "D3":
		if l.stg.SharesOutstanding, err = strconv.ParseInt(strings.TrimSpace(line[20:34]), 10, 64); err != nil {
			return errors.Wrap(err, "cannot parse shares outstanding from "+recType)
		}
		l.stg.SharesOutstandingMarker = strings.TrimSpace(line[36:37])
		if l.stg.SharesOutstandingDate, err = parseDate(line[37:45]); err != nil {
			return errors.Wrap(err, "cannot parse shares outstanding date from "+recType)
		}
		l.stg.SharesOutstandingChangeFlag = strings.TrimSpace(line[46:47])

	case "E0":
		l.stg.IndicatedDividendFootnote = strings.TrimSpace(line[32:33])
		if l.stg.IndicatedDividendDate, err = parseDate(line[33:41]); err != nil {
			return errors.Wrap(err, "cannot parse indicated dividend date from "+recType)
		}
		l.stg.IndicatedDividend = strings.TrimSpace(line[41:52])
		l.stg.EtfIndicator = strings.TrimSpace(line[53:54])
		l.stg.IndicatedDividendScale = strings.TrimSpace(line[54:55])
		l.stg.DepositoryIndicator = strings.TrimSpace(line[72:73])
		l.stg.VotingRightsIndicator = strings.TrimSpace(line[73:74])

	case "T1":
		l.stg.ExchangeCode1 = strings.TrimSpace(line[18:21])
		if l.stg.EffectiveDate1, err = parseDate(line[21:29]); err != nil {
			return errors.Wrap(err, "cannot parse effective date1 from "+recType)
		}
		l.stg.StatusCode1 = strings.TrimSpace(line[29:30])
		l.stg.Ticker1 = strings.TrimSpace(line[30:51])

	case "T2":
		l.stg.ExchangeCode2 = strings.TrimSpace(line[18:21])
		if l.stg.EffectiveDate2, err = parseDate(line[21:29]); err != nil {
			return errors.Wrap(err, "cannot parse effective date2 from "+recType)
		}
		l.stg.StatusCode2 = strings.TrimSpace(line[29:30])
		l.stg.Ticker2 = strings.TrimSpace(line[30:42])
	}

	return nil
}

func parseDate(value string) (*date.Date, error) {
	if value == "00000000" {
		return nil, nil
	}
	d, err := date.Parse("20060102", value)
	return &d, err
}
