package configs

import (
	"fmt"
	"strings"
)

type Exchange string

const (
	AMEX     = Exchange("AMEX")
	ARCA     = Exchange("ARCA")
	BATS     = Exchange("BATS")
	NYSE     = Exchange("NYSE")
	NASDAQ   = Exchange("NASDAQ")
	NYSEARCA = Exchange("NYSEARCA")
	OTC      = Exchange("OTC")
)

var (
	validExchanges = map[string]struct{}{string(AMEX): {}, string(ARCA): {}, string(BATS): {}, string(NYSE): {},
		string(NASDAQ): {}, string(NYSEARCA): {}, string(OTC): {},
	}
)

func (e *Exchange) Valid() error {
	if _, found := validExchanges[string(*e)]; !found {
		return fmt.Errorf("invalid exchange: %s", *e)
	}
	return nil
}

func (e *Exchange) UnmarshalJSON(s []byte) error {
	*e = Exchange(strings.Trim(string(s), `"`))
	return e.Valid()
}
