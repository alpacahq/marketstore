package symbols

import (
	"fmt"

	"github.com/alpacahq/marketstore/contrib/xignitefeeder/api"
	"github.com/alpacahq/marketstore/utils/log"
)

// Manager manages symbols in the target stock exchanges.
// symbol(s) can be newly registered / removed from the exchange,
// so target symbols should be update periodically
type Manager interface {
	GetAllIdentifiers() []string
}

// ManagerImpl is an implementation of the Manager.
type ManagerImpl struct {
	APIClient       api.Client
	TargetExchanges []string
	// identifier = {symbol}.{exchange} (i.e. "7203.XTKS").
	Identifiers map[string][]string
}

// NewManager initializes the SymbolManager object with the specified parameters.
func NewManager(apiClient api.Client, targetExchanges []string) *ManagerImpl {
	return &ManagerImpl{APIClient: apiClient, TargetExchanges: targetExchanges, Identifiers: map[string][]string{}}
}

// GetAllIdentifiers returns Identifiers for the target symbols for all the target exchanges
// identifier = {exchange}.{symbol} (ex. "XTKS.1301")
func (m ManagerImpl) GetAllIdentifiers() []string {
	var identifiers []string
	for _, exchange := range m.TargetExchanges {
		identifiers = append(identifiers, m.Identifiers[exchange]...)
	}
	return identifiers
}

// UpdateSymbols calls the ListSymbols endpoint, convert the symbols to the Identifiers and store them to the Identifiers map
func (m ManagerImpl) UpdateSymbols() {
	for _, exchange := range m.TargetExchanges {
		resp, err := m.APIClient.ListSymbols(exchange)

		// if ListSymbols API returns an error, don't update the target symbols
		if err != nil || resp.Outcome != "Success" {
			log.Warn("err=%v, API response=%v", err, resp)
			return
		}

		// convert the symbol strings (i.e. "1234") to the identifier strings (i.e. "1234.XTKS") and store them to the map
		var identifiers []string
		for _, securityDescription := range resp.ArrayOfSecurityDescription {
			if securityDescription.Symbol != "" {
				identifier := fmt.Sprintf("%s.%s", securityDescription.Symbol, exchange)
				identifiers = append(identifiers, identifier)
			}
		}

		// update target symbols for the stock exchange
		m.Identifiers[exchange] = identifiers
		log.Debug(fmt.Sprintf("Updated symbols. The number of symbols in %s is %d", exchange, len(m.Identifiers[exchange])))
	}
}
