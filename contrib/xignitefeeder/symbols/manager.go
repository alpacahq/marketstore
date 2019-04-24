package symbols

import (
	"fmt"
	"github.com/alpacahq/marketstore/contrib/xignitefeeder/api"
	"github.com/alpacahq/marketstore/utils/log"
)

// Manager manages symbols in the target stock exchanges.
// symbol(s) can be newly registered / removed from the exchange,
// so target symbols should be update periodically
type Manager struct {
	APIClient       api.Client
	TargetExchanges []string
	// identifier = {symbol}.{exchange} (i.e. "7203.XTKS").
	identifiers map[string][]string
}

func NewManager(apiClient api.Client, targetExchanges []string) *Manager {
	return &Manager{APIClient: apiClient, TargetExchanges: targetExchanges, identifiers: map[string][]string{}}
}

// GetIdentifiers returns identifiers for the target symbols for all the target exchanges
// identifier = {exchange}.{symbol} (ex. "XTKS.1301")
func (m *Manager) GetAllIdentifiers() (identifiers []string) {
	identifiers = make([]string, 1)
	for _, exchange := range m.TargetExchanges {
		identifiers = append(identifiers, m.identifiers[exchange]...)

	}
	return identifiers
}

func (m *Manager) UpdateSymbols() {
	for _, exchange := range m.TargetExchanges {
		resp, err := m.APIClient.ListSymbols(exchange)

		// if ListSymbols API returns an error, don't update the target symbols
		if err != nil || resp.Outcome != "Success" {
			log.Warn("err=%v, API response=%v", err, resp)
			return
		}

		identifiers := make([]string, 1)
		for _, securityDescription := range resp.ArrayOfSecurityDescription {
			if securityDescription.Symbol != "" {
				identifier := fmt.Sprintf("%s.%s", securityDescription.Symbol, exchange)
				identifiers = append(identifiers, identifier)
			}
		}

		// update target symbols for the stock exchange
		m.identifiers[exchange] = identifiers
	}
}
