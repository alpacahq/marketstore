package symbols

import (
	"fmt"
	"github.com/alpacahq/marketstore/contrib/xignitefeeder/api"
	"time"
)

// Manager manages symbols in the target stock exchanges.
// symbol(s) can be newly registered / removed from the exchange,
// so target symbols should be update periodically
type Manager struct {
	APIClient       api.Client
	TargetExchanges []string
	symbols         map[string][]string
}

func NewManager(apiClient api.Client, targetExchanges []string) *Manager {
	return &Manager{APIClient: apiClient, TargetExchanges: targetExchanges, symbols: map[string][]string{}}
}

// getSymbols returns target symbols for a stock exchange
func (m *Manager) getSymbols(exchange string) (symbols []string) {
	return m.symbols[exchange]
}

// GetIdentifiers returns identifiers for the target symbols for all the target exchanges
// identifier = {exchange}.{symbol} (ex. "XTKS.1301")
func (m *Manager) GetAllIdentifiers() (identifiers []string) {
	identifiers = make([]string, 1)
	for _, exchange := range m.TargetExchanges {
		for _, symbol := range m.getSymbols(exchange) {
			identifiers = append(identifiers, fmt.Sprintf("%s.%s", symbol, exchange))
		}
	}
	return identifiers
}

func (m *Manager) UpdateSymbols() {
	for _, exchange := range m.TargetExchanges {
		resp, err := m.APIClient.ListSymbols(exchange)

		// if ListSymbols API returns an error, don't update the target symbols
		if err != nil || resp.Outcome != "Success" {
			fmt.Sprintln("err=%v, API response=%v", err, resp)
			return
		}

		symbols := make([]string, 1)
		for _, securityDescription := range resp.ArrayOfSecurityDescription {
			if securityDescription.Symbol != "" {
				symbols = append(symbols, securityDescription.Symbol)
			}
		}

		// update target symbols for the stock exchange
		m.symbols[exchange] = symbols
	}
}

// UpdateEveryDayAt updates the symbols every day at the specified hour
func (m *Manager) UpdateEveryDayAt(hour int) {
	m.UpdateSymbols()
	time.AfterFunc(timeToNext(hour), m.UpdateSymbols)
}

// timeToNext returns the time duration from now to next {hour}:00:00
// For example, when the current time is 8pm, timeToNext(16) = 20 * time.Hour
func timeToNext(hour int) time.Duration {
	t := time.Now()
	n := time.Date(t.Year(), t.Month(), t.Day(), hour, 0, 0, 0, t.Location())
	if t.After(n) {
		n = n.Add(24 * time.Hour)
	}
	d := n.Sub(t)
	return d
}
