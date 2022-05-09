package symbols

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/alpacahq/marketstore/v4/utils/log"
)

// JSONFileManager is a symbol manager that reads a json file to get the list of symbols.
type JSONFileManager struct {
	httpClient          *http.Client
	stocksJSONURL       string
	stocksJSONBasicAuth string
	symbols             []string
}

// NewJSONFileManager initializes the SymbolManager object with the specified parameters.
func NewJSONFileManager(hc *http.Client, stocksJSONURL, stocksJSONBasicAuth string) *JSONFileManager {
	return &JSONFileManager{
		httpClient:          hc,
		stocksJSONURL:       stocksJSONURL,
		stocksJSONBasicAuth: stocksJSONBasicAuth,
		symbols:             []string{},
	}
}

// GetAllSymbols returns symbols for all the target exchanges.
func (m *JSONFileManager) GetAllSymbols() []string {
	return m.symbols
}

// UpdateSymbols gets a remote json file, store the symbols in the file to the symbols map.
func (m *JSONFileManager) UpdateSymbols() {
	if symbols := m.downloadSymbols(context.Background()); symbols != nil {
		// replace target symbols
		m.symbols = symbols
		log.Debug(fmt.Sprintf("Updated symbols. The number of symbols is %d", len(m.symbols)))
	}
}

type Stocks struct {
	Data map[string]interface{} `json:"data"`
}

func (m *JSONFileManager) downloadSymbols(ctx context.Context) []string {
	req, err := http.NewRequestWithContext(ctx, "GET", m.stocksJSONURL, http.NoBody)
	if err != nil {
		log.Error(fmt.Sprintf("failed to create a http req for stocks Json(URL=%s). err=%v",
			m.stocksJSONURL, err,
		))
		return nil
	}
	// set basic auth
	if m.stocksJSONBasicAuth != "" {
		req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(m.stocksJSONBasicAuth)))
	}

	resp, err := m.httpClient.Do(req)
	if err != nil {
		log.Error("failed to download stocks Json(URL=%s). err=%v",
			m.stocksJSONURL, err,
		)
		return nil
	}
	defer func(Body io.ReadCloser) { _ = Body.Close() }(resp.Body)

	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Error("failed to read body(URL=%s). err=%v",
			m.stocksJSONURL, err,
		)
		return nil
	}

	var s Stocks
	err = json.Unmarshal(b, &s)
	if err != nil {
		log.Error("failed to unmarshal json(URL=%s). err=%v",
			m.stocksJSONURL, err,
		)
		return nil
	}

	symbols := make([]string, len(s.Data))
	i := 0
	for symbol := range s.Data {
		symbols[i] = symbol
		i++
	}
	log.Info(fmt.Sprintf("downloaded a json file(URL=%s), len(symbols)=%d", m.stocksJSONURL, len(symbols)))
	return symbols
}
