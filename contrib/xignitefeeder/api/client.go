package api

import (
	"encoding/json"
	"fmt"
	"github.com/alpacahq/marketstore/utils/log"
	"github.com/pkg/errors"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	GetQuotesURL = "https://api.marketdata-cloud.quick-co.jp/QUICKEquityRealTime.json/GetQuotes"
	// ?IdentifierType=Symbol&Identifiers=6501.XTKS,7751.XTKS&_Language=English"
	ListSymbolsURL = "https://api.marketdata-cloud.quick-co.jp/QUICKEquityRealTime.json/ListSymbols"
	// ?Exchange=XJAS&_Language=Japanese
)

type Client interface {
	GetRealTimeQuotes(identifiers []string) (GetQuotesResponse, error)
	ListSymbols(exchange string) (ListSymbolsResponse, error)
}

type GetQuotesResponse struct {
	ArrayOfEquityQuote []EquityQuote `json:"ArrayOfEquityQuote"`
}

type EquityQuote struct {
	Outcome  string   `json:"Outcome"`
	Security Security `json:"Security"`
	Quote    Quote    `json:"Quote"`
}

type Security struct {
	Symbol string `json:"Symbol"`
}

type Quote struct {
	DateTime string
	Ask      float32
	Bid      float32
}

type ListSymbolsResponse struct {
	Outcome                    string                `json:"Outcome"`
	ArrayOfSecurityDescription []SecurityDescription `json:"ArrayOfSecurityDescription"`
}

type SecurityDescription struct {
	Symbol string `json:"Symbol"`
}

func NewDefaultAPIClient(token string, timeoutSec int) *DefaultClient {
	return &DefaultClient{
		httpClient: &http.Client{Timeout: time.Duration(timeoutSec) * time.Second},
		token:      token,
	}
}

type DefaultClient struct {
	httpClient *http.Client
	token      string
}

func (c *DefaultClient) createFormData(identifiers []string) url.Values {
	return url.Values{
		"_Language":      {"English"},
		"IdentifierType": {"Symbol"},
		"_token":         {c.token},
		"Identifiers":    {strings.Join(identifiers, ",")},
	}
}

// GetRealTimeQuotes calls GetQuotes endpoint of Xignite API with specified identifiers
//// and returns the parsed API response
// https://www.marketdata-cloud.quick-co.jp/Products/QUICKEquityRealTime/Overview/GetQuotes
func (c *DefaultClient) GetRealTimeQuotes(identifiers []string) (GetQuotesResponse, error) {
	var response GetQuotesResponse

	formData := c.createFormData(identifiers)
	resp, err := http.PostForm(GetQuotesURL, formData)
	if err != nil {
		return response, errors.Wrap(err, fmt.Sprintf("failed to execute GetQuotes request. url=%v, formdata=", GetQuotesURL, formData))
	}
	defer func() {
		if cerr := resp.Body.Close(); err == nil {
			err = errors.Wrap(cerr, fmt.Sprintf("failed to close GetQuotes response. resp=%v", resp))
		}
	}()

	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return response, errors.Wrap(err, fmt.Sprintf("failed to read GetQuotes response body. resp=%v", resp))
	}
	log.Debug("response from Xignite = " + string(b))

	err = json.Unmarshal(b, &response)
	if err != nil {
		return response, errors.Wrap(err, fmt.Sprintf("failed to json_parse GetQuotes response body. resp.Body=%v", string(b)))
	}

	return response, nil
}

// ListSymbols calls ListSymbols endpoint of Xignite API with a specified exchange
// and returns the parsed API response
// https://www.marketdata-cloud.quick-co.jp/Products/QUICKEquityRealTime/Overview/ListSymbols
// exchange: XTKS, XNGO, XSAP, XFKA, XJAS, XTAM
func (c *DefaultClient) ListSymbols(exchange string) (ListSymbolsResponse, error) {
	var response ListSymbolsResponse

	apiUrl := ListSymbolsURL + fmt.Sprintf("?_token=%s&Exchange=%s", c.token, exchange)
	resp, err := http.Get(apiUrl)
	if err != nil {
		return response, errors.Wrap(err, fmt.Sprintf("failed to execute ListSymbols request. url=%v, ", apiUrl))
	}
	defer func() {
		if cerr := resp.Body.Close(); err == nil {
			err = errors.Wrap(cerr, fmt.Sprintf("failed to close ListSymbols response. resp=%v", resp))
		}
	}()

	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return response, errors.Wrap(err, fmt.Sprintf("failed to read ListSymbols response body. resp=%v", resp))
	}
	log.Debug("response from Xignite = " + string(b))

	err = json.Unmarshal(b, &response)
	if err != nil {
		return response, errors.Wrap(err, fmt.Sprintf("failed to json_parse ListSymbols response body. resp.Body=%v", string(b)))
	}

	return response, nil
}
