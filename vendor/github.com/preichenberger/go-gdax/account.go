package gdax

import (
	"fmt"
)

type Account struct {
	Id        string `json:"id"`
	Balance   string `json:"balance"`
	Hold      string `json:"hold"`
	Available string `json:"available"`
	Currency  string `json:"currency"`
}

// Ledger

type LedgerEntry struct {
	Id        int           `json:"id,number"`
	CreatedAt Time          `json:"created_at,string"`
	Amount    string        `json:"amount"`
	Balance   string        `json:"balance"`
	Type      string        `json:"type"`
	Details   LedgerDetails `json:"details"`
}

type LedgerDetails struct {
	OrderId   string `json:"order_id"`
	TradeId   string `json:"trade_id"`
	ProductId string `json:"product_id"`
}

type GetAccountLedgerParams struct {
	Pagination PaginationParams
}

// Holds

type Hold struct {
	AccountId string `json:"account_id"`
	CreatedAt Time   `json:"created_at,string"`
	UpdatedAt Time   `json:"updated_at,string"`
	Amount    string `json:"amount"`
	Type      string `json:"type"`
	Ref       string `json:"ref"`
}

type ListHoldsParams struct {
	Pagination PaginationParams
}

// Client Funcs
func (c *Client) GetAccounts() ([]Account, error) {
	var accounts []Account
	_, err := c.Request("GET", "/accounts", nil, &accounts)

	return accounts, err
}

func (c *Client) GetAccount(id string) (Account, error) {
	account := Account{}

	url := fmt.Sprintf("/accounts/%s", id)
	_, err := c.Request("GET", url, nil, &account)
	return account, err
}

func (c *Client) ListAccountLedger(id string,
	p ...GetAccountLedgerParams) *Cursor {
	paginationParams := PaginationParams{}
	if len(p) > 0 {
		paginationParams = p[0].Pagination
	}

	return NewCursor(c, "GET", fmt.Sprintf("/accounts/%s/ledger", id),
		&paginationParams)
}

func (c *Client) ListHolds(id string, p ...ListHoldsParams) *Cursor {
	paginationParams := PaginationParams{}
	if len(p) > 0 {
		paginationParams = p[0].Pagination
	}

	return NewCursor(c, "GET", fmt.Sprintf("/accounts/%s/holds", id),
		&paginationParams)
}
