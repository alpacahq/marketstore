package gdax

import (
	"fmt"
)

type Cursor struct {
	Client     *Client
	Pagination *PaginationParams
	Method     string
	Params     interface{}
	URL        string
	HasMore    bool
}

func NewCursor(client *Client, method, url string,
	paginationParams *PaginationParams) *Cursor {
	return &Cursor{
		Client:     client,
		Method:     method,
		URL:        url,
		Pagination: paginationParams,
		HasMore:    true,
	}
}

func (c *Cursor) Page(i interface{}, direction string) error {
	url := c.URL
	if c.Pagination.Encode(direction) != "" {
		url = fmt.Sprintf("%s?%s", c.URL, c.Pagination.Encode(direction))
	}

	res, err := c.Client.Request(c.Method, url, c.Params, i)
	if err != nil {
		c.HasMore = false
		return err
	}

	c.Pagination.Before = res.Header.Get("CB-BEFORE")
	c.Pagination.After = res.Header.Get("CB-AFTER")

	if c.Pagination.Done(direction) {
		c.HasMore = false
	}

	return nil
}

func (c *Cursor) NextPage(i interface{}) error {
	return c.Page(i, "next")
}

func (c *Cursor) PrevPage(i interface{}) error {
	return c.Page(i, "prev")
}
