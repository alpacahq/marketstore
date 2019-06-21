package gdax

import (
	"fmt"
	"time"
)

type ReportParams struct {
	StartDate time.Time
	EndDate   time.Time
}

type CreateReportParams struct {
	Start time.Time
	End   time.Time
}

type Report struct {
	Id          string       `json:"id"`
	Type        string       `json:"type"`
	Status      string       `json:"status"`
	CreatedAt   Time         `json:"created_at,string"`
	CompletedAt Time         `json:"completed_at,string,"`
	ExpiresAt   Time         `json:"expires_at,string"`
	FileURL     string       `json:"file_url"`
	Params      ReportParams `json:"params"`
	StartDate   time.Time
	EndDate     time.Time
}

func (c *Client) CreateReport(newReport *Report) (Report, error) {
	var savedReport Report

	url := fmt.Sprintf("/reports")
	_, err := c.Request("POST", url, newReport, &savedReport)

	return savedReport, err
}

func (c *Client) GetReportStatus(id string) (Report, error) {
	report := Report{}

	url := fmt.Sprintf("/reports/%s", id)
	_, err := c.Request("GET", url, nil, &report)

	return report, err
}
