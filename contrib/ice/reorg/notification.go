package reorg

import (
	"strings"
	"time"

	"github.com/alpacahq/marketstore/v4/contrib/ice/enum"
)

type Notification struct {
	EntryDate               time.Time `reorg:"line:0 pos:64-72 format:01/02/06"`
	DealNumber              string    `reorg:"line:1 pos:6-8"`
	TextNumber              int64     `reorg:"line:1 pos:16-23"`
	Remarks                 string    `reorg:"line:1 pos:33-45"`
	NotificationType        string    `reorg:"line:1 pos:53"`
	Status                  string    `reorg:"line:1 pos:54"`
	UpdatedNotificationType string    `reorg:"line:1 pos:55"`
	NrOfOptions             string    `reorg:"line:1 pos:56"`
	SecurityType            string    `reorg:"line:1 pos:57"`
	EffectiveDate           time.Time `reorg:"line:1 pos:64-72 format:01/02/06"`
	TargetCusip             string    `reorg:"line:2 pos:7-16"`
	TargetDescription       string    `reorg:"line:2 pos:19-29"`
	InitiatingCusip         string    `reorg:"line:2 pos:36-45"`
	InitiatingDescription   string    `reorg:"line:2 pos:48-58"`
	ExpirationDate          time.Time `reorg:"line:2 pos:64-72 format:01/02/06"`
	Cash                    float64   `reorg:"line:3 pos:5-15"`
	CashCode                string    `reorg:"line:3 pos:19"`
	StateCode               string    `reorg:"line:3 pos:30-32"`
	RecordDate              time.Time `reorg:"line:3 pos:64-72 format:01/02/06"`
	Rate                    float64   `reorg:"line:4 pos:6-15"`
	RateCode                string    `reorg:"line:4 pos:19"`
	VoluntaryMandatoryCode  string    `reorg:"line:4 pos:33"`
	UpdateTextNumber        int64     `reorg:"func:ParseUpdateTextNumber"`
	DeleteTextNumber        int64     `reorg:"func:ParseDeleteTextNumber"`
	NewRate                 float64   `reorg:"func:ParseNewRate"`
	OldRate                 float64   `reorg:"func:ParseOldRate"`
	DueRedemptionDate       time.Time `reorg:"func:ParseDueRedemptionDate"`
}

func (i Notification) Is(code byte) bool {
	return i.NotificationType[0] == code
}

func (i Notification) ParseUpdateTextNumber(lines []string) string {
	if strings.TrimSpace(lines[5][54:61]) == "UPDTEXT" {
		return strings.TrimSpace(lines[5][62:69])
	}
	return ""
}

func (i Notification) ParseDeleteTextNumber(lines []string) string {
	if strings.TrimSpace(lines[5][54:61]) == "DELTEXT" {
		return strings.TrimSpace(lines[5][62:69])
	}
	return ""
}

func (i Notification) ParseNewRate(lines []string) string {
	if i.Is(enum.StockSplit) || i.Is(enum.ReverseStockSplit) {
		return lines[7][56:69]
	}
	return lines[4][5:15]
}

func (i Notification) ParseOldRate(lines []string) string {
	if i.Is(enum.StockSplit) || i.Is(enum.ReverseStockSplit) {
		return lines[8][56:69]
	}
	return lines[4][23:32]
}

func (i Notification) ParseDueRedemptionDate(lines []string) string {
	if len(lines) > 12 && lines[12][0:25] == "DUE BILL REDEMPTION DATE:" {
		return lines[12][25:33]
	}
	return ""
}
