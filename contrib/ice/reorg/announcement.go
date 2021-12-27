package reorg

import (
	"strings"
	"time"

	announcement "github.com/alpacahq/marketstore/v4/contrib/ice/enum"
)

type Announcement struct {
	EntryDate               time.Time                     `reorg:"line:0 pos:64-72 format:01/02/06"`
	DealNumber              string                        `reorg:"line:1 pos:6-8"`
	TextNumber              int                           `reorg:"line:1 pos:16-23"`
	Remarks                 string                        `reorg:"line:1 pos:33-45"`
	NotificationType        announcement.NotificationType `reorg:"line:1 pos:53 format:%c"`
	Status                  announcement.StatusCode       `reorg:"line:1 pos:54 format:%c"`
	UpdatedNotificationType announcement.NotificationType `reorg:"line:1 pos:55 format:%c"`
	NrOfOptions             string                        `reorg:"line:1 pos:56"`
	SecurityType            announcement.SecurityType     `reorg:"line:1 pos:57 format:%c"`
	EffectiveDate           time.Time                     `reorg:"line:1 pos:64-72 format:01/02/06"`
	TargetCusip             string                        `reorg:"line:2 pos:7-16"`
	TargetDescription       string                        `reorg:"line:2 pos:19-29"`
	InitiatingCusip         string                        `reorg:"line:2 pos:36-45"`
	InitiatingDescription   string                        `reorg:"line:2 pos:48-58"`
	ExpirationDate          time.Time                     `reorg:"line:2 pos:64-72 format:01/02/06"`
	Cash                    float64                       `reorg:"line:3 pos:5-15"`
	CashCode                string                        `reorg:"line:3 pos:19"`
	StateCode               string                        `reorg:"line:3 pos:30-32"`
	RecordDate              time.Time                     `reorg:"line:3 pos:64-72 format:01/02/06"`
	Rate                    float64                       `reorg:"line:4 pos:6-15"`
	RateCode                string                        `reorg:"line:4 pos:19"`
	VoluntaryMandatoryCode  announcement.ActionCode       `reorg:"line:4 pos:33 format:%c"`
	UpdateTextNumber        int                           `reorg:"func:ParseUpdateTextNumber"`
	DeleteTextNumber        int                           `reorg:"func:ParseDeleteTextNumber"`
	NewRate                 float64                       `reorg:"func:ParseNewRate"`
	OldRate                 float64                       `reorg:"func:ParseOldRate"`
	DueRedemptionDate       time.Time                     `reorg:"func:ParseDueRedemptionDate"`
}

func (i Announcement) Is(code announcement.NotificationType) bool {
	return i.NotificationType == code
}

func (i Announcement) ParseUpdateTextNumber(lines []string) string {
	if strings.TrimSpace(lines[5][54:61]) == "UPDTEXT" {
		return strings.TrimSpace(lines[5][62:69])
	}
	return ""
}

func (i Announcement) ParseDeleteTextNumber(lines []string) string {
	if strings.TrimSpace(lines[5][54:61]) == "DELTEXT" {
		return strings.TrimSpace(lines[5][62:69])
	}
	return ""
}

func (i Announcement) ParseNewRate(lines []string) string {
	if i.Is(announcement.StockSplit) || i.Is(announcement.ReverseStockSplit) {
		return lines[7][56:69]
	}
	return lines[4][5:15]
}

func (i Announcement) ParseOldRate(lines []string) string {
	if i.Is(announcement.StockSplit) || i.Is(announcement.ReverseStockSplit) {
		return lines[8][56:69]
	}
	return lines[4][23:32]
}

func (i Announcement) ParseDueRedemptionDate(lines []string) string {
	if len(lines) > 12 && lines[12][0:25] == "DUE BILL REDEMPTION DATE:" {
		return lines[12][25:33]
	}
	return ""
}
