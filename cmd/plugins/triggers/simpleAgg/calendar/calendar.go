// Package calendar provides market calendar, with which you can
// check if the market is open at specific point of time.
// Though the package is generalized to support different market
// calendars, only the NASDAQ is implemented at this moment.
// You can create your own calendar if you provide the calendar
// json string.  See nasdaq.go for the format.
package calendar

import (
	"encoding/json"
	"strconv"
	"strings"
	"time"
)

type MarketState int

const (
	Closed MarketState = iota
	EarlyClose
)

type Time struct {
	hour, minute, second int
}

type Calendar struct {
	days           map[int]MarketState
	tz             *time.Location
	openTime       Time
	closeTime      Time
	earlyCloseTime Time
}

type calendarJson struct {
	NonTradingDays []string `json:"non_trading_days"`
	EarlyCloses    []string `json:"early_closes"`
	Timezone       string   `json:"timezone"`
	OpenTime       string   `json:"open_time"`
	CloseTime      string   `json:"close_time"`
	EarlyCloseTime string   `json:"early_close_time"`
}

// Nasdaq implements market calendar for the NASDAQ.
var Nasdaq *Calendar = New(NasdaqJson)

func jd(t time.Time) int {
	// Note: Date() is faster than calling Hour(), Month(), and Day() separately
	i, m, k := t.Date()
	j := int(m)
	return k - 32075 +
		1461*(i+4800+(j-14)/12)/4 +
		367*(j-2-(j-14)/12*12)/12 -
		3*((i+4900+(j-14)/12)/100)/4
}

func ParseTime(tstr string) Time {
	seps := strings.Split(tstr, ":")
	h, _ := strconv.Atoi(seps[0])
	m, _ := strconv.Atoi(seps[1])
	s, _ := strconv.Atoi(seps[2])
	return Time{h, m, s}
}

func New(calendarJSON string) *Calendar {
	cal := Calendar{days: map[int]MarketState{}}
	cmap := calendarJson{}
	json.Unmarshal([]byte(calendarJSON), &cmap)
	for _, dateString := range cmap.NonTradingDays {
		t, _ := time.Parse("2006-01-02", dateString)
		cal.days[jd(t)] = Closed
	}
	for _, dateString := range cmap.EarlyCloses {
		t, _ := time.Parse("2006-01-02", dateString)
		cal.days[jd(t)] = EarlyClose
	}
	cal.tz, _ = time.LoadLocation(cmap.Timezone)
	cal.openTime = ParseTime(cmap.OpenTime)
	cal.closeTime = ParseTime(cmap.CloseTime)
	cal.earlyCloseTime = ParseTime(cmap.EarlyCloseTime)
	return &cal
}

// IsMarketDay check if today is a trading day or not.
func (calendar *Calendar) IsMarketDay(t time.Time) bool {
	if t.Weekday() == time.Saturday || t.Weekday() == time.Sunday {
		return false
	}
	if state, ok := calendar.days[jd(t)]; ok {
		return state != Closed
	}
	return true
}

// IsMarketOpen returns true if t is in the market hours
func (calendar *Calendar) IsMarketOpen(t time.Time) bool {
	wd := t.Weekday()
	if wd == time.Saturday || wd == time.Sunday {
		return false
	}

	year, month, day := t.Date()
	ot := calendar.openTime
	open := time.Date(year, month, day, ot.hour, ot.minute, ot.second, 0, calendar.tz)
	if state, ok := calendar.days[jd(t)]; ok {
		switch state {
		case EarlyClose:
			et := calendar.earlyCloseTime
			close := time.Date(year, month, day, et.hour, et.minute, et.second, 0, calendar.tz)
			if t.Before(open) || t.Equal(close) || t.After(close) {
				return false
			} else {
				return true
			}
		case Closed:
			fallthrough
		default:
			return false
		}
	} else {
		ct := calendar.closeTime
		close := time.Date(year, month, day, ct.hour, ct.minute, ct.second, 0, calendar.tz)
		if t.Before(open) || t.Equal(close) || t.After(close) {
			return false
		} else {
			return true
		}
	}
}
