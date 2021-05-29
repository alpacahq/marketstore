package loader

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestParseTime(t *testing.T) {
	t.Parallel()

	tt := time.Date(2016, 12, 30, 21, 59, 20, 383000000, time.UTC)
	var fAdj int
	timeFormat := "20060102 15:04:05"
	dateTime := "20161230 21:59:20 383000"
	tzLoc := time.UTC
	tTest, err := parseTime(timeFormat, dateTime, tzLoc, fAdj)
	assert.Equal(t, err != nil, true)
	formatAdj := len(dateTime) - len(timeFormat)
	tTest, err = parseTime(timeFormat, dateTime, tzLoc, formatAdj)
	assert.Equal(t, tt == tTest, true)
}

func TestParseTimestamp(t *testing.T) {
	t.Parallel()

	tt := time.Date(2017, 11, 07, 07, 8, 23, 383000000, time.UTC)
	var fAdj int
	timeFormat := "timestamp"
	dateTime := "1510038503.383"
	tzLoc := time.UTC
	tTest, err := parseTime(timeFormat, dateTime, tzLoc, fAdj)
	assert.Equal(t, err == nil, true)
	assert.Equal(t, tt == tTest, true)

	tt1 := time.Date(2017, 11, 07, 07, 8, 23, 0, time.UTC)
	dateTime = "1510038503"
	tTest, err = parseTime(timeFormat, dateTime, tzLoc, fAdj)
	assert.Equal(t, err == nil, true)
	assert.Equal(t, tt1 == tTest, true)
}
