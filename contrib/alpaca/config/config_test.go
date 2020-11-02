package config

import (
	"testing"

	"github.com/alpacahq/marketstore/v4/contrib/alpaca/enums"
	"github.com/stretchr/testify/assert"
)

func TestAsCanonical(t *testing.T) {
	agg := string(enums.Agg)
	q := string(enums.Quote)
	tr := string(enums.Trade)
	var tests = []struct {
		sub      Subscription
		expected []string
	}{
		{Subscription{}, []string{}},
		{
			Subscription{
				MinuteBarSymbols: []string{"PACA", "APCA"},
				QuoteSymbols:     []string{"AAPL", "*"},
			},
			[]string{agg + "PACA", q + "*", agg + "APCA"},
		},
		{
			Subscription{
				MinuteBarSymbols: []string{"AAPL"},
				TradeSymbols:     []string{"AAPL"},
				QuoteSymbols:     []string{"AAPL"},
			},
			[]string{agg + "AAPL", q + "AAPL", tr + "AAPL"},
		},
	}

	for _, tt := range tests {
		got := tt.sub.AsCanonical()
		assert.ElementsMatchf(
			t,
			tt.expected,
			got,
			"s=%+v, got=%q, expected=%q",
			tt.sub,
			got,
			tt.expected,
		)
	}
}

func TestConcat(t *testing.T) {
	var tests = []struct {
		lists    [][]string
		expected []string
	}{
		{[][]string{}, []string{}},
		{[][]string{{}, {}, {}, {}}, []string{}},
		{[][]string{{"A"}}, []string{"A"}},
		{[][]string{{"A", "A"}}, []string{"A", "A"}},
		{[][]string{{"A", ""}, {"P"}}, []string{"A", "", "P"}},
		{[][]string{{}, {}, {"A"}}, []string{"A"}},
	}

	for _, tt := range tests {
		got := concat(tt.lists...)
		assert.ElementsMatchf(
			t,
			tt.expected,
			got,
			"lists=%q, got=%q, expected=%q",
			tt.lists,
			got,
			tt.expected,
		)
	}
}

func TestPrefixStrings(t *testing.T) {
	agg := string(enums.Agg)
	var tests = []struct {
		list     []string
		prefix   enums.Prefix
		expected []string
	}{
		{[]string{}, enums.Agg, []string{}},
		{[]string{"A", "P", "C", ""}, enums.Agg, []string{agg + "A", agg + "P", agg + "C", agg}},
	}

	for _, tt := range tests {
		got := prefixStrings(tt.list, tt.prefix)
		assert.ElementsMatchf(
			t,
			tt.expected,
			got,
			"list=%q, prefix=%q, got=%q, expected=%q",
			tt.list,
			tt.prefix,
			got,
			tt.expected,
		)
	}
}

func TestNormalizeSubscriptions(t *testing.T) {
	var tests = []struct {
		list     []string
		expected []string
	}{
		{[]string{}, []string{}},
		{[]string{""}, []string{""}},
		{[]string{"", "A"}, []string{"", "A"}},
		{[]string{"", "A*"}, []string{"*"}},
		{[]string{"*", "AAPL"}, []string{"*"}},
	}

	for _, tt := range tests {
		got := normalizeSubscriptions(tt.list)
		assert.ElementsMatchf(
			t,
			tt.expected,
			got,
			"list=%q, got=%q, expected=%q",
			tt.list,
			got,
			tt.expected,
		)
	}
}

func TestContainsWildcard(t *testing.T) {
	var tests = []struct {
		list     []string
		expected bool
	}{
		{[]string{}, false},
		{[]string{""}, false},
		{[]string{"", "A"}, false},
		{[]string{`!@#$%^&()_+-=~[]{}|\\:",./<>?'`, "A"}, false},
		{[]string{"*", "AAPL"}, true},
		{[]string{"APCA", "A*"}, true},
	}

	for _, tt := range tests {
		got := containsWildcard(tt.list)
		assert.Equalf(
			t,
			tt.expected,
			got,
			"list=%q, got=%t, expected=%t",
			tt.list,
			got,
			tt.expected,
		)
	}
}
