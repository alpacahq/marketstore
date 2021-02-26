package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestString(t *testing.T) {
	var tests = []struct {
		sub      Subscription
		expected string
	}{
		{Subscription{}, `"trades":[],"qoutes":[],"bars":[]`},
		{
			Subscription{
				MinuteBarSymbols: []string{"PACA", "APCA"},
				QuoteSymbols:     []string{"AAPL", "*"},
			},
			`"trades":[],"qoutes":["AAPL","*"],"bars":["PACA","APCA"]`,
		},
		{
			Subscription{
				TradeSymbols: []string{"PACA", "APCA"},
				QuoteSymbols: []string{"AAPL", "*"},
			},
			`"trades":["PACA","APCA"],"qoutes":["AAPL","*"],"bars":[]`,
		},
		{
			Subscription{
				MinuteBarSymbols: []string{"AAPL"},
				TradeSymbols:     []string{"AAPL"},
				QuoteSymbols:     []string{"AAPL"},
			},
			`"trades":["AAPL"],"qoutes":["AAPL"],"bars":["AAPL"]`,
		},
	}

	for _, tt := range tests {
		got := tt.sub.String()
		assert.Equal(
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
