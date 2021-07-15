package models

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/alpacahq/marketstore/v4/models/enum"

	"github.com/alpacahq/marketstore/v4/utils"
)

func TestFromTradesFieldExcludes(t *testing.T) {
	t.Parallel()
	// Given a set of trades including ones with condition
	// which should be excluded from the volume roll-up
	symbol := "TEST_TICK_TO_BAR_FIELD_EXCLUDES"
	trades := NewTrade(symbol, 10)
	trades.Add(
		time.Date(2020, 11, 20, 10, 3, 0, 0, utils.InstanceConfig.Timezone).Unix(), 1,
		100.1, 10, enum.NYSEAmerican, enum.TapeA, enum.RegularSale,
	)
	trades.Add( // Odd-lot is excluded from high/low & last
		time.Date(2020, 11, 20, 10, 3, 1, 0, utils.InstanceConfig.Timezone).Unix(), 2,
		111.2, 11, enum.Nasdaq, enum.TapeA, enum.OddLotTrade,
	)
	trades.Add( // Corrected Consolidated Close is excluded from volume
		time.Date(2020, 11, 20, 10, 4, 2, 0, utils.InstanceConfig.Timezone).Unix(), 3,
		100.2, 12, enum.NYSEAmerican, enum.TapeA, enum.RegularSale, enum.CorrectedConsolidatedClose,
	)
	trades.Add( // DerivativelyPriced is excluded from last
		time.Date(2020, 11, 20, 10, 4, 3, 0, utils.InstanceConfig.Timezone).Unix(), 4,
		99.6, 13, enum.NYSE, enum.TapeA, enum.DerivativelyPriced,
	)

	// When converted to bars
	bars := FromTrades(trades, symbol, "1Min")

	assert.NotNil(t, bars)
	assert.Len(t, bars.Epoch, 2)
	assert.Equal(t, bars.Epoch[0],
		time.Date(2020, 11, 20, 10, 3, 0, 0, utils.InstanceConfig.Timezone).Unix())
	assert.Equal(t, bars.Open[0], enum.Price(100.1))
	assert.Equal(t, bars.Close[0], enum.Price(100.1))
	assert.Equal(t, bars.High[0], enum.Price(100.1))
	assert.Equal(t, bars.Low[0], enum.Price(100.1))
	assert.Equal(t, bars.Volume[0], enum.Size(21))

	// Then the second bar will exclude the last & volume update for affected trades
	assert.Equal(t, bars.Epoch[1],
		time.Date(2020, 11, 20, 10, 4, 0, 0, utils.InstanceConfig.Timezone).Unix())
	assert.Equal(t, bars.Open[1], enum.Price(100.2))
	assert.Equal(t, bars.Close[1], enum.Price(100.2))
	assert.Equal(t, bars.High[1], enum.Price(100.2))
	assert.Equal(t, bars.Low[1], enum.Price(99.6))
	assert.Equal(t, bars.Volume[1], enum.Size(13))
}

func TestFromTradesDailyRollup(t *testing.T) {
	t.Parallel()
	// Given a set of trades including ones with condition
	// which signals end of day price
	symbol := "TEST_TICK_TO_BAR_DAILY_ROLLUP"
	trades := NewTrade(symbol, 10)
	trades.Add(
		time.Date(2020, 11, 20, 10, 3, 0, 0, utils.InstanceConfig.Timezone).Unix(), 1,
		100.1, 10, enum.NYSEAmerican, enum.TapeA, enum.RegularSale,
	)
	trades.Add(
		time.Date(2020, 11, 20, 10, 3, 1, 0, utils.InstanceConfig.Timezone).Unix(), 2,
		111.2, 11, enum.Nasdaq, enum.TapeA,
	)
	trades.Add(
		time.Date(2020, 11, 20, 10, 4, 2, 0, utils.InstanceConfig.Timezone).Unix(), 3,
		100.2, 12, enum.NYSEAmerican, enum.TapeA, enum.RegularSale,
	)
	trades.Add( // MarketCenterOfficialClose should be the last for daily bars
		time.Date(2020, 11, 20, 10, 4, 3, 0, utils.InstanceConfig.Timezone).Unix(), 4,
		105.6, 130, enum.NYSE, enum.TapeA, enum.MarketCenterOfficialClose,
	)
	trades.Add( // After hours trade
		time.Date(2020, 11, 20, 10, 4, 3, 0, utils.InstanceConfig.Timezone).Unix(), 4,
		105.8, 31, enum.NYSE, enum.TapeA, enum.FormT,
	)

	// When converted to bars
	bars := FromTrades(trades, symbol, "1D")

	// Then the daily close price should match to the specified
	assert.NotNil(t, bars)
	assert.Len(t, bars.Epoch, 1)
	assert.Equal(t, bars.Epoch[0],
		time.Date(2020, 11, 20, 0, 0, 0, 0, utils.InstanceConfig.Timezone).Unix())
	assert.Equal(t, bars.Open[0], enum.Price(100.1))
	assert.Equal(t, bars.Close[0], enum.Price(105.6))
	assert.Equal(t, bars.High[0], enum.Price(111.2))
	assert.Equal(t, bars.Low[0], enum.Price(100.1))
	assert.Equal(t, bars.Volume[0], enum.Size(130))
}
