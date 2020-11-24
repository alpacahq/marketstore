package models

import (
	"github.com/alpacahq/marketstore/v4/models/enum"
	"testing"
	"time"

	"github.com/alpacahq/marketstore/v4/utils"
	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

var _ = Suite(&TestSuite{})

type TestSuite struct{}

func (t *TestSuite) TestFromTradesFieldExcludes(c *C) {
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

	// Then the first bar will exclude the volume
	c.Check(bars, NotNil)
	c.Check(len(bars.Epoch), Equals, 2)
	c.Check(bars.Epoch[0], Equals,
		time.Date(2020, 11, 20, 10, 3, 0, 0, utils.InstanceConfig.Timezone).Unix())
	c.Check(bars.Open[0], Equals, enum.Price(100.1))
	c.Check(bars.Close[0], Equals, enum.Price(100.1))
	c.Check(bars.High[0], Equals, enum.Price(100.1))
	c.Check(bars.Low[0], Equals, enum.Price(100.1))
	c.Check(bars.Volume[0], Equals, enum.Size(21))

	// Then the second bar will exclude the last & volume update for affected trades
	c.Check(bars.Epoch[1], Equals,
		time.Date(2020, 11, 20, 10, 4, 0, 0, utils.InstanceConfig.Timezone).Unix())
	c.Check(bars.Open[1], Equals, enum.Price(100.2))
	c.Check(bars.Close[1], Equals, enum.Price(100.2))
	c.Check(bars.High[1], Equals, enum.Price(100.2))
	c.Check(bars.Low[1], Equals, enum.Price(99.6))
	c.Check(bars.Volume[1], Equals, enum.Size(13))
}

func (t *TestSuite) TestFromTradesDailyRollup(c *C) {
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
	bars := FromTrades(trades, symbol, "1Day")

	// Then the daily close price should match to the specified
	c.Check(bars, NotNil)
	c.Check(len(bars.Epoch), Equals, 1)
	c.Check(bars.Epoch[0], Equals,
		time.Date(2020, 11, 20, 0, 0, 0, 0, utils.InstanceConfig.Timezone).Unix())
	c.Check(bars.Open[0], Equals, enum.Price(100.1))
	c.Check(bars.Close[0], Equals, enum.Price(105.6))
	c.Check(bars.High[0], Equals, enum.Price(111.2))
	c.Check(bars.Low[0], Equals, enum.Price(100.1))
	c.Check(bars.Volume[0], Equals, enum.Size(130))
}
