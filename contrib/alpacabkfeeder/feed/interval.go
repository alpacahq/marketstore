package feed

import (
	"time"

	"github.com/alpacahq/marketstore/v4/utils/log"
)

// IntervalMarketTimeChecker is used where periodic processing is needed to run even when the market is closed.
type IntervalMarketTimeChecker struct {
	MarketTimeChecker
	// LastTime holds the last time that IntervalTimeChceker.IsOpen returned true.
	LastTime time.Time
	Interval time.Duration
}

func NewIntervalMarketTimeChecker(
	mtc MarketTimeChecker,
	interval time.Duration,
) *IntervalMarketTimeChecker {
	return &IntervalMarketTimeChecker{
		MarketTimeChecker: mtc,
		LastTime:          time.Time{},
		Interval:          interval,
	}
}

// IsOpen returns true when the market is open or the interval elapsed since LastTime.
func (c *IntervalMarketTimeChecker) IsOpen(t time.Time) bool {
	return c.MarketTimeChecker.IsOpen(t) || c.intervalElapsed(t)
}

func (c *IntervalMarketTimeChecker) intervalElapsed(t time.Time) bool {
	elapsed := t.Sub(c.LastTime) >= c.Interval
	if elapsed {
		// log if this is not the first time
		if !c.LastTime.IsZero() {
			log.Debug("[Alpaca Broker Feeder] interval elapsed since last time: " + t.String())
		}
		c.LastTime = t
	}
	return elapsed
}
