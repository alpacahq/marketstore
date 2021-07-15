package timer

import (
	"context"
	"time"

	"github.com/alpacahq/marketstore/v4/utils/log"
)

// RunEveryDayAt runs a specified function every day at a specified hour
func RunEveryDayAt(ctx context.Context, hour int, f func()) {
	timeToNextRun := timeToNext(time.Now(), hour)

	// run at a specified time on the next day
	time.AfterFunc(timeToNextRun, f)

	// at the same time, run  every 24 hour
	time.AfterFunc(timeToNextRun, func() {
		ticker := time.NewTicker(24 * time.Hour)

		for {
			select {
			case <-ticker.C:
				f()
			case <-ctx.Done():
				log.Debug("job stopped due to ctx.Done()")
				ticker.Stop()
				return
			}
		}
	})

	return
}

// timeToNext returns the time duration from now to next {hour}:00:00
// For example, when the current time is 8pm, timeToNext(16) = 20 * time.Hour
func timeToNext(now time.Time, hour int) time.Duration {
	n := time.Date(now.Year(), now.Month(), now.Day(), hour, 0, 0, 0, now.Location())
	if now.After(n) {
		n = n.Add(24 * time.Hour)
	}
	d := n.Sub(now)
	return d
}
