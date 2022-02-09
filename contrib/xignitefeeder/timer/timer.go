package timer

import (
	"context"
	"time"

	"github.com/alpacahq/marketstore/v4/utils/log"
)

// RunEveryDayAt runs a specified function every day at a specified hour.
func RunEveryDayAt(ctx context.Context, t time.Time, f func(context.Context)) {
	timeToNextRun := timeToNext(time.Now(), t)

	// run at a specified time on the next day
	time.AfterFunc(timeToNextRun, func() { f(ctx) })

	// at the same time, run  every 24 hour
	time.AfterFunc(timeToNextRun, func() {
		ticker := time.NewTicker(24 * time.Hour)

		for {
			select {
			case <-ticker.C:
				f(ctx)
			case <-ctx.Done():
				log.Debug("job stopped due to ctx.Done()")
				ticker.Stop()
				return
			}
		}
	})
}

// timeToNext returns the time duration from now to next {hour}:{minute}:{second}
// For example, when the current time is 8pm, timeToNext(16:00:00) = 20 * time.Hour.
func timeToNext(now, next time.Time) time.Duration {
	n := time.Date(now.Year(), now.Month(), now.Day(), next.Hour(), next.Minute(), next.Second(),
		0, next.Location(),
	)

	d := n.Sub(now)
	if d < 0 {
		d += 24 * time.Hour
	}
	if d >= 24*time.Hour {
		d -= 24 * time.Hour
	}
	return d
}
