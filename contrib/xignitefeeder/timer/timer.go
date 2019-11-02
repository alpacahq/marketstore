package timer

import (
	"time"
)

// RunEveryDayAt runs a specified function every day at a specified hour
func RunEveryDayAt(hour int, f func()) {
	stopped := make(chan bool, 1)

	go func() {
		// run at a specified time on the next day
		time.AfterFunc(timeToNext(time.Now(), hour), f)

		// after that, ticker runs every 24 hour
		ticker := time.NewTicker(24 * time.Hour)

		for {
			select {
			case <-ticker.C:
				f()
			case <-stopped:
				ticker.Stop()
				return
			}
		}
	}()

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
