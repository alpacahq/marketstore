package timer

import "time"

// RunEveryDayAt updates the symbols every day at the specified hour
func RunEveryDayAt(hour int, f func()) {
	f()
	time.AfterFunc(timeToNext(time.Now(), hour), f)
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
