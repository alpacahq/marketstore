package timer

import "time"

// UpdateEveryDayAt updates the symbols every day at the specified hour
func RunEveryDayAt(hour int, f func()) {
	f()
	time.AfterFunc(timeToNext(hour), f)
}

// timeToNext returns the time duration from now to next {hour}:00:00
// For example, when the current time is 8pm, timeToNext(16) = 20 * time.Hour
func timeToNext(hour int) time.Duration {
	t := time.Now()
	n := time.Date(t.Year(), t.Month(), t.Day(), hour, 0, 0, 0, t.Location())
	if t.After(n) {
		n = n.Add(24 * time.Hour)
	}
	d := n.Sub(t)
	return d
}
