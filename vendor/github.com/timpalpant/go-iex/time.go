package iex

import (
	"encoding/json"
	"strconv"
	"time"
)

// IEX timestamps are always provided in milliseconds since the Unix epoch.
// Time implements JSON unmarshaling into the native Go Time type.
type Time struct {
	time.Time
}

func (t *Time) UnmarshalJSON(b []byte) error {
	timestampMillis, err := strconv.ParseInt(string(b), 10, 64)
	if err != nil {
		return err
	}

	var ts time.Time
	if timestampMillis != -1 {
		secs := timestampMillis / 1000
		ns := 1000000 * (timestampMillis % 1000)
		ts = time.Unix(secs, ns)
	}

	*t = Time{ts}
	return nil
}

func (t *Time) MarshalJSON() ([]byte, error) {
	ns := t.Time.UnixNano()
	ms := ns / 1000000
	return json.Marshal(ms)
}
