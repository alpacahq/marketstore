package cache

import (
	"encoding/json"
	"time"
)

func GenData() (e Entries) {
	for i := 4; i >= 0; i-- {
		e = append(e, &Entry{
			Timestamp: time.Now().Add(-time.Duration(i) * time.Minute),
			Data:      json.RawMessage("{\"some\": \"data\"}"),
		})
	}
	return e
}
