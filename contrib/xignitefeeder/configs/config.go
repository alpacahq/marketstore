package configs

import (
	"encoding/json"
	"errors"
)

// FetchConfig is the configuration for TickFeeder you can define in
// marketstore's config file through bgworker extension.
type DefaultConfig struct {
	Identifiers []string `json:"identifiers"`
	Timeframe   string   `json:"timeframe"`
	APIToken    string   `json:"token"`
	Timeout     int      `json:"timeout"`
	CloseDates  []string `json:"closedates"`
	Interval    int      `json:"interval"`
}

// NewConfig casts a map object to Config struct and returns it
func NewConfig(config map[string]interface{}) (*DefaultConfig, error) {
	data, err := json.Marshal(config)
	if err != nil {
		return nil, err
	}

	ret := DefaultConfig{}
	err = json.Unmarshal(data, &ret)
	if err != nil {
		return nil, err
	}

	if len(ret.Identifiers) < 1 {
		return nil, errors.New("must have 1 or more identifiers in the config file")
	}

	return &ret, nil
}
