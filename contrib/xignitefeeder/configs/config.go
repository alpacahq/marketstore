package configs

import (
	"encoding/json"
	"errors"
	"strings"
	"time"
)

// FetchConfig is the configuration for TickFeeder you can define in
// marketstore's config file through bgworker extension.
type DefaultConfig struct {
	Exchanges           []string    `json:"exchanges"`
	UpdatingHour        int         `json:"updatingHour"`
	Timeframe           string      `json:"timeframe"`
	APIToken            string      `json:"token"`
	Timeout             int         `json:"timeout"`
	OpenTime            CustomTime  `json:"openTime"`
	CloseTime           CustomTime  `json:"closeTime"`
	ClosedDaysOfTheWeek []string    `json:"closedDaysOfTheWeek"`
	ClosedDays          []CustomDay `json:"closedDays"`
	Interval            int         `json:"interval"`
}

const ctLayout = "15:04:05"

type CustomTime time.Time

func (ct *CustomTime) UnmarshalJSON(input []byte) error {
	s := strings.Trim(string(input), "\"")
	if s == "null" {
		*ct = CustomTime(time.Time{})
		return nil
	}
	t, err := time.Parse(ctLayout, s)
	if err != nil {
		return err
	}
	*ct = CustomTime(t)
	return nil
}

const cdLayout = "2006/01/02"

type CustomDay time.Time

func (cd *CustomDay) UnmarshalJSON(input []byte) error {
	s := strings.Trim(string(input), "\"")
	if s == "null" {
		*cd = CustomDay(time.Time{})
		return nil
	}
	t, err := time.Parse(cdLayout, s)
	if err != nil {
		return err
	}
	*cd = CustomDay(t)
	return nil
}

func ToTimes(s []CustomDay) []time.Time {
	c := make([]time.Time, len(s))
	for i, v := range s {
		c[i] = time.Time(v)
	}
	return c
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

	if len(ret.Exchanges) < 1 {
		return nil, errors.New("must have 1 or more stock exchanges in the config file")
	}

	return &ret, nil
}
