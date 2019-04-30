package configs

import (
	"github.com/json-iterator/go"
	"github.com/pkg/errors"
	"strings"
	"time"
)

// json iter supports marshal/unmarshal of map[interface{}]interface{] type.
// when the config file contains (a) nested structure(s) like follows:
//
// backfill:
//   enabled: true
//
// the standard "encoding/json" library cannot marshal the structure
// because the config is parsed from a yaml file (mkts.yaml) to map[string]interface{} and passed to this file,
// and config["backfill"] object has map[interface{}]interface{} type.
var json = jsoniter.ConfigCompatibleWithStandardLibrary


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
	Backfill            struct {
		Enabled   bool      `json:"enabled"`
		Since     CustomDay `json:"since"`
		Timeframe string    `json:"timeframe"`
	} `json:"backfill"`
}

// NewConfig casts a map object to Config struct and returns it through json marshal->unmarshal
func NewConfig(config map[string]interface{}) (*DefaultConfig, error) {
	data, err := json.Marshal(config)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse the config file through json marshal->unmarshal")
	}

	ret := DefaultConfig{}
	if err := json.Unmarshal(data, &ret); err != nil {
		return nil, err
	}

	if len(ret.Exchanges) < 1 {
		return nil, errors.New("must have 1 or more stock exchanges in the config file")
	}

	return &ret, nil
}

// Custom Time. hh:mm:ss only
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

// Custom Date. yyyy/mm/dd only
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

// ToTimes converts a slice of CustomDay to a slice of time.Time
func ToTimes(s []CustomDay) []time.Time {
	c := make([]time.Time, len(s))
	for i, v := range s {
		c[i] = time.Time(v)
	}
	return c
}
