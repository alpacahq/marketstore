package trigger

import (
	"regexp"
	"strings"
)

type Trigger interface {
	Fire(keyPath string, offsets []int64)
}

type TriggerMatcher struct {
	Trigger Trigger
	On      string
}

type NewFuncType func(config map[string]interface{}) (Trigger, error)

func NewMatcher(trigger Trigger, on string) *TriggerMatcher {
	return &TriggerMatcher{
		Trigger: trigger, On: on,
	}
}

func (tm *TriggerMatcher) Match(keyPath string) bool {
	pattern := strings.Replace(tm.On, "*", "[^/]+", -1)
	matched, _ := regexp.MatchString(pattern, keyPath)
	return matched
}
