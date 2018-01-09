// A trigger plugin has to implement the following function.
// - NewTrigger(config map[string]interface{}) (Trigger, error)
//
// The trigger instance returned by this function will be called on Fire()
// with the filePath (relative to root directory) and indexes that have been written
// (appended or updated).  It is guaranteed that the new content has been written
// on disk when Fire() is called, so it is safe to read it from disk.
//
// Triggers can be configured in the marketstore config file.
//
// 	triggers:
// 	  - module: xxxTrigger.so
// 	    on: "*/1Min/OHLCV"
// 	    config:
// 	      - destinations:
// 	          - 5Min
// 	          - 15Min
// 	          - 1D
//
// The "on" value is matched with the file path to decide whether the trigger
// is fired or not.  It can contain wildcard character "*".
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
