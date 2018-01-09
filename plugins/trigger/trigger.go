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
// As of now, trigger fires only on the running state.  Trigger on WAL replay
// may be added later.
package trigger

import (
	"regexp"
	"strings"
)

// Trigger is an interface every trigger plugin has to implement.
type Trigger interface {
	// Fire is called when the target file has been modified.
	// keyPath is the string path of the modified file relative
	// from the catalog root directory.  indexes is a slice
	// containing indexes of the rows being modified.
	Fire(keyPath string, indexes []int64)
}

// TriggerMatcher checks if the trigger should be fired or not.
type TriggerMatcher struct {
	Trigger Trigger
	// On is a string representing the condition of the trigger
	// fire event.  It is the prefix of file path such as
	// ""*/1Min/OHLC"
	On string
}

// NewMatcher creates a new TriggerMatcher.
func NewMatcher(trigger Trigger, on string) *TriggerMatcher {
	return &TriggerMatcher{
		Trigger: trigger, On: on,
	}
}

// Match returns true if keyPath matches the On condition.
func (tm *TriggerMatcher) Match(keyPath string) bool {
	pattern := strings.Replace(tm.On, "*", "[^/]+", -1)
	matched, _ := regexp.MatchString(pattern, keyPath)
	return matched
}
