// Package trigger provides interface for trigger plugins.
// A trigger plugin has to implement the following function.
// - NewTrigger(config map[string]interface{}) (Trigger, error)
//
// The trigger instance returned by this function will be called on Fire()
// with the filePath (relative to root directory) and indexes that have been written
// (appended or updated).  It is guaranteed that the new content has been written
// on disk when Fire() is called, so it is safe to read it from disk.  Keep in mind
// that the trigger might be called on the startup, due to the WAL recovery.
//
// Triggers can be configured in the marketstore config file.
//
// 	triggers:
// 	  - module: xxxTrigger.so
// 	    on: "*/1Min/OHLCV"
// 	    config: <according to the plugin>
//
// The "on" value is matched with the file path to decide whether the trigger
// is fired or not.  It can contain wildcard character "*".
// As of now, trigger fires only on the running state.  Trigger on WAL replay
// may be added later.
package trigger

import (
	"fmt"
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

// SymbolLoader is an interface to retrieve symbol object from plugin
type SymbolLoader interface {
	LoadSymbol(symbolName string) (interface{}, error)
}

// Load loads a function named NewTrigger with a parameter type map[string]interface{}
// and initialize the trigger.
func Load(loader SymbolLoader, config map[string]interface{}) (Trigger, error) {
	symbolName := "NewTrigger"
	sym, err := loader.LoadSymbol(symbolName)
	if err != nil {
		return nil, fmt.Errorf("Unable to load %s", symbolName)
	}

	newFunc, ok := sym.(func(map[string]interface{}) (Trigger, error))
	if !ok {
		return nil, fmt.Errorf("%s does not comply function spec")
	}
	return newFunc(config)
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
