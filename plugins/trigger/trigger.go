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
	"time"

	"github.com/alpacahq/marketstore/v4/plugins"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/log"

	"github.com/alpacahq/marketstore/v4/utils/io"
)

// Trigger is an interface every trigger plugin has to implement.
type Trigger interface {
	// Fire is called when the target file has been modified.
	// keyPath is the string path of the modified file relative
	// from the catalog root directory.  indexes is a slice
	// containing indexes of the rows being modified.
	Fire(keyPath string, records []Record)
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

// Record represents a serialized byte buffer
// for a record written to the DB
type Record []byte

// Bytes returns the raw record buffer
func (r *Record) Bytes() []byte {
	return *r
}

// Index returns the index of the record
func (r *Record) Index() int64 {
	if r == nil {
		return 0
	}
	return io.ToInt64((*r)[0:8])
}

// Payload returns the data payload of the record,
// excluding the index
func (r *Record) Payload() []byte {
	if r == nil {
		return nil
	}
	return (*r)[8:]
}

// RecordsToColumnSeries takes a slice of Record, along with the required
// information for constructing a ColumnSeries, and builds it from the
// slice of Record.
func RecordsToColumnSeries(
	tbk io.TimeBucketKey,
	ds []io.DataShape,
	tf time.Duration,
	year int16,
	records []Record) *io.ColumnSeries {

	cs := io.NewColumnSeries()

	index := 0

	for _, s := range ds {
		data := []byte{}

		for _, record := range records {
			slc := record.Bytes()[index : index+s.Len()]
			if strings.EqualFold(s.Name, "Epoch") {
				buf, _ := io.Serialize(nil,
					io.IndexToTime(io.ToInt64(slc), tf, year).Unix())
				data = append(data, buf...)
			} else {
				data = append(data, slc...)
			}
		}

		cs.AddColumn(s.Name, s.Type.ConvertByteSliceInto(data))
		index += s.Len()
	}

	return cs
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
		return nil, fmt.Errorf("%s does not comply function spec", symbolName)
	}
	return newFunc(config)
}

func NewTriggerMatchers(triggers []*utils.TriggerSetting) []*TriggerMatcher {
	log.Info("InitializeTriggers")
	var triggerMatchers []*TriggerMatcher

	for _, triggerSetting := range triggers {
		log.Info("triggerSetting = %v", triggerSetting)
		tmatcher := NewTriggerMatcher(triggerSetting)
		if tmatcher != nil {
			triggerMatchers = append(
				triggerMatchers, tmatcher)
		}
	}
	log.Info("InitializeTriggers - Done")
	return triggerMatchers
}

func NewTriggerMatcher(ts *utils.TriggerSetting) *TriggerMatcher {
	loader, err := plugins.NewSymbolLoader(ts.Module)
	if err != nil {
		log.Error("Unable to open plugin for trigger in %s: %v", ts.Module, err)
		return nil
	}
	trig, err := Load(loader, ts.Config)
	if err != nil {
		log.Error("Error returned while creating a trigger: %v", err)
		return nil
	}
	return NewMatcher(trig, ts.On)
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
