package executor

import (
	"runtime/debug"
	"sync"

	"github.com/golang/glog"

	"github.com/alpacahq/marketstore/plugins/trigger"
)

var once sync.Once
var c chan writtenIndexes
var done chan struct{}
var m map[string][]int64

type writtenIndexes struct {
	key     string
	indexes []int64
}

func setup() {
	c = make(chan writtenIndexes, WriteChannelCommandDepth)
	done = make(chan struct{})
	go run()
}

// AddWrittenIndex collects the index value from the serialized buffer.
func addWrittenIndex(keyPath string, index int64) {
	once.Do(setup)
	if m == nil {
		m = make(map[string][]int64)
	}
	m[keyPath] = append(m[keyPath], index)
}

// DispatchWrittenIndexes iterates over the registered triggers and fire the event
// if the file path matches the condition.  This is meant to be
// run in a separate goroutine and recovers from panics in the triggers.
func dispatchWrittenIndexes() {
	for key, indexes := range m {
		c <- writtenIndexes{key: key, indexes: indexes}
	}
	m = nil // for GC
}

func run() {
	defer func() { done <- struct{}{} }()
	for wi := range c {
		for _, tmatcher := range ThisInstance.TriggerMatchers {
			if tmatcher.Match(wi.key) {
				fire(tmatcher.Trigger, wi.key, wi.indexes)
			}
		}
	}
}

func fire(trig trigger.Trigger, key string, indexes []int64) {
	defer func() {
		if r := recover(); r != nil {
			glog.Errorf("recovering from %v\n%s", r, string(debug.Stack()))
		}
	}()
	trig.Fire(key, indexes)
}

// FinishAndWait closes the writtenIndexes channel, and waits
// for the remaining triggers to fire, returning
func FinishAndWait() {
	close(c)
	<-done
}
