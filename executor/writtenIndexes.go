package executor

import (
	"runtime/debug"

	"github.com/golang/glog"
)

// WrittenIndexes collects row indexes of files being modified
// for triggers to act on changes.
type WrittenIndexes struct {
	// key of the map is string relative path of the modified file
	indexesMap map[string][]int64
}

// NewWrittenIndexes creates a new WrittenIndexes.
func NewWrittenIndexes() *WrittenIndexes {
	return &WrittenIndexes{
		indexesMap: map[string][]int64{},
	}
}

// Add collects the index value from the serialized buffer.
func (wo *WrittenIndexes) Add(keyPath string, buffer offsetIndexBuffer) {
	index := buffer.Index()
	wo.indexesMap[keyPath] = append(wo.indexesMap[keyPath], index)
}

// Dispatch iterates over the registered triggers and fire the event
// if the file path matches the condition.  This is meant to be
// run in a separate goroutine and recovers from panics in the triggers.
func (wo *WrittenIndexes) Dispatch() {
	defer func() {
		if r := recover(); r != nil {
			glog.Errorf("recovering from %v\n%s", r, string(debug.Stack()))
		}
	}()
	for keyPath, indexes := range wo.indexesMap {
		for _, tmatcher := range ThisInstance.TriggerMatchers {
			if tmatcher.Match(keyPath) {
				tmatcher.Trigger.Fire(keyPath, indexes)
			}
		}
	}
}
