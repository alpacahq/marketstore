package executor

import (
	"runtime/debug"
	"sync"
	"time"

	"github.com/alpacahq/marketstore/v4/utils/log"

	"github.com/alpacahq/marketstore/v4/plugins/trigger"
)

var (
	once      sync.Once
	c         chan writtenRecords
	done      chan struct{}
	m         map[string][]trigger.Record
	triggerWg sync.WaitGroup
)

type writtenRecords struct {
	key     string
	records []trigger.Record
}

func setup() {
	c = make(chan writtenRecords, WriteChannelCommandDepth)
	done = make(chan struct{})
	go run()
}

// appendRecord collects the record from the serialized buffer.
func appendRecord(keyPath string, record []byte) {
	once.Do(setup)
	if m == nil {
		m = make(map[string][]trigger.Record)
	}
	m[keyPath] = append(m[keyPath], record)
}

// dispatchRecords iterates over the registered triggers and fire the event
// if the file path matches the condition.  This is meant to be
// run in a separate goroutine and recovers from panics in the triggers.
func dispatchRecords() {
	for key, records := range m {
		c <- writtenRecords{key: key, records: records}
	}
	m = nil // for GC
}

func run() {
	defer func() { done <- struct{}{} }()
	for wr := range c {
		for _, tmatcher := range ThisInstance.TriggerMatchers {
			if tmatcher.Match(wr.key) {
				triggerWg.Add(1)
				go fire(tmatcher.Trigger, wr.key, wr.records)
			}
		}
	}
}

func fire(trig trigger.Trigger, key string, records []trigger.Record) {
	defer func() {
		triggerWg.Done()
		if r := recover(); r != nil {
			log.Error("recovering from %v\n%s", r, string(debug.Stack()))
		}
	}()
	trig.Fire(key, records)
}

// FinishAndWait closes the writtenIndexes channel, and waits
// for the remaining triggers to fire, returning
func FinishAndWait() {
	triggerWg.Wait()
	for {
		if len(ThisInstance.TXNPipe.writeChannel) == 0 && len(c) == 0 {
			close(c)
			<-done
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
}
