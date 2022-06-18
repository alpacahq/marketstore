package executor

import (
	"runtime/debug"
	"sync"

	"github.com/alpacahq/marketstore/v4/plugins/trigger"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

type TriggerPluginDispatcher struct {
	c               chan writtenRecords
	done            chan struct{}
	m               map[string][]trigger.Record
	triggerMatchers []*trigger.Matcher
	triggerWg       *sync.WaitGroup
}

type writtenRecords struct {
	key     string
	records []trigger.Record
}

func StartNewTriggerPluginDispatcher(triggerMatchers []*trigger.Matcher) *TriggerPluginDispatcher {
	tpd := TriggerPluginDispatcher{
		c:               make(chan writtenRecords, WriteChannelCommandDepth),
		done:            make(chan struct{}),
		m:               nil,
		triggerMatchers: triggerMatchers,
		triggerWg:       &sync.WaitGroup{},
	}
	go tpd.run()

	return &tpd
}

func (tpd *TriggerPluginDispatcher) run() {
	defer func() { tpd.done <- struct{}{} }()

	for wr := range tpd.c {
		for _, tmatcher := range tpd.triggerMatchers {
			if tmatcher.Match(wr.key) {
				tpd.triggerWg.Add(1)
				go tpd.fire(tmatcher.Trigger, wr.key, wr.records)
			}
		}
	}
}

// AppendRecord collects the record from the serialized buffer.
func (tpd *TriggerPluginDispatcher) AppendRecord(keyPath string, record []byte) {
	if tpd.m == nil {
		tpd.m = make(map[string][]trigger.Record)
	}

	tpd.m[keyPath] = append(tpd.m[keyPath], record)
}

// DispatchRecords iterates over the registered triggers and fire the event
// if the file path matches the condition.  This is meant to be
// run in a separate goroutine and recovers from panics in the triggers.
func (tpd *TriggerPluginDispatcher) DispatchRecords() {
	for key, records := range tpd.m {
		tpd.c <- writtenRecords{key: key, records: records}
	}
	tpd.m = nil // for GC
}

func (tpd *TriggerPluginDispatcher) fire(trig trigger.Trigger, key string, records []trigger.Record) {
	defer func() {
		tpd.triggerWg.Done()
		if r := recover(); r != nil {
			log.Error("recovering from %v\n%s", r, string(debug.Stack()))
		}
	}()
	trig.Fire(key, records)
}
