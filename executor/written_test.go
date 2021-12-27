package executor_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/executor/wal"
	"github.com/alpacahq/marketstore/v4/plugins/trigger"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

type FakeTrigger struct {
	calledWith [][]interface{}
	fireC      chan struct{}
	toPanic    bool
}

func NewFakeTrigger(toPanic bool) *FakeTrigger {
	return &FakeTrigger{
		fireC:   make(chan struct{}),
		toPanic: toPanic,
	}
}

// Fire sends a message to fireC channel when a record is triggered.
func (t *FakeTrigger) Fire(keyPath string, records []trigger.Record) {
	defer func() { t.fireC <- struct{}{} }()

	if t.toPanic {
		panic("panic")
	}
	t.calledWith = append(t.calledWith, []interface{}{keyPath, records})
}

func TestTriggerPluginDispatcher(t *testing.T) {
	t.Parallel()

	type record struct {
		keyPath string
	}
	tests := []struct {
		name              string
		trigger           *FakeTrigger
		on                string
		records           []record
		wantCalledWith    string
		wantCalledWithLen int
	}{
		{
			name:    "only records that match the keypath should be triggered",
			trigger: NewFakeTrigger(false),
			on:      "AAPL/1Min/OHLCV",
			records: []record{
				{keyPath: "AAPL/1Min/OHLCV/2017.bin"},
				{keyPath: "TSLA/1Min/OHLCV/2017.bin"},
			},
			wantCalledWith:    "AAPL/1Min/OHLCV/2017.bin",
			wantCalledWithLen: 1,
		},
		{
			name:    "recovered when panic is triggered",
			trigger: NewFakeTrigger(true),
			on:      "AAPL/1Min/OHLCV",
			records: []record{
				{keyPath: "AAPL/1Min/OHLCV/2017.bin"},
			},
			wantCalledWith:    "AAPL/1Min/OHLCV/2017.bin",
			wantCalledWithLen: 0,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// --- given ---
			matchers := []*trigger.TriggerMatcher{trigger.NewMatcher(tt.trigger, tt.on)}
			tpd := executor.NewTriggerPluginDispatcher(matchers)
			fakeBuffer := io.SwapSliceData([]int64{0, 5}, byte(0)).([]byte)

			// --- when
			for _, r := range tt.records {
				tpd.AppendRecord(r.keyPath, wal.OffsetIndexBuffer(fakeBuffer).IndexAndPayload())
			}
			tpd.DispatchRecords()

			<-tt.trigger.fireC // wait until fired

			// --- then ---
			assert.Equal(t, len(tt.trigger.calledWith), tt.wantCalledWithLen)
			if tt.wantCalledWithLen > 0 {
				assert.Equal(t, tt.trigger.calledWith[0][0].(string), tt.wantCalledWith)
			}
		})
	}
}
