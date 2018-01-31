package executor

import (
	. "gopkg.in/check.v1"

	"github.com/alpacahq/marketstore/plugins/trigger"
	"github.com/alpacahq/marketstore/utils/io"
)

type WrittenIndexesTests struct{}

var _ = Suite(&WrittenIndexesTests{})

type FakeTrigger struct {
	calledWith [][]interface{}
	toPanic    bool
	fireC      chan struct{}
}

func (t *FakeTrigger) Fire(keyPath string, indexes []int64) {
	if t.toPanic {
		panic("panic test")
	}
	t.calledWith = append(t.calledWith, []interface{}{keyPath, indexes})
	t.fireC <- struct{}{}
}

func (s *WrittenIndexesTests) SetUpSuite(c *C) {
	ThisInstance = &InstanceMetadata{}
}

func (s *WrittenIndexesTests) TearDownSuite(c *C) {
	ThisInstance.TriggerMatchers = nil
}

func (s *WrittenIndexesTests) SetTrigger(t trigger.Trigger, on string) {
	matchers := []*trigger.TriggerMatcher{
		trigger.NewMatcher(t, on),
	}
	ThisInstance.TriggerMatchers = matchers
}

func (s *WrittenIndexesTests) TestWrittenIndexes(c *C) {
	t := &FakeTrigger{fireC: make(chan struct{})}
	s.SetTrigger(t, "AAPL/1Min/OHLCV")

	buffer := io.SwapSliceData([]int64{0, 5}, byte(0)).([]byte)
	addWrittenIndex("AAPL/1Min/OHLCV/2017.bin", offsetIndexBuffer(buffer).Index())
	addWrittenIndex("TSLA/1Min/OHLCV/2017.bin", offsetIndexBuffer(buffer).Index())
	dispatchWrittenIndexes()

	<-t.fireC
	c.Check(t.calledWith[0][0].(string), Equals, "AAPL/1Min/OHLCV/2017.bin")
	c.Check(len(t.calledWith), Equals, 1)

	t.calledWith = [][]interface{}{}
	t.toPanic = true
	dispatchWrittenIndexes()
	c.Check(len(t.calledWith), Equals, 0)

	FinishAndWait()
}
