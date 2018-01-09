package executor

import (
	. "github.com/alpacahq/marketstore/utils/io"
)

type WrittenIndexes struct {
	indexesMap map[string][]int64
}

func NewWrittenIndexes() *WrittenIndexes {
	return &WrittenIndexes{
		indexesMap: map[string][]int64{},
	}
}

func (wo *WrittenIndexes) Accum(keyPath string, offsetIndexDataBuffer []byte) {
	offset := ToInt64(offsetIndexDataBuffer[8:])
	wo.indexesMap[keyPath] = append(wo.indexesMap[keyPath], offset)
}

func (wo *WrittenIndexes) Dispatch() {
	for keyPath, offsets := range wo.indexesMap {
		for _, tmatcher := range ThisInstance.TriggerMatchers {
			if tmatcher.Match(keyPath) {
				tmatcher.Trigger.Fire(keyPath, offsets)
			}
		}
	}
}
