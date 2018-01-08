package executor

import (
	. "github.com/alpacahq/marketstore/utils/io"
)

type WrittenOffsets struct {
	offsetsMap map[string][]int64
}

func NewWrittenOffsets() *WrittenOffsets {
	return &WrittenOffsets{
		offsetsMap: map[string][]int64{},
	}
}

func (wo *WrittenOffsets) Accum(keyPath string, offsetIndexDataBuffer []byte) {
	offset := ToInt64(offsetIndexDataBuffer[:7])
	wo.offsetsMap[keyPath] = append(wo.offsetsMap[keyPath], offset)
}

func (wo *WrittenOffsets) Dispatch() {
	for keyPath, offsets := range wo.offsetsMap {
		for _, tmatcher := range ThisInstance.TriggerMatchers {
			if tmatcher.Match(keyPath) {
				tmatcher.Trigger.Fire(keyPath, offsets)
			}
		}
	}
}
