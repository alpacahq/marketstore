package executor

import "C"
import (
	"github.com/alpacahq/marketstore/utils/io"
	"sort"
)

// ByIntervalTicks implements a custom sort for Variable length record.
// Sort by the last 4 byte (= intervalTicks) of each record.
type ByIntervalTicks struct {
	buffer       []byte
	numWords     int
	recordLength int
}

func NewByIntervalTicks(buffer []byte, numWords, recordLength int) sort.Interface {
	return &ByIntervalTicks{
		buffer:       buffer,
		numWords:     numWords,
		recordLength: recordLength,
	}
}

func (ei *ByIntervalTicks) Len() int { return ei.numWords }

// Less reports whether the element with
// index i should sort before the element with index j.
func (ei *ByIntervalTicks) Less(i, j int) bool {
	cursor_i := i * ei.recordLength
	cursor_j := j * ei.recordLength

	intervalTicks_i := io.ToUInt32(ei.buffer[cursor_i+ei.recordLength-4 : cursor_i+ei.recordLength])
	intervalTicks_j := io.ToUInt32(ei.buffer[cursor_j+ei.recordLength-4 : cursor_j+ei.recordLength])

	return intervalTicks_i < intervalTicks_j
}

// Swap swaps the elements with indexes i and j.
func (ei *ByIntervalTicks) Swap(i, j int) {
	cursor_i := ei.recordLength * i
	cursor_j := ei.recordLength * j

	// swap slice elements
	for k := 0; k < ei.recordLength; k++ {
		ei.buffer[cursor_i], ei.buffer[cursor_j] = ei.buffer[cursor_j], ei.buffer[cursor_i]
		cursor_i++
		cursor_j++
	}
}
