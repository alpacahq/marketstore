package executor

import (
	"sort"

	"github.com/alpacahq/marketstore/v4/utils/io"
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
	cursorI := i * ei.recordLength
	cursorJ := j * ei.recordLength

	intervalTicksI := io.ToUInt32(ei.buffer[cursorI+ei.recordLength-4 : cursorI+ei.recordLength])
	intervalTicksJ := io.ToUInt32(ei.buffer[cursorJ+ei.recordLength-4 : cursorJ+ei.recordLength])

	return intervalTicksI < intervalTicksJ
}

// Swap swaps the elements with indexes i and j.
func (ei *ByIntervalTicks) Swap(i, j int) {
	cursorI := ei.recordLength * i
	cursorJ := ei.recordLength * j

	// swap slice elements
	for k := 0; k < ei.recordLength; k++ {
		ei.buffer[cursorI], ei.buffer[cursorJ] = ei.buffer[cursorJ], ei.buffer[cursorI]
		cursorI++
		cursorJ++
	}
}
