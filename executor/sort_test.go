package executor

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSortBuffer(t *testing.T) {
	t.Parallel()
	// --- given ---
	// 4 records * 8byte, [3,4,2,1]
	buffer := []byte{
		0, 0, 0, 3, 0, 0, 0, 3,
		0, 0, 0, 4, 0, 0, 0, 4,
		0, 0, 0, 2, 0, 0, 0, 2,
		0, 0, 0, 1, 0, 0, 0, 1,
	}
	var dataLen int = 32
	var recordLength int = 8

	// --- when ---
	// sort by the last 4 byte (=
	sort.Stable(NewByIntervalTicks(buffer, dataLen/recordLength, recordLength))

	// --- then ---
	expected := []byte{
		0, 0, 0, 1, 0, 0, 0, 1,
		0, 0, 0, 2, 0, 0, 0, 2,
		0, 0, 0, 3, 0, 0, 0, 3,
		0, 0, 0, 4, 0, 0, 0, 4,
	}
	assert.Equal(t, expected, buffer)
}
