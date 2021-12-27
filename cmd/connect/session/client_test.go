package session

import (
	"testing"

	"github.com/alpacahq/marketstore/v4/utils/io"
)

func TestPrintResult(t *testing.T) {
	t.Parallel()

	// --- given ---
	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", []int64{1483228800, 1483315200, 1483401600}) // 2017-01-01,02,03
	cs.AddColumn("Memo", [][16]int32{
		{72, 101, 108, 108, 111, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},    // Hello
		{87, 111, 114, 108, 100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},    // World
		{26085, 26412, 35486, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, // 日本語 (Multi-byte chars)
	},
	)

	// --- when ---
	err := printResult("", cs)
	// --- then ---
	if err != nil {
		t.Fatal(err)
	}
}
