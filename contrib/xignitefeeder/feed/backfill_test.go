package feed

import (
	"testing"
	"time"

	"github.com/alpacahq/marketstore/contrib/xignitefeeder/internal"
	"github.com/alpacahq/marketstore/contrib/xignitefeeder/writer"
	"github.com/pkg/errors"

	"github.com/alpacahq/marketstore/contrib/xignitefeeder/api"
)

var TestIdentifiers = []string{"XTKS.1301", "XTKS.1305", "XJAS.1376"}

type MockErrorAPIClient struct {
	internal.MockAPIClient
}

// GetQuotesRange returns "Request Error" to certain identifier, but returns "Success" to other identifiers
func (mac *MockErrorAPIClient) GetQuotesRange(i string, sd, ed time.Time) (resp api.GetQuotesRangeResponse, err error) {

	if i == "XTKS.1301" {
		return api.GetQuotesRangeResponse{Outcome: "RequestError"}, errors.New("error")
	}

	return api.GetQuotesRangeResponse{Outcome: "Success"}, nil
}

type MockQuotesRangeWriter struct {
	WriteCount int
}

func (mqrw *MockQuotesRangeWriter) Write(quotesRange api.GetQuotesRangeResponse) error {
	// in order to assert the number of writes in the test
	mqrw.WriteCount++
	return nil
}

// 3 writes should be successfully done with the 3 identifiers
func TestBackfill_Update(t *testing.T) {
	// --- given ---
	var w writer.QuotesRangeWriter = &MockQuotesRangeWriter{WriteCount: 0}

	SUT := &Backfill{
		symbolManager: internal.MockSymbolsManager{Identifiers: TestIdentifiers},
		apiClient:     &internal.MockAPIClient{},
		writer:        w,
		since:         time.Now().UTC(),
	}

	// --- when ---
	SUT.Update()

	// --- then ---
	if mw, ok := w.(*MockQuotesRangeWriter); ok {
		if mw.WriteCount != 3 {
			t.Errorf("3 writes should be performed. got: WriteCount=%v", mw.WriteCount)
		}
	} else {
		t.Fatalf("type error")
	}
}

// Even if Xignite returns Outcome:"RequestError" to an identifier, Backfill writes data for the other identifiers
func TestBackfill_Update_RequestErrorIdentifier(t *testing.T) {
	// --- given ---
	var w writer.QuotesRangeWriter = &MockQuotesRangeWriter{WriteCount: 0}

	SUT := &Backfill{
		symbolManager: internal.MockSymbolsManager{Identifiers: []string{"XTKS.1301", "XTKS.1305", "XJAS.1376"}},
		apiClient:     &MockErrorAPIClient{},
		writer:        w,
		since:         time.Now().UTC(),
	}

	// --- when ---
	SUT.Update()

	// --- then ---
	// write fails for 1 out of 3 identifiers
	if mw, ok := w.(*MockQuotesRangeWriter); ok {
		if mw.WriteCount != 2 {
			t.Errorf("2 writes should be performed. got: WriteCount=%v", mw.WriteCount)
		}
	} else {
		t.Fatalf("type error")
	}
}
