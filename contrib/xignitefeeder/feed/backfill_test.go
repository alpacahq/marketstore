package feed

import (
	"context"
	"testing"
	"time"

	"github.com/pkg/errors"

	"github.com/alpacahq/marketstore/v4/contrib/xignitefeeder/api"
	"github.com/alpacahq/marketstore/v4/contrib/xignitefeeder/internal"
	"github.com/alpacahq/marketstore/v4/contrib/xignitefeeder/writer"
)

var TestIdentifiers = []string{"XTKS.1301", "XTKS.1305", "XJAS.1376"}

type MockErrorAPIClient struct {
	internal.MockAPIClient
}

// GetQuotesRange returns "Request Error" to certain identifier, but returns "Success" to other identifiers.
func (mac *MockErrorAPIClient) GetQuotesRange(_ context.Context, i string, _, _ time.Time,
) (resp api.GetQuotesRangeResponse, err error) {
	if i == "XTKS.1301" {
		return api.GetQuotesRangeResponse{
			Outcome:              "RequestError",
			Security:             &api.Security{Symbol: "1301"},
			ArrayOfEndOfDayQuote: []api.EndOfDayQuote{},
		}, errors.New("error")
	}

	return api.GetQuotesRangeResponse{
		Outcome:              api.SuccessOutcome,
		Security:             &api.Security{Symbol: "1301"},
		ArrayOfEndOfDayQuote: []api.EndOfDayQuote{},
	}, nil
}

type MockQuotesRangeWriter struct {
	WriteCount      int
	WriteIndexCount int
}

func (mqrw *MockQuotesRangeWriter) Write(_ string, _ []api.EndOfDayQuote, _ bool) error {
	// in order to assert the number of writes in the test
	mqrw.WriteCount++
	return nil
}

func (mqrw *MockQuotesRangeWriter) WriteIndex(_ api.GetIndexQuotesRangeResponse) error {
	// in order to assert the number of writes in the test
	mqrw.WriteIndexCount++
	return nil
}

// 3 writes should be successfully done with the 3 identifiers.
func TestBackfill_Update(t *testing.T) {
	t.Parallel()
	// --- given ---
	var rw writer.QuotesRangeWriter = &MockQuotesRangeWriter{WriteCount: 0}
	var w writer.QuotesWriter = &MockQuotesWriter{WriteCount: 0}

	SUT := &Backfill{
		symbolManager: internal.MockSymbolsManager{Identifiers: TestIdentifiers},
		apiClient:     &internal.MockAPIClient{},
		writer:        w,
		rangeWriter:   rw,
		since:         time.Now().UTC(),
	}

	// --- when ---
	SUT.Update(context.Background())

	// --- then ---
	if mrw, ok := rw.(*MockQuotesRangeWriter); ok {
		if mrw.WriteCount != 3 {
			t.Errorf("3 writes should be performed (1 write for 1 symbol). got: WriteCount=%v", mrw.WriteCount)
		}
	} else {
		t.Fatalf("type error")
	}

	if mw, ok := w.(*MockQuotesWriter); ok {
		if mw.WriteCount != 1 {
			t.Errorf("1 writes should be performed (1 write for 3 symbols). got: WriteCount=%v", mw.WriteCount)
		}
	} else {
		t.Fatalf("type error")
	}
}

// Even if Xignite returns Outcome:"RequestError" to an identifier, Backfill writes data for the other identifiers.
func TestBackfill_Update_RequestErrorIdentifier(t *testing.T) {
	t.Parallel()
	// --- given ---
	var rw writer.QuotesRangeWriter = &MockQuotesRangeWriter{WriteCount: 0}
	var w writer.QuotesWriter = &MockQuotesWriter{WriteCount: 0}

	SUT := &Backfill{
		symbolManager: internal.MockSymbolsManager{Identifiers: []string{"XTKS.1301", "XTKS.1305", "XJAS.1376"}},
		apiClient:     &MockErrorAPIClient{},
		writer:        w,
		rangeWriter:   rw,
		since:         time.Now().UTC(),
	}

	// --- when ---
	SUT.Update(context.Background())

	// --- then ---
	// write fails for 1 out of 3 identifiers
	if mw, ok := rw.(*MockQuotesRangeWriter); ok {
		if mw.WriteCount != 2 {
			t.Errorf("2 writes should be performed. got: WriteCount=%v", mw.WriteCount)
		}
	} else {
		t.Fatalf("type error")
	}
}
