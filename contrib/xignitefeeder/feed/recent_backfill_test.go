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

// GetRealTimeBars returns "Request Error" to certain identifier, but returns "Success" to other identifiers.
func (mac *MockErrorAPIClient) GetRealTimeBars(_ context.Context, i string, _, _ time.Time,
) (resp api.GetBarsResponse, err error) {
	if i == "XTKS.1301" {
		return api.GetBarsResponse{
			Outcome:    "RequestError",
			Security:   &api.Security{Symbol: "1301"},
			ArrayOfBar: []api.Bar{},
		}, errors.New("error")
	}

	return api.GetBarsResponse{
		Outcome:    api.SuccessOutcome,
		Security:   &api.Security{Symbol: "1301"},
		ArrayOfBar: []api.Bar{},
	}, nil
}

type MockBarWriter struct {
	WriteCount int
}

func (mbw *MockBarWriter) Write(_ string, _ []api.Bar, _ bool) error {
	// in order to assert the number of writes in the test
	mbw.WriteCount++
	return nil
}

// 3 writes should be successfully done with the 3 identifiers.
func TestRecentBackfill_Update(t *testing.T) {
	t.Parallel()
	// --- given ---
	var w writer.BarWriter = &MockBarWriter{WriteCount: 0}

	SUT := &RecentBackfill{
		symbolManager:     internal.MockSymbolsManager{Identifiers: TestIdentifiers},
		marketTimeChecker: &internal.MockTimeChecker{},
		apiClient:         &internal.MockAPIClient{},
		writer:            w,
		days:              7,
	}

	// --- when ---
	SUT.Update(context.Background())

	// --- then ---
	if mw, ok := w.(*MockBarWriter); ok {
		if mw.WriteCount != 3 {
			t.Errorf("3 writes should be performed. got: WriteCount=%v", mw.WriteCount)
		}
	} else {
		t.Fatalf("type error")
	}
}

// Even if Xignite returns Outcome:"RequestError" to an identifier, Backfill writes data for the other identifiers.
func TestRecentBackfill_Update_RequestErrorIdentifier(t *testing.T) {
	t.Parallel()
	// --- given ---
	var w writer.BarWriter = &MockBarWriter{WriteCount: 0}

	SUT := &RecentBackfill{
		symbolManager:     internal.MockSymbolsManager{Identifiers: []string{"XTKS.1301", "XTKS.1305", "XJAS.1376"}},
		marketTimeChecker: &internal.MockTimeChecker{},
		apiClient:         &MockErrorAPIClient{},
		writer:            w,
	}

	// --- when ---
	SUT.Update(context.Background())

	// --- then ---
	// write fails for 1 out of 3 identifiers
	if mw, ok := w.(*MockBarWriter); ok {
		if mw.WriteCount != 2 {
			t.Errorf("2 writes should be performed. got: WriteCount=%v", mw.WriteCount)
		}
	} else {
		t.Fatalf("type error")
	}
}
