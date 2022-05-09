package frontend

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/alpacahq/marketstore/v4/utils"
)

func TestHandler(t *testing.T) {
	startTime := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	utils.Tag = "dev"
	TestValues := map[string]struct {
		Recorder        *httptest.ResponseRecorder
		Version         string
		ExpectedVersion string
	}{
		"Failure": {
			httptest.NewRecorder(),
			"dev",
			"dev",
		},
		"Success": {
			httptest.NewRecorder(),
			"dev",
			"dev",
		},
	}
	for key, val := range TestValues {
		switch key {
		case "Success":
			atomic.StoreUint32(&Queryable, uint32(1))
			NewUtilityAPIHandlers(startTime).heartbeat(val.Recorder, nil)
			hm := HeartbeatMessage{}
			err := json.NewDecoder(val.Recorder.Body).Decode(&hm)
			if err != nil {
				t.Fatal(err)
			}
			if hm.Version != val.ExpectedVersion {
				t.Error("Wrong version - Expected:", val.ExpectedVersion, "Got:", hm.Version)
			}
			assert.Equal(t, hm.Status, "queryable")
			assert.Equal(t, val.Recorder.Code, http.StatusOK)
		case "Failure":
			atomic.StoreUint32(&Queryable, uint32(0))
			NewUtilityAPIHandlers(startTime).heartbeat(val.Recorder, nil)
			hm := HeartbeatMessage{}
			err := json.NewDecoder(val.Recorder.Body).Decode(&hm)
			if err != nil {
				t.Fatal(err)
			}
			if hm.Version != val.ExpectedVersion {
				t.Error("Wrong version - Expected:", val.ExpectedVersion, "Got:", hm.Version)
			}
			assert.Equal(t, hm.Status, "not queryable")
			assert.Equal(t, val.Recorder.Code, http.StatusServiceUnavailable)
		}
	}
}
