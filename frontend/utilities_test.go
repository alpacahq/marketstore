package frontend

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"time"

	"github.com/alpacahq/marketstore/v4/utils"
	. "gopkg.in/check.v1"
)

// Hook up gocheck into the "go test" runner.
type HeartbeatTestSuite struct{}

var _ = Suite(&HeartbeatTestSuite{})

func (s *HeartbeatTestSuite) TestHandler(c *C) {
	startTime := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	utils.Tag = "dev"
	var TestValues = map[string]struct {
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
				c.Fatal(err)
			}
			if hm.Version != val.ExpectedVersion {
				c.Error("Wrong version - Expected:", val.ExpectedVersion, "Got:", hm.Version)
			}
			c.Assert(hm.Status, Equals, "queryable")
			c.Assert(val.Recorder.Code, Equals, http.StatusOK)
		case "Failure":
			atomic.StoreUint32(&Queryable, uint32(0))
			NewUtilityAPIHandlers(startTime).heartbeat(val.Recorder, nil)
			hm := HeartbeatMessage{}
			err := json.NewDecoder(val.Recorder.Body).Decode(&hm)
			if err != nil {
				c.Fatal(err)
			}
			if hm.Version != val.ExpectedVersion {
				c.Error("Wrong version - Expected:", val.ExpectedVersion, "Got:", hm.Version)
			}
			c.Assert(hm.Status, Equals, "not queryable")
			c.Assert(val.Recorder.Code, Equals, http.StatusServiceUnavailable)
		}
	}
}
