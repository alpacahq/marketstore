package frontend

import (
	"testing"

	. "gopkg.in/check.v1"

	"sync/atomic"

	"github.com/alpacahq/marketstore/v4/catalog"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/utils/test"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }

type ServerTestSuite struct {
	root    *catalog.Directory
	Rootdir string
}

var _ = Suite(&ServerTestSuite{nil, ""})

func (s *ServerTestSuite) SetUpSuite(c *C) {
	s.Rootdir = c.MkDir()
	//s.Rootdir = "/tmp/LALtemp"
	test.MakeDummyCurrencyDir(s.Rootdir, true, false)
	executor.NewInstanceSetup(s.Rootdir, true, true, false, false)
	atomic.StoreUint32(&Queryable, uint32(1))
}

func (s *ServerTestSuite) TearDownSuite(c *C) {
	test.CleanupDummyDataDir(s.Rootdir)
}

func (s *ServerTestSuite) TestNewServer(c *C) {
	serv, _ := NewServer()
	c.Check(serv.HasMethod("DataService.Query"), Equals, true)
}
