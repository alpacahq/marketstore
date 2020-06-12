package planner

import (
	"testing"
	"time"

	. "gopkg.in/check.v1"

	. "github.com/alpacahq/marketstore/v4/catalog"
	. "github.com/alpacahq/marketstore/v4/utils/test"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }

type TestSuite struct {
	DataDirectory *Directory
	Rootdir       string
}

var _ = Suite(&TestSuite{nil, ""})

func (s *TestSuite) SetUpSuite(c *C) {
	s.Rootdir = c.MkDir()
	MakeDummyCurrencyDir(s.Rootdir, false, false)
	s.DataDirectory = NewDirectory(s.Rootdir)
}

func (s *TestSuite) TearDownSuite(c *C) {
	CleanupDummyDataDir(s.Rootdir)
}

func (s *TestSuite) TestQuery(c *C) {
	q := NewQuery(s.DataDirectory)
	q.AddRestriction("Symbol", "NZDUSD")
	q.AddRestriction("AttributeGroup", "OHLC")
	q.AddRestriction("Symbol", "USDJPY")
	q.AddRestriction("Timeframe", "1Min")
	q.SetRange(
		time.Date(2001, 1, 1, 12, 0, 0, 0, time.UTC),
		time.Date(2002, 12, 20, 12, 0, 0, 0, time.UTC),
	)
	pr, _ := q.Parse()
	c.Assert(len(pr.QualifiedFiles), Equals, 6)

	q = NewQuery(s.DataDirectory)
	q.AddRestriction("Symbol", "BBBYYY")
	pr, err := q.Parse()
	c.Assert(err != nil, Equals, true)
	c.Assert(len(pr.QualifiedFiles), Equals, 0)

	q = NewQuery(s.DataDirectory)
	q.AddRestriction("YYYYYY", "BBBYYY")
	pr, err = q.Parse()
	c.Assert(err != nil, Equals, true)

	q = NewQuery(s.DataDirectory)
	q.AddRestriction("AttributeGroup", "OHLC")
	pr, err = q.Parse()
	qfs := pr.QualifiedFiles
	c.Assert(len(qfs), Equals, 54)
}
