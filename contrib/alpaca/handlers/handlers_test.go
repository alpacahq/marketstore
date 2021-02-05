package handlers

import (
	"testing"

	"github.com/alpacahq/marketstore/v4/catalog"

	"github.com/alpacahq/marketstore/v4/executor"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

var _ = Suite(&HandlersTestSuite{})

type HandlersTestSuite struct {
	DataDirectory *catalog.Directory
	Rootdir       string
	WALFile       *executor.WALFileType
}

func (s *HandlersTestSuite) SetUpSuite(c *C) {
	s.Rootdir = c.MkDir()
	metadata, _ := executor.NewInstanceSetup(s.Rootdir, nil, 5, true, true, false, true) // WAL Bypass
	s.DataDirectory = metadata.CatalogDir
	s.WALFile = metadata.WALFile
}

func (s *HandlersTestSuite) TearDownSuite(c *C) {}

func getTestTrade() []byte {
	return []byte(`{"data":{"ev":"T","T":"SPY","i":117537207,"x":2,"p":283.63,"s":2,"t":1587407015152775000,"c":[14, 37, 41],"z":2}}`)
}
func getTestQuote() []byte {
	return []byte(`{"data":{"ev":"Q","T":"SPY","x":17,"p":283.35,"s":1,"X":17,"P":283.4,"S":1,"c":[1],"t":1587407015152775000}}`)
}
func getTestAggregate() []byte {
	return []byte(`{"data":{"ev":"AM","T":"SPY","v":48526,"av":9663586,"op":282.6,"vw":282.0362,"o":282.13,"c":281.94,"h":282.14,"l":281.86,"a":284.4963,"s":1587409020000,"e":1587409080000}}`)
}
func (s *HandlersTestSuite) TestHandlers(c *C) {
	// trade
	{
		MessageHandler(getTestTrade())
	}
	// quote
	{
		MessageHandler(getTestQuote())
	}
	// aggregate
	{
		MessageHandler(getTestAggregate())
	}
}
