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
	metadata, _, _ := executor.NewInstanceSetup(s.Rootdir, nil, 5, true, true, false, true) // WAL Bypass
	s.DataDirectory = metadata.CatalogDir
	s.WALFile = metadata.WALFile
}

func (s *HandlersTestSuite) TearDownSuite(c *C) {}

func getTestTrade() string {
	return `{"T": "t","i": 96921,"S": "AAPL","x": "D","p": 126.55,"s": 1,"t": "2021-02-22T15:51:44.208Z","c": ["@","I"],"z": "C"}`
}
func getTestQuote() string {
	return `{"T": "q","S": "AMD","bx": "U","bp": 87.66,"bs": 1,"ax": "Q","ap": 87.68,"as": 4,"t": "2021-02-22T15:51:45.335689322Z","c": ["R"],"z": "C"}`
}
func getTestAggregate() string {
	return `{"T": "b","S": "SPY","o": 388.985,"h": 389.13,"l": 388.975,"c": 389.12,"v": 49378,"t": "2021-02-22T19:15:00Z"}`
}
func getTestMultiple() string {
	return getTestTrade() + "," + getTestQuote() + "," + getTestAggregate()
}
func getMessage(s string) []byte {
	return []byte("[" + s + "]")
}

func (s *HandlersTestSuite) TestHandlers(c *C) {
	// trade
	{
		MessageHandler(getMessage(getTestTrade()))
	}
	// quote
	{
		MessageHandler(getMessage(getTestQuote()))
	}
	// aggregate
	{
		MessageHandler(getMessage(getTestAggregate()))
	}
	// multiple
	{
		MessageHandler(getMessage(getTestMultiple()))
	}
}
