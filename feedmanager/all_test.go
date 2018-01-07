package feedmanager

import (
	"fmt"
	"os/exec"
	"runtime"
	"testing"
	"time"

	"github.com/alpacahq/marketstore/executor"
	"github.com/alpacahq/marketstore/utils"
	"github.com/alpacahq/marketstore/utils/io"
	. "gopkg.in/check.v1"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }

type TestSuite struct {
	TestPluginLib string
}

var _ = Suite(&TestSuite{})

func (s *TestSuite) SetUpSuite(c *C) {
	/*
		Build a shared library for testing
	*/
	osType := runtime.GOOS
	if osType != "linux" {
		c.Skip("Only linux runs plugins")
	}
	var testPluginSrc = "testplugin/testplugin.go"
	s.TestPluginLib = "./testplugin.so"
	cmd := exec.Command("go",
		"build",
		"-buildmode=plugin",
		"-o",
		s.TestPluginLib,
		testPluginSrc)
	fmt.Println("About to execute: ", cmd.Args)
	err := cmd.Run()
	if err != nil {
		fmt.Println(err)
		c.Skip("Unable to build test plugin ** is go version > 1.9 in your path?")
	}
	fmt.Println("Succeeded in building test plugin")

	executor.NewInstanceSetup(c.MkDir(), true, true, false, true)
}

func (s *TestSuite) TestPluginLoading(c *C) {
	var err error
	_, err = OpenPluginInGOPATH(s.TestPluginLib)
	if err != nil {
		fmt.Println(err)
	}
	c.Assert(err == nil, Equals, true)
	_, err = OpenPluginInGOPATH("@@@badpluginname@@@")
	fmt.Println("error: ", err)
	c.Assert(err != nil, Equals, true)
}

func (s *TestSuite) TestPluginAPI(c *C) {
	var err error
	d := executor.ThisInstance.CatalogDir

	itemKey := "TESTFEED/1Min/OHLCV"
	catKey := "Symbol/Timeframe/AttributeGroup"

	tbk := io.NewTimeBucketKey(itemKey, catKey)

	fd, err := NewFeed(s.TestPluginLib, "http://foo.com/", d, []*io.TimeBucketKey{tbk}, false)
	if err != nil {
		fmt.Println(err)
	}

	csm, err := fd.Datafeed.Poll(fd.FeedState, nil)
	if err != nil {
		fmt.Println(err)
	}
	c.Assert(err == nil, Equals, true)
	c.Assert(csm[*tbk].Len(), Equals, 1)
	cs := csm[*tbk]

	/*
		Write the row
	*/
	row := cs.ToRowSeries(*tbk).GetData()
	timestamp := time.Unix(io.ToInt64(row[:8]), 0).UTC()
	fd.Writers[0].WriteRecords([]time.Time{timestamp}, row)
	c.Assert(err == nil, Equals, true)
}

func (s *TestSuite) TestFeedRunner(c *C) {
	pluginName := s.TestPluginLib
	baseURL := ""
	d := executor.ThisInstance.CatalogDir
	destinations := []*io.TimeBucketKey{
		io.NewTimeBucketKey(
			"TSLA/1Min/OHLCV",
			"Symbol/Timeframe/AttributeGroup",
		),
	}
	isVariable := false

	fd, err := NewFeed(pluginName, baseURL, d, destinations, isVariable)
	if err != nil {
		fmt.Println(err)
		c.Fail()
	}

	pollFrequency := 500 * time.Millisecond
	pid, err := PollFeed(pollFrequency, fd)
	if err != nil {
		fmt.Println("Error: ", err)
		c.Fail()
	}
	time.Sleep(2 * time.Second)
	proc := utils.GetProcFromPID(pid)
	proc.Kill()
}
