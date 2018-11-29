package executor

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"

	. "gopkg.in/check.v1"

	. "github.com/alpacahq/marketstore/catalog"
	. "github.com/alpacahq/marketstore/planner"
	"github.com/alpacahq/marketstore/utils"
	. "github.com/alpacahq/marketstore/utils/io"
	. "github.com/alpacahq/marketstore/utils/test"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }

var (
	_ = Suite(&TestSuite{nil, "", nil, nil})
	_ = Suite(&DestructiveWALTests{nil, "", nil, nil})
	_ = Suite(&DestructiveWALTest2{nil, "", nil, nil})
	_ = Suite(&CGOTests{})
)

type TestSuite struct {
	DataDirectory *Directory
	Rootdir       string
	// Number of items written in sample data (non-zero index)
	ItemsWritten map[string]int
	WALFile      *WALFileType
}

type DestructiveWALTests struct {
	DataDirectory *Directory
	Rootdir       string
	// Number of items written in sample data (non-zero index)
	ItemsWritten map[string]int
	WALFile      *WALFileType
}

type DestructiveWALTest2 struct {
	DataDirectory *Directory
	Rootdir       string
	// Number of items written in sample data (non-zero index)
	ItemsWritten map[string]int
	WALFile      *WALFileType
}

type CGOTests struct{}

func (s *TestSuite) SetUpSuite(c *C) {
	s.Rootdir = c.MkDir()
	s.ItemsWritten = MakeDummyCurrencyDir(s.Rootdir, true, false)
	NewInstanceSetup(s.Rootdir, true, true, false)
	s.DataDirectory = ThisInstance.CatalogDir
	s.WALFile = ThisInstance.WALFile
}

func (s *TestSuite) TearDownSuite(c *C) {
	CleanupDummyDataDir(s.Rootdir)
}

func (s *TestSuite) TestAddDir(c *C) {
	year := int16(time.Now().Year())
	eNames := []string{"Bid", "Ask"}
	eTypes := []EnumElementType{FLOAT32, FLOAT32}
	dsv := NewDataShapeVector(eNames, eTypes)

	tbk := NewTimeBucketKey("TEST/1Min/TICKS")

	tf, err := tbk.GetTimeFrame()
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	rt := EnumRecordTypeByName("variable")

	rootDir := c.MkDir()
	catDir := NewDirectory(rootDir)
	tbinfo := NewTimeBucketInfo(*tf, tbk.GetPathToYearFiles(rootDir), "Default", year, dsv, rt)
	err = catDir.AddTimeBucket(tbk, tbinfo)
	c.Assert(err == nil, Equals, true)
	catDir = NewDirectory(rootDir)
	c.Assert(catDir.GetCategory(), Equals, "Symbol")
}

func (s *TestSuite) TestQueryMulti(c *C) {
	// Create a new variable data bucket
	tbk := NewTimeBucketKey("AAPL/1Min/OHLCV")
	tf := utils.TimeframeFromString("1Min")
	eTypes := []EnumElementType{FLOAT32, FLOAT32, FLOAT32, FLOAT32, INT32}
	eNames := []string{"Open", "High", "Low", "Close", "Volume"}
	dsv := NewDataShapeVector(eNames, eTypes)
	tbinfo := NewTimeBucketInfo(*tf, tbk.GetPathToYearFiles(s.Rootdir), "Test", int16(2016), dsv, FIXED)
	err := ThisInstance.CatalogDir.AddTimeBucket(tbk, tbinfo)
	c.Assert(err, IsNil)
	tgc := ThisInstance.TXNPipe
	/*
		Write some data
	*/
	tbi, err := ThisInstance.CatalogDir.GetLatestTimeBucketInfoFromKey(tbk)
	c.Assert(err, IsNil)
	writer, err := NewWriter(tbi, tgc, s.DataDirectory)
	c.Assert(err == nil, Equals, true)
	row := struct {
		Epoch                  int64
		Open, High, Low, Close float32
		Volume                 int32
	}{0, 100, 200, 300, 400, 500}
	buffer, _ := Serialize([]byte{}, row)
	ts := time.Now().UTC()
	for ii := 0; ii < 10; ii++ {
		ts = ts.Add(time.Minute)
		row.Epoch = ts.Unix()
		writer.WriteRecords([]time.Time{ts}, buffer)
	}
	c.Assert(err == nil, Equals, true)
	s.WALFile.flushToWAL(tgc)
	s.WALFile.createCheckpoint()

	q := NewQuery(s.DataDirectory)
	q.AddRestriction("Timeframe", "1Min")
	q.SetRowLimit(LAST, 5)
	parsed, _ := q.Parse()
	reader, _ := NewReader(parsed)
	csm, _ := reader.Read()
	c.Assert(len(csm) >= 4, Equals, true)
	for _, cs := range csm {
		c.Assert(cs.Len() <= 5, Equals, true)
	}
}
func (s *TestSuite) TestWriteVariable(c *C) {
	// Create a new variable data bucket
	tbk := NewTimeBucketKey("TEST-WV/1Min/TICK-BIDASK")
	tf := utils.TimeframeFromString("1Min")
	eTypes := []EnumElementType{FLOAT32, FLOAT32}
	eNames := []string{"Bid", "Ask"}
	dsv := NewDataShapeVector(eNames, eTypes)
	tbinfo := NewTimeBucketInfo(*tf, tbk.GetPathToYearFiles(s.Rootdir), "Test", int16(2016), dsv, VARIABLE)
	err := ThisInstance.CatalogDir.AddTimeBucket(tbk, tbinfo)
	c.Assert(err, IsNil)
	tgc := ThisInstance.TXNPipe

	/*
		Write some data
	*/
	q := NewQuery(s.DataDirectory)
	q.AddRestriction("Symbol", "TEST-WV")
	q.AddRestriction("AttributeGroup", "TICK-BIDASK")
	q.AddRestriction("Timeframe", "1Min")
	q.SetStart(time.Date(2016, time.November, 1, 12, 0, 0, 0, time.UTC).Unix())
	parsed, _ := q.Parse()
	tbi, err := ThisInstance.CatalogDir.GetLatestTimeBucketInfoFromKey(tbk)
	c.Assert(err, IsNil)
	writer, err := NewWriter(tbi, tgc, s.DataDirectory)
	c.Assert(err == nil, Equals, true)
	row := struct {
		Epoch    int64
		Bid, Ask float32
	}{0, 100, 200}
	ts := time.Date(2016, time.December, 31, 2, 59, 18, 0, time.UTC)
	var inputTime []time.Time
	for ii := 0; ii < 2; ii++ {
		ts = ts.Add(250 * time.Millisecond)
		row.Epoch = ts.Unix()
		inputTime = append(inputTime, ts)
		buffer, _ := Serialize([]byte{}, row)
		writer.WriteRecords([]time.Time{ts}, buffer)
	}
	c.Assert(err == nil, Equals, true)
	s.WALFile.flushToWAL(tgc)
	s.WALFile.createCheckpoint()

	/*
		Read the data back
	*/
	reader, err := NewReader(parsed)
	c.Assert(err == nil, Equals, true)
	csm, err := reader.Read()
	c.Assert(err == nil, Equals, true)
	c.Assert(len(csm), Equals, 1)
	for _, cs := range csm {
		epoch := cs.GetEpoch()
		nanos := cs.GetByName("Nanoseconds").([]int32)
		c.Assert(cs.Len(), Equals, 2)
		for i, ep := range epoch {
			checkSecs := inputTime[i].Unix()
			checkNanos := inputTime[i].Nanosecond()
			secs := nearestSecond(ep, nanos[i])
			//fmt.Println("ep, nanos, checkSecs, checkNanos =", ep, nanos[i], checkSecs, checkNanos)
			c.Assert(checkSecs, Equals, secs)
			c.Assert(math.Abs(float64(int32(checkNanos)-nanos[i])) < 100, Equals, true)
		}
	}

	/*
		Write more data at a different timestamp
	*/
	ts = ts.Add(time.Minute)
	for ii := 0; ii < 3; ii++ {
		ts = ts.Add(time.Second)
		row.Epoch = ts.Unix()
		inputTime = append(inputTime, ts)
		buffer, _ := Serialize([]byte{}, row)
		writer.WriteRecords([]time.Time{ts}, buffer)
	}
	c.Assert(err == nil, Equals, true)
	s.WALFile.flushToWAL(tgc)
	s.WALFile.createCheckpoint()

	csm, err = reader.Read()
	c.Assert(err == nil, Equals, true)
	c.Assert(len(csm), Equals, 1)
	for _, cs := range csm {
		c.Assert(cs.Len(), Equals, 5)
		epoch := cs.GetEpoch()[2:]
		nanos := cs.GetByName("Nanoseconds").([]int32)[2:]
		for i, ep := range epoch {
			checkSecs := inputTime[2+i].Unix()
			checkNanos := inputTime[2+i].Nanosecond()
			secs := nearestSecond(ep, nanos[i])
			//			fmt.Println("check, secs, nanos[i]: ", check, secs, nanos[i])
			c.Assert(checkSecs, Equals, secs)
			c.Assert(math.Abs(float64(int32(checkNanos)-nanos[i])) < 100, Equals, true)
		}
	}
}
func (s *TestSuite) TestFileRead(c *C) {
	q := NewQuery(s.DataDirectory)
	q.AddRestriction("Symbol", "NZDUSD")
	q.AddRestriction("AttributeGroup", "OHLC")
	q.AddRestriction("Timeframe", "1Min")
	q.SetRange(
		time.Date(2001, time.January, 1, 0, 0, 0, 0, time.UTC).Unix(),
		time.Date(2001, time.December, 31, 23, 59, 59, 0, time.UTC).Unix(),
	)
	parsed, err := q.Parse()
	if err != nil {
		c.Fatalf(fmt.Sprintf("Failed to parse query"), err)
	}
	scanner, err := NewReader(parsed)
	c.Assert(err == nil, Equals, true)
	// Sum up the total number of items in the query set for validation
	var nitems, recordlen int
	var minYear int16
	for _, iop := range scanner.IOPMap {
		c.Assert(len(iop.FilePlan), Equals, 1)
		for _, fp := range iop.FilePlan {
			year := int16(time.Unix(fp.BaseTime, 0).UTC().Year())
			if minYear == 0 {
				minYear = year
			} else if year < minYear {
				minYear = year
			}
			if year == 2001 {
				//fmt.Printf("File: %s Year: %d Number Written: %d\n", fp.FullPath, year, s.ItemsWritten[fp.FullPath])
				nitems += s.ItemsWritten[fp.FullPath]
				recordlen = int(iop.RecordLen)
			}
		}
		c.Assert(minYear, Equals, int16(2001))
		csm, _ := scanner.Read()
		/*
			for _, cs := range csm {
				epoch := cs.GetEpoch()
				fmt.Println("ResultSet Count, nitems, recordLen:", len(epoch), nitems, recordlen)
				printoutCandles(cs, 0, 0)
			}
		*/
		_, _ = csm, recordlen
	}
}

func (s *TestSuite) TestSortedFiles(c *C) {
	q := NewQuery(s.DataDirectory)
	q.AddRestriction("Symbol", "NZDUSD")
	q.AddRestriction("AttributeGroup", "OHLC")
	//	q.AddRestriction("Symbol", "USDJPY")
	q.AddRestriction("Timeframe", "1Min")
	q.SetRange(
		time.Date(2001, time.January, 1, 0, 0, 0, 0, time.UTC).Unix(),
		time.Date(2003, time.January, 1, 0, 0, 0, 0, time.UTC).Unix(),
	)
	parsed, err := q.Parse()
	if err != nil {
		c.Fatalf(fmt.Sprintf("Failed to parse query %s", err))
	}
	scanner, err := NewReader(parsed)
	if err != nil {
		fmt.Println(err)
	}
	c.Assert(err == nil, Equals, true)
	// Sum up the total number of items in the query set for validation
	sortedFiles := SortedFileList(parsed.QualifiedFiles)
	sort.Sort(sortedFiles)
	var nitems int
	for _, qf := range sortedFiles {
		if qf.File.Year >= 2001 && qf.File.Year <= 2002 {
			nitems += s.ItemsWritten[qf.File.Path]
		}
	}
	c.Assert(len(sortedFiles), Equals, 3)
	c.Assert(sortedFiles[0].File.Year, Equals, int16(2000))
	c.Assert(sortedFiles[1].File.Year, Equals, int16(2001))
	c.Assert(sortedFiles[2].File.Year, Equals, int16(2002))
	csm, err := scanner.Read()
	for _, cs := range csm {
		epoch := cs.GetEpoch()
		c.Assert(len(epoch), Equals, nitems)
		//printoutCandles(cs, 0, 0)
	}

	// Test Limit Query - First N
	q = NewQuery(s.DataDirectory)
	q.AddRestriction("Symbol", "NZDUSD")
	q.AddRestriction("AttributeGroup", "OHLC")
	q.AddRestriction("Symbol", "USDJPY")
	q.AddRestriction("Timeframe", "1Min")
	q.SetRowLimit(FIRST, 200)
	parsed, err = q.Parse()
	if err != nil {
		c.Fatalf(fmt.Sprintf("Failed to parse query %s", err))
	}
	scanner, err = NewReader(parsed)
	c.Assert(err == nil, Equals, true)
	csm, err = scanner.Read()
	for _, cs := range csm {
		epoch := cs.GetEpoch()

		//printoutCandles(cs, 0, 0)
		//length := len(epoch)
		//printoutCandles(cs, length-1, length-1)

		//fmt.Printf("Length: %d\n", length)
		c.Assert(len(epoch) == 200, Equals, true)
	}

	// Test Limit Query - Last N
	q = NewQuery(s.DataDirectory)
	q.AddRestriction("Symbol", "NZDUSD")
	q.AddRestriction("AttributeGroup", "OHLC")
	//	q.AddRestriction("Symbol", "USDJPY")
	q.AddRestriction("Timeframe", "1Min")
	q.SetRowLimit(LAST, 200)
	parsed, err = q.Parse()
	if err != nil {
		c.Fatalf(fmt.Sprintf("Failed to parse query %s", err))
	}
	scanner, err = NewReader(parsed)
	c.Assert(err == nil, Equals, true)
	csm, err = scanner.Read()
	for _, cs := range csm {
		epoch := cs.GetEpoch()
		c.Assert(len(epoch) == 200, Equals, true)
	}

	// Test data range query - 5 min interval
	q = NewQuery(s.DataDirectory)
	q.AddRestriction("Symbol", "NZDUSD")
	q.AddRestriction("AttributeGroup", "OHLC")
	q.AddRestriction("Timeframe", "5Min")
	q.SetRange(
		time.Date(2001, time.January, 15, 12, 0, 0, 0, time.UTC).Unix(),
		time.Date(2001, time.January, 15, 12, 5, 0, 0, time.UTC).Unix(),
	)
	parsed, err = q.Parse()
	scanner, err = NewReader(parsed)
	c.Assert(err == nil, Equals, true)
	csm, err = scanner.Read()
	for _, cs := range csm {
		epoch := cs.GetEpoch()
		//printoutCandles(cs, -1, -1)
		c.Assert(len(epoch), Equals, 2)
	}
}

func (s *TestSuite) TestCrossYear(c *C) {
	// Test data range query - across year
	q := NewQuery(s.DataDirectory)
	q.AddRestriction("AttributeGroup", "OHLC")
	q.AddRestriction("Symbol", "USDJPY")
	q.AddRestriction("Timeframe", "5Min")
	startDate := time.Date(2001, time.October, 15, 12, 0, 0, 0, time.UTC)
	endDate := time.Date(2002, time.October, 15, 12, 5, 0, 0, time.UTC)
	q.SetRange(startDate.Unix(), endDate.Unix())
	parsed, _ := q.Parse()
	scanner, err := NewReader(parsed)
	c.Assert(err == nil, Equals, true)
	csm, _ := scanner.Read()
	for _, cs := range csm {
		epoch := cs.GetEpoch()
		//printoutCandles(cs, -1, 1)
		c.Assert(time.Unix(epoch[0], 0).UTC(), Equals, startDate)
		c.Assert(time.Unix(epoch[len(epoch)-1], 0).UTC(), Equals, endDate)
	}

	// Test Last N over year boundary
	forwardBackwardScan(366, s.DataDirectory, c)
}

func (s *TestSuite) TestLastN(c *C) {
	q := NewQuery(s.DataDirectory)
	q.AddRestriction("Symbol", "NZDUSD")
	q.AddRestriction("AttributeGroup", "OHLC")
	q.AddRestriction("Timeframe", "1Min")
	q.SetRange(
		time.Date(2001, time.January, 1, 0, 0, 0, 0, time.UTC).Unix(),
		time.Date(2001, time.December, 31, 23, 59, 59, 0, time.UTC).Unix(),
	)
	q.SetRowLimit(LAST, 100)
	parsed, _ := q.Parse()
	scanner, err := NewReader(parsed)
	c.Assert(err == nil, Equals, true)
	csm, _ := scanner.Read()
	for _, cs := range csm {
		epoch := cs.GetEpoch()
		//	printoutCandles(OHLCSlice, 0, -1)
		c.Assert(len(epoch), Equals, 100)
		endTime := time.Date(2001, time.December, 31, 23, 59, 0, 0, time.UTC)
		c.Assert(epoch[99], Equals, endTime.Unix())
	}

	forwardBackwardScan(201, s.DataDirectory, c)

	// Test with no data in bounding query
	/*
		//  Zero out the first day of data in the 2000.bin file inside NZDUSD/OHLC/1Min:
		filename := filepath.Join(RootDir, "NZDUSD", "1Min", "OHLC", "2000.bin")
		fmt.Println("Filename: ", filename)
		fp, err := os.OpenFile(filename, os.O_RDWR, 0600)
		c.Assert(err == nil, Equals, true)
		fp.Seek(io.Headersize, os.SEEK_SET)
		OneDayOfMinutes := int(24 * 60 * 24)
		buffer = make([]byte, OneDayOfMinutes)
		n, _ := fp.Write(buffer)
		c.Assert(n, Equals, OneDayOfMinutes)
		fp.Sync()
		fp.Close()
	*/

	// Query data with an end date of 1/1 asking for the last 10 rows
	q = NewQuery(s.DataDirectory)
	q.AddRestriction("Symbol", "NZDUSD")
	q.AddRestriction("AttributeGroup", "OHLC")
	q.AddRestriction("Timeframe", "1Min")
	q.SetRange(
		time.Date(1999, time.January, 1, 0, 0, 0, 0, time.UTC).Unix(),
		time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC).Unix(),
	)
	q.SetRowLimit(LAST, 10)
	parsed, _ = q.Parse()
	scanner, err = NewReader(parsed)
	c.Assert(err == nil, Equals, true)
	csm, _ = scanner.Read()
	for _, cs := range csm {
		epoch := cs.GetEpoch()
		c.Log(epoch)
		c.Assert(len(epoch), Equals, 1)
	}

	// Query data with an end date of 12/31 asking for last 10 rows
	q = NewQuery(s.DataDirectory)
	q.AddRestriction("Symbol", "NZDUSD")
	q.AddRestriction("AttributeGroup", "OHLC")
	q.AddRestriction("Timeframe", "1Min")
	q.SetRange(
		time.Date(1999, time.January, 1, 0, 0, 0, 0, time.UTC).Unix(),
		time.Date(1999, time.December, 23, 59, 0, 0, 0, time.UTC).Unix(),
	)
	q.SetRowLimit(LAST, 10)
	parsed, _ = q.Parse()
	scanner, err = NewReader(parsed)
	c.Assert(err == nil, Equals, true)
	csm, _ = scanner.Read()
	for _, cs := range csm {
		epoch := cs.GetEpoch()
		c.Log(epoch)
		c.Assert(len(epoch), Equals, 0)
	}

	// Query data with an end date of 1/1 asking for the last 10 rows
	q = NewQuery(s.DataDirectory)
	q.AddRestriction("Symbol", "NZDUSD")
	q.AddRestriction("AttributeGroup", "OHLC")
	q.AddRestriction("Timeframe", "1Min")
	q.SetRange(
		time.Date(1999, time.January, 1, 0, 0, 0, 0, time.UTC).Unix(),
		time.Date(2000, time.January, 1, 0, 1, 0, 0, time.UTC).Unix(),
	)
	q.SetRowLimit(LAST, 10)
	parsed, _ = q.Parse()
	scanner, err = NewReader(parsed)
	c.Assert(err, IsNil)
	csm, err = scanner.Read()
	c.Assert(err, IsNil)
	c.Assert(csm.IsEmpty(), Equals, false)
	for _, cs := range csm {
		epoch := cs.GetEpoch()
		c.Log(epoch)
		//printoutCandles(cs, 0, -1)
		c.Assert(len(epoch), Equals, 2)
	}
}

func (s *TestSuite) TestAddSymbolThenWrite(c *C) {
	d := ThisInstance.CatalogDir
	dataItemKey := "TEST/1Min/OHLCV"
	dataItemPath := filepath.Join(d.GetPath(), dataItemKey)
	dsv := NewDataShapeVector(
		[]string{"Open", "High", "Low", "Close", "Volume"},
		[]EnumElementType{FLOAT32, FLOAT32, FLOAT32, FLOAT32, INT32},
	)
	tbinfo := NewTimeBucketInfo(*utils.TimeframeFromString("1Min"), dataItemPath, "Test item", 2016,
		dsv, FIXED)
	tbk := NewTimeBucketKey(dataItemKey)
	err := d.AddTimeBucket(tbk, tbinfo)
	c.Assert(err, Equals, nil)

	q := NewQuery(d)
	q.AddRestriction("Symbol", "TEST")
	pr, _ := q.Parse()
	tbi, err := ThisInstance.CatalogDir.GetLatestTimeBucketInfoFromKey(tbk)
	c.Assert(err, IsNil)
	w, err := NewWriter(tbi, ThisInstance.TXNPipe, d)
	c.Assert(err, Equals, nil)
	ts := time.Now().UTC()
	row := OHLCVtest{0, 100., 200., 300., 400., 1000}
	buffer, _ := Serialize([]byte{}, row)
	w.WriteRecords([]time.Time{ts}, buffer)
	c.Assert(err == nil, Equals, true)
	err = ThisInstance.WALFile.flushToWAL(ThisInstance.TXNPipe)
	c.Assert(err == nil, Equals, true)

	q = NewQuery(d)
	q.AddRestriction("Symbol", "TEST")
	pr, _ = q.Parse()
	rd, err := NewReader(pr)
	c.Assert(err == nil, Equals, true)
	columnSeries, err := rd.Read()
	c.Assert(err == nil, Equals, true)
	c.Assert(len(columnSeries) != 0, Equals, true)
	for _, cs := range columnSeries {
		open := cs.GetByName("Open").([]float32)
		high := cs.GetByName("High").([]float32)
		low := cs.GetByName("Low").([]float32)
		close := cs.GetByName("Close").([]float32)
		volume := cs.GetByName("Volume").([]int32)
		c.Assert(open[0], Equals, row.Open)
		c.Assert(high[0], Equals, row.High)
		c.Assert(low[0], Equals, row.Low)
		c.Assert(close[0], Equals, row.Close)
		c.Assert(volume[0], Equals, row.Volume)
	}
}

func (s *TestSuite) TestWriter(c *C) {
	tgc := ThisInstance.TXNPipe
	dataItemKey := "TEST/1Min/OHLCV"
	tbk := NewTimeBucketKey(dataItemKey)
	tbi, err := ThisInstance.CatalogDir.GetLatestTimeBucketInfoFromKey(tbk)
	c.Assert(err, IsNil)
	writer, err := NewWriter(tbi, tgc, s.DataDirectory)
	c.Assert(err == nil, Equals, true)
	ts := time.Now().UTC()
	row := OHLCtest{0, 100., 200., 300., 400.}
	buffer, _ := Serialize([]byte{}, row)
	writer.WriteRecords([]time.Time{ts}, buffer)
	c.Assert(err == nil, Equals, true)
	s.WALFile.flushToWAL(tgc)
	s.WALFile.createCheckpoint()
}

func (s *DestructiveWALTests) SetUpSuite(c *C) {
	s.Rootdir = c.MkDir()
	s.ItemsWritten = MakeDummyCurrencyDir(s.Rootdir, true, false)
	NewInstanceSetup(s.Rootdir, true, true, false)
	s.DataDirectory = ThisInstance.CatalogDir
	s.WALFile = ThisInstance.WALFile
}

func (s *DestructiveWALTests) TearDownSuite(c *C) {
	CleanupDummyDataDir(s.Rootdir)
}

func (s *DestructiveWALTests) TestWALWrite(c *C) {
	var err error
	s.WALFile, err = NewWALFile(s.Rootdir, "")
	if err != nil {
		fmt.Println(err)
		c.Fail()
	}
	tgc := NewTransactionPipe()

	queryFiles, err := addTGData(s.DataDirectory, tgc, 1000, false)
	if err != nil {
		fmt.Println(err)
		c.Fail()
	}

	// Get the base files associated with this cache so that we can verify they remain correct after flush
	originalFileContents := createBufferFromFiles(queryFiles, c)

	err = s.WALFile.flushToWAL(tgc)
	if err != nil {
		fmt.Println(err)
	}
	// Verify that the file contents have not changed
	c.Assert(compareFileToBuf(originalFileContents, queryFiles, c), Equals, true)

	err = s.WALFile.createCheckpoint()
	if err != nil {
		fmt.Println(err)
	}
	// Verify that the file contents have not changed
	c.Assert(compareFileToBuf(originalFileContents, queryFiles, c), Equals, true)

	// Add some mixed up data to the cache
	queryFiles, err = addTGData(s.DataDirectory, tgc, 200, true)
	if err != nil {
		fmt.Println(err)
		c.Fail()
	}

	err = s.WALFile.flushToWAL(tgc)
	if err != nil {
		fmt.Println(err)
	}
	c.Assert(err == nil, Equals, true)

	// Old file contents should be different
	c.Assert(compareFileToBuf(originalFileContents, queryFiles, c), Equals, false)

	c.Assert(s.WALFile.IsOpen(), Equals, true)
	c.Assert(s.WALFile.CanWrite("WALTest"), Equals, true)
	s.WALFile.WriteStatus(OPEN, REPLAYED)

	if s.WALFile.CanDeleteSafely() {
		s.WALFile.Delete()
	}
	c.Assert(s.WALFile.IsOpen(), Equals, false)

}

func (s *DestructiveWALTests) TestBrokenWAL(c *C) {
	var err error

	tgc := NewTransactionPipe()

	// Add some mixed up data to the cache
	_, err = addTGData(s.DataDirectory, tgc, 1000, true)
	if err != nil {
		fmt.Println(err)
		c.Fail()
	}

	// Get the base files associated with this cache so that we can verify later
	// Note that at this point the files are unmodified
	//	originalFileContents := createBufferFromFiles(tgc, c)

	err = s.WALFile.flushToWAL(tgc)
	if err != nil {
		fmt.Println(err)
	}

	// Save the WALFile contents after WAL flush, but before flush to primary
	fstat, _ := s.WALFile.FilePtr.Stat()
	fsize := fstat.Size()
	WALFileAfterWALFlush := make([]byte, fsize)
	n, err := s.WALFile.FilePtr.ReadAt(WALFileAfterWALFlush, 0)
	c.Assert(int64(n), Equals, fsize)

	err = s.WALFile.createCheckpoint()
	if err != nil {
		fmt.Println(err)
	}

	// Now we have a completed WALFile, we can write some degraded files for testing
	fstat, _ = s.WALFile.FilePtr.Stat()
	WALLength := fstat.Size()
	WALBuffer := make([]byte, WALLength)
	n, err = s.WALFile.FilePtr.ReadAt(WALBuffer, 0)
	c.Assert(n == int(WALLength), Equals, true)
	c.Assert(err == nil, Equals, true)

	// We write a broken WAL File, but we need to replace the Owning PID with a bogus one before we write
	for i, val := range [8]byte{1, 1, 1, 1, 1, 1, 1, 1} {
		WALBuffer[3+i] = val
	}
	BrokenWAL := WALBuffer[:(3 * len(WALBuffer) / 4)]
	//BrokenWAL := WALBuffer[:]
	BrokenWALFilePath := s.Rootdir + "/BrokenWAL"
	os.Remove(BrokenWALFilePath)
	fp, err := os.OpenFile(BrokenWALFilePath, os.O_CREATE|os.O_RDWR, 0600)
	c.Assert(err == nil, Equals, true)
	_, err = fp.Write(BrokenWAL)
	c.Assert(err == nil, Equals, true)
	Syncfs()
	fp.Close()

	// Take over the broken WALFile and replay it
	WALFile, err := NewWALFile(s.Rootdir, BrokenWALFilePath)
	newTGC := NewTransactionPipe()
	c.Assert(newTGC != nil, Equals, true)
	c.Assert(WALFile.Replay(true) == nil, Equals, true)
	c.Assert(WALFile.Delete() == nil, Equals, true)
}

func (s *DestructiveWALTest2) SetUpSuite(c *C) {
	s.Rootdir = c.MkDir()
	s.ItemsWritten = MakeDummyCurrencyDir(s.Rootdir, true, false)
	NewInstanceSetup(s.Rootdir, true, true, false)
	s.DataDirectory = ThisInstance.CatalogDir
	s.WALFile = ThisInstance.WALFile
}

func (s *DestructiveWALTest2) TearDownSuite(c *C) {
	CleanupDummyDataDir(s.Rootdir)
}

func (s *DestructiveWALTest2) TestWALReplay(c *C) {
	var err error

	// Add some mixed up data to the cache
	tgc := NewTransactionPipe()

	allQueryFiles, err := addTGData(s.DataDirectory, tgc, 1000, true)
	if err != nil {
		fmt.Println(err)
		c.Fail()
	}
	// Filter out only year 2003 in the resulting file list
	queryFiles2002 := make([]string, 0)
	for _, filePath := range allQueryFiles {
		if filepath.Base(filePath) == "2002.bin" {
			queryFiles2002 = append(queryFiles2002, filePath)
		}
	}

	// Get the base files associated with this cache so that we can verify later
	// Note that at this point the files are unmodified
	allFileContents := createBufferFromFiles(queryFiles2002, c)
	fileContentsOriginal2002 := make(map[string][]byte, 0)
	for filePath, buffer := range allFileContents {
		if filepath.Base(filePath) == "2002.bin" {
			fileContentsOriginal2002[filePath] = buffer

		}
	}

	err = s.WALFile.flushToWAL(tgc)
	if err != nil {
		fmt.Println(err)
	}

	// Save the WALFile contents after WAL flush, but before checkpoint
	fstat, _ := s.WALFile.FilePtr.Stat()
	fsize := fstat.Size()
	WALFileAfterWALFlush := make([]byte, fsize)
	n, err := s.WALFile.FilePtr.ReadAt(WALFileAfterWALFlush, 0)
	c.Assert(int64(n) == fsize, Equals, true)

	err = s.WALFile.createCheckpoint()
	if err != nil {
		fmt.Println(err)
		c.FailNow()
	}
	// Put the modified files into a buffer and then verify the state of the files
	modifiedFileContents := createBufferFromFiles(queryFiles2002, c)
	c.Assert(compareFileToBuf(modifiedFileContents, queryFiles2002, c), Equals, true)

	// Verify that the file contents have changed for year 2002
	for key, buf := range fileContentsOriginal2002 {
		//fmt.Println("Key:", key, "Len1: ", len(buf), " Len2: ", len(modifiedFileContents[key]))
		c.Assert(bytes.Equal(buf, modifiedFileContents[key]), Equals, false)
	}

	// Re-write the original files
	//fmt.Println("Rewrite")
	rewriteFilesFromBuffer(fileContentsOriginal2002, c)
	// At this point, we should have our original files
	c.Assert(compareFileToBuf(fileContentsOriginal2002, queryFiles2002, c), Equals, true)

	// Write a WAL file with the pre-flushed state - we will replay this to get the modified files
	newWALFilePath := s.Rootdir + "/ReplayWAL"
	os.Remove(newWALFilePath) // Remove it if it exists
	fp, err := os.OpenFile(newWALFilePath, os.O_CREATE|os.O_RDWR, 0600)
	// Replace PID with a bogus PID
	for i, val := range [8]byte{1, 1, 1, 1, 1, 1, 1, 1} {
		WALFileAfterWALFlush[3+i] = val
	}
	n, err = fp.WriteAt(WALFileAfterWALFlush, 0)
	c.Assert(err == nil && n == len(WALFileAfterWALFlush), Equals, true)
	Syncfs()

	// Take over the new WALFile and replay it into a new TG cache
	WALFile, err := NewWALFile(s.Rootdir, newWALFilePath)
	data, _ := ioutil.ReadFile(newWALFilePath)
	ioutil.WriteFile("/tmp/wal", data, 0644)
	newTGC := NewTransactionPipe()
	c.Assert(newTGC != nil, Equals, true)
	// Verify that our files are in original state prior to replay
	c.Assert(compareFileToBuf(fileContentsOriginal2002, queryFiles2002, c), Equals, true)

	// Replay the WALFile into the new cache
	err = WALFile.Replay(true)
	c.Assert(err, IsNil)

	// Verify that the files are in the correct state after replay
	postReplayFileContents := createBufferFromFiles(queryFiles2002, c)
	for key, buf := range modifiedFileContents {
		if filepath.Base(key) == "2002.bin" {
			buf2 := postReplayFileContents[key]
			//fmt.Println("Key:", key, "Len1: ", len(buf), " Len2: ", len(buf2))
			if !bytes.Equal(buf, postReplayFileContents[key]) {
				for i, val := range buf {
					if val != buf2[i] {
						fmt.Println("Diff: pre/post:", buf[i:i+8], buf2[i:i+8])
						fmt.Println("Diff: pre/post int64:", ToInt64(buf[i:i+8]), ToInt64(buf2[i:i+8]))
						fmt.Println("Diff: pre/post float32:", ToFloat32(buf[i:i+4]), ToFloat32(buf2[i:i+4]))
						c.Assert(false, Equals, true)
					}
				}
			}
		}
	}
	// Final verify after replay
	c.Assert(compareFileToBuf(modifiedFileContents, queryFiles2002, c), Equals, true)
	c.Assert(WALFile.Delete() == nil, Equals, true)
}

/*
	===================== Helper Functions =================================
*/
func createBufferFromFiles(queryFiles []string, c *C) (originalFileContents map[string][]byte) {
	// Get the base files associated with this cache so that we can verify they remain correct after flush
	originalFileContents = make(map[string][]byte, 0)
	for _, filePath := range queryFiles {
		fp, err := os.OpenFile(filePath, os.O_RDONLY, 0600)
		c.Assert(err == nil, Equals, true)
		fstat, err := fp.Stat()
		c.Assert(err == nil, Equals, true)
		size := fstat.Size()
		originalFileContents[filePath] = make([]byte, size)
		_, err = fp.Read(originalFileContents[filePath])
		c.Assert(err == nil, Equals, true)
		//		fmt.Println("Read file ", filePath, " Size: ", n)
		fp.Close()
	}
	return originalFileContents
}

func rewriteFilesFromBuffer(originalFileContents map[string][]byte, c *C) {
	// Replace the file contents with the contents of the buffer
	for filePath, _ := range originalFileContents {
		fp, err := os.OpenFile(filePath, os.O_RDWR, 0600)
		c.Assert(err == nil, Equals, true)
		n, err := fp.WriteAt(originalFileContents[filePath], 0)
		c.Assert(err == nil && n == len(originalFileContents[filePath]), Equals, true)
		//		fmt.Println("Read file ", filePath, " Size: ", n)
		fp.Close()
	}
}

func compareFileToBuf(originalFileContents map[string][]byte, queryFiles []string, c *C) (isTheSame bool) {
	for _, filePath := range queryFiles {
		fp, err := os.OpenFile(filePath, os.O_RDONLY, 0600)
		c.Assert(err == nil, Equals, true)
		fstat, err := fp.Stat()
		c.Assert(err == nil, Equals, true)
		size := fstat.Size()
		content := make([]byte, size)
		_, err = fp.Read(content)
		c.Assert(err == nil, Equals, true)
		//		fmt.Println("Read original file ", filePath, " Size: ", n)
		fp.Close()
		if !bytes.Equal(content, originalFileContents[filePath]) {
			return false
		}
	}
	return true
}

func forwardBackwardScan(numRecs int, d *Directory, c *C) {
	// First we grab records forward from a date
	endDate := time.Date(2002, time.December, 31, 1, 0, 0, 0, time.UTC)
	startDate := endDate.AddDate(0, 0, -numRecs+1)

	RefColumnSet := NewColumnSeriesMap()

	q := NewQuery(d)
	q.AddRestriction("AttributeGroup", "OHLC")
	q.AddRestriction("Symbol", "USDJPY")
	q.AddRestriction("Timeframe", "1D")
	q.SetRange(startDate.Unix(), endDate.Unix())
	q.SetRowLimit(FIRST, numRecs)
	parsed, _ := q.Parse()
	scanner, err := NewReader(parsed)
	c.Assert(err == nil, Equals, true)
	csm, err := scanner.Read()
	for key, cs := range csm {
		c.Assert(err == nil, Equals, true)
		RefColumnSet[key] = cs
		epoch := cs.GetEpoch()
		//fmt.Println("Total number of rows: ", len(epoch))
		c.Assert(len(epoch), Equals, numRecs)
	}

	q = NewQuery(d)
	q.AddRestriction("AttributeGroup", "OHLC")
	q.AddRestriction("Symbol", "USDJPY")
	q.AddRestriction("Timeframe", "1D")
	/*
		startDate = time.Date(2001, time.October, 15, 12, 0, 0, 0, time.UTC)
		endDate = time.Date(2002, time.October, 15, 12, 5, 0, 0, time.UTC)
		q.SetRange(startDate, endDate)
	*/
	q.SetRowLimit(LAST, numRecs)
	parsed, _ = q.Parse()
	scanner, err = NewReader(parsed)
	c.Assert(err == nil, Equals, true)
	csm, err = scanner.Read()
	for key, cs := range csm {
		c.Assert(err == nil, Equals, true)
		epoch := cs.GetEpoch()
		//fmt.Println("Total number of rows: ", len(epoch))
		c.Assert(len(epoch), Equals, numRecs)
		if !isEqual(RefColumnSet[key], cs) {
			epoch, r_epoch := cs.GetEpoch(), RefColumnSet[key].GetEpoch()
			for i, r_ts := range r_epoch {
				tstamp1 := time.Unix(r_ts, 0).UTC().Format(time.UnixDate)
				tstamp2 := time.Unix(epoch[i], 0).UTC().Format(time.UnixDate)
				fmt.Println("Should be: ", tstamp1, " Is: ", tstamp2)
			}
		}
	}
}

func isEqual(left, right *ColumnSeries) bool {
	if left.GetNumColumns() != right.GetNumColumns() {
		return false
	}

	for key, l_column := range left.GetColumns() {
		r_column := right.GetColumns()[key]
		if !reflect.DeepEqual(l_column, r_column) {
			return false
		}
	}
	return true
}

func addTGData(root *Directory, tgc *TransactionPipe, number int, mixup bool) (queryFiles []string, err error) {
	// Need a local version of this to avoid an import cycle
	type CurrencyData struct {
		Epoch                  int64
		Open, High, Low, Close float32
	}

	// Create some data via a query
	symbols := []string{"NZDUSD", "USDJPY", "EURUSD"}
	writerByKey := make(map[TimeBucketKey]*Writer, 0)
	csm := NewColumnSeriesMap()
	queryFiles = make([]string, 0)

	for _, sym := range symbols {
		q := NewQuery(root)
		q.AddRestriction("Symbol", sym)
		q.AddRestriction("AttributeGroup", "OHLC")
		q.AddRestriction("Timeframe", "1Min")
		q.SetRowLimit(LAST, number)
		parsed, _ := q.Parse()
		scanner, err := NewReader(parsed)
		if err != nil {
			fmt.Printf("Failed to create a new reader")
			return nil, err
		}

		csmSym, err := scanner.Read()
		if err != nil {
			fmt.Printf("scanner.Read failed: Err: %s", err)
			return nil, err
		}

		for key, cs := range csmSym {
			// Add this result data to the overall
			csm[key] = cs
			tbi, err := ThisInstance.CatalogDir.GetLatestTimeBucketInfoFromKey(&key)
			writerByKey[key], err = NewWriter(tbi, tgc, root)
			if err != nil {
				fmt.Printf("Failed to create a new writer")
				return nil, err
			}
		}
		for _, iop := range scanner.IOPMap {
			for _, iofp := range iop.FilePlan {
				queryFiles = append(queryFiles, iofp.FullPath)
			}
		}
	}

	// Write the data to the TG cache
	for key, cs := range csm {
		epoch := cs.GetEpoch()
		open := cs.GetByName("Open").([]float32)
		high := cs.GetByName("High").([]float32)
		low := cs.GetByName("Low").([]float32)
		close := cs.GetByName("Close").([]float32)
		// If we have the mixup flag set, change the data
		if mixup {
			asize := len(epoch)
			for i := 0; i < asize/2; i++ {
				ii := (asize - 1) - i
				time1 := epoch[i]
				epoch[i] = epoch[ii]
				epoch[ii] = time1
				open[i] = float32(-1 * i)
				high[i] = float32(-2 * ii)
				low[i] = -3
				close[i] = -4
			}
		}
		timestamps := make([]time.Time, len(epoch))
		var buffer []byte
		for i := range epoch {
			timestamps[i] = time.Unix(epoch[i], 0).UTC()
			buffer = append(buffer, DataToByteSlice(epoch[i])...)
			buffer = append(buffer, DataToByteSlice(open[i])...)
			buffer = append(buffer, DataToByteSlice(high[i])...)
			buffer = append(buffer, DataToByteSlice(low[i])...)
			buffer = append(buffer, DataToByteSlice(close[i])...)
		}
		writerByKey[key].WriteRecords(timestamps, buffer)
	}

	return queryFiles, nil
}

type OHLCtest struct {
	Epoch                  int64
	Open, High, Low, Close float32
}

type OHLCVtest struct {
	Epoch                  int64
	Open, High, Low, Close float32
	Volume                 int32
}

type testOHLC struct {
	timestamp              int64
	open, high, low, close float32
}

type testOHLCV struct {
	Timestamp              int64
	Open, High, Low, Close float32
	Volume                 int32
}

func nearestSecond(seconds int64, nanos int32) int64 {
	if nanos > 500000000 {
		return seconds + 1
	}
	return seconds
}
