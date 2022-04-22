package executor_test

import (
	"fmt"
	"math"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	. "github.com/alpacahq/marketstore/v4/catalog"
	"github.com/alpacahq/marketstore/v4/executor"
	. "github.com/alpacahq/marketstore/v4/planner"
	"github.com/alpacahq/marketstore/v4/utils"
	. "github.com/alpacahq/marketstore/v4/utils/io"
	. "github.com/alpacahq/marketstore/v4/utils/test"
)

func setup(t *testing.T) (rootDir string, itemsWritten map[string]int,
	metadata *executor.InstanceMetadata,
) {
	t.Helper()

	rootDir = t.TempDir()
	itemsWritten = MakeDummyCurrencyDir(rootDir, true, false)
	metadata, _, err := executor.NewInstanceSetup(rootDir, nil, nil, 5,
		executor.BackgroundSync(false))
	assert.Nil(t, err)

	return rootDir, itemsWritten, metadata
}

func TestAddDir(t *testing.T) {
	// --- given ---
	// make temporary catalog directory
	tempRootDir := t.TempDir()

	// make catelog directory
	catDir, err := NewDirectory(tempRootDir)
	var e ErrCategoryFileNotFound
	if err != nil && !errors.As(err, &e) {
		t.Fatal("failed to create a catalog dir.err=" + err.Error())
		return
	}

	year := int16(time.Now().Year())
	eNames := []string{"Bid", "Ask"}
	eTypes := []EnumElementType{FLOAT32, FLOAT32}
	dsv := NewDataShapeVector(eNames, eTypes)
	tbk := NewTimeBucketKey("TEST/1Min/TICKS")
	tf, err := tbk.GetTimeFrame()
	if err != nil {
		t.Log(err.Error())
		return
	}
	rt := EnumRecordTypeByName("variable")
	tbinfo := NewTimeBucketInfo(*tf, tbk.GetPathToYearFiles(tempRootDir), "Default", year, dsv, rt)

	// --- when a new time bucket is added to catalog directory ---
	err = catDir.AddTimeBucket(tbk, tbinfo)
	// --- then ---
	assert.Nil(t, err)

	// --- when a new catalog directory is made on the same root directory ---
	catDir, err = NewDirectory(tempRootDir)
	// --- then ---
	assert.Nil(t, err)

	assert.Equal(t, catDir.GetCategory(), "Symbol")
}

func TestQueryMulti(t *testing.T) {
	rootDir, _, metadata := setup(t)

	// Create a new variable data bucket
	tbk := NewTimeBucketKey("AAPL/1Min/OHLCV")
	tf := utils.TimeframeFromString("1Min")
	eTypes := []EnumElementType{FLOAT32, FLOAT32, FLOAT32, FLOAT32, INT32}
	eNames := []string{"Open", "High", "Low", "Close", "Volume"}
	dsv := NewDataShapeVector(eNames, eTypes)
	tbinfo := NewTimeBucketInfo(*tf, tbk.GetPathToYearFiles(rootDir), "Test", int16(2016), dsv, FIXED)
	err := metadata.CatalogDir.AddTimeBucket(tbk, tbinfo)
	assert.Nil(t, err)
	/*
		Write some data
	*/
	tbi, err := metadata.CatalogDir.GetLatestTimeBucketInfoFromKey(tbk)
	assert.Nil(t, err)
	writer, err := executor.NewWriter(metadata.CatalogDir, metadata.WALFile)
	assert.Nil(t, err)
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
		writer.WriteRecords([]time.Time{ts}, buffer, dsv, tbi)
	}
	assert.Nil(t, err)
	metadata.WALFile.FlushToWAL()
	metadata.WALFile.CreateCheckpoint()

	q := NewQuery(metadata.CatalogDir)
	q.AddRestriction("Timeframe", "1Min")
	q.SetRowLimit(LAST, 5)
	parsed, _ := q.Parse()
	reader, _ := executor.NewReader(parsed)
	csm, _ := reader.Read()
	assert.True(t, len(csm) >= 4)
	for _, cs := range csm {
		assert.True(t, cs.Len() <= 5)
	}
}

func TestWriteVariable(t *testing.T) {
	rootDir, _, metadata := setup(t)

	// Create a new variable data bucket
	tbk := NewTimeBucketKey("TEST-WV/1Min/TICK-BIDASK")
	tf := utils.TimeframeFromString("1Min")
	eTypes := []EnumElementType{FLOAT32, FLOAT32}
	eNames := []string{"Bid", "Ask"}
	dsv := NewDataShapeVector(eNames, eTypes)
	tbinfo := NewTimeBucketInfo(*tf, tbk.GetPathToYearFiles(rootDir), "Test", int16(2016), dsv, VARIABLE)
	err := metadata.CatalogDir.AddTimeBucket(tbk, tbinfo)
	assert.Nil(t, err)

	/*
		Write some data
	*/
	q := NewQuery(metadata.CatalogDir)
	q.AddRestriction("Symbol", "TEST-WV")
	q.AddRestriction("AttributeGroup", "TICK-BIDASK")
	q.AddRestriction("Timeframe", "1Min")
	q.SetStart(time.Date(2016, time.November, 1, 12, 0, 0, 0, time.UTC))
	parsed, _ := q.Parse()
	tbi, err := metadata.CatalogDir.GetLatestTimeBucketInfoFromKey(tbk)
	assert.Nil(t, err)
	writer, err := executor.NewWriter(metadata.CatalogDir, metadata.WALFile)
	assert.Nil(t, err)
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
		writer.WriteRecords([]time.Time{ts}, buffer, dsv, tbi)
	}
	assert.Nil(t, err)
	metadata.WALFile.FlushToWAL()
	metadata.WALFile.CreateCheckpoint()

	/*
		Read the data back
	*/
	reader, err := executor.NewReader(parsed)
	assert.Nil(t, err)
	csm, err := reader.Read()
	assert.Nil(t, err)
	assert.Len(t, csm, 1)
	for _, cs := range csm {
		epoch := cs.GetEpoch()
		nanos, ok := cs.GetByName("Nanoseconds").([]int32)
		assert.True(t, ok)
		assert.Equal(t, cs.Len(), 2)
		for i, ep := range epoch {
			checkSecs := inputTime[i].Unix()
			checkNanos := inputTime[i].Nanosecond()
			secs := nearestSecond(ep, nanos[i])
			// t.Log("ep, nanos, checkSecs, checkNanos =", ep, nanos[i], checkSecs, checkNanos)
			assert.Equal(t, checkSecs, secs)
			assert.True(t, math.Abs(float64(int32(checkNanos)-nanos[i])) < 100)
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
		writer.WriteRecords([]time.Time{ts}, buffer, dsv, tbi)
	}
	assert.Nil(t, err)
	metadata.WALFile.FlushToWAL()
	metadata.WALFile.CreateCheckpoint()

	csm, err = reader.Read()
	assert.Nil(t, err)
	assert.Len(t, csm, 1)
	for _, cs := range csm {
		assert.Equal(t, cs.Len(), 5)
		epoch := cs.GetEpoch()[2:]
		int32Nanos, ok := cs.GetByName("Nanoseconds").([]int32)
		require.True(t, ok)
		nanos := int32Nanos[2:]
		for i, ep := range epoch {
			checkSecs := inputTime[2+i].Unix()
			checkNanos := inputTime[2+i].Nanosecond()
			secs := nearestSecond(ep, nanos[i])
			//	t.Log("check, secs, nanos[i]: ", check, secs, nanos[i])
			assert.Equal(t, checkSecs, secs)
			assert.True(t, math.Abs(float64(int32(checkNanos)-nanos[i])) < 100)
		}
	}
	/*
		Write 100 records at a new timestamp
	*/
	ts = ts.Add(time.Minute)
	for ii := 0; ii < 100; ii++ {
		ts = ts.Add(time.Millisecond)
		row.Epoch = ts.Unix()
		inputTime = append(inputTime, ts)
		buffer, _ := Serialize([]byte{}, row)
		writer.WriteRecords([]time.Time{ts}, buffer, dsv, tbi)
	}
	assert.Nil(t, err)
	metadata.WALFile.FlushToWAL()
	metadata.WALFile.CreateCheckpoint()

	q = NewQuery(metadata.CatalogDir)
	q.AddRestriction("Symbol", "TEST-WV")
	q.AddRestriction("AttributeGroup", "TICK-BIDASK")
	q.AddRestriction("Timeframe", "1Min")
	q.SetRowLimit(LAST, 10)

	// Test last N query
	parsed, _ = q.Parse()
	reader, err = executor.NewReader(parsed)
	assert.Nil(t, err)
	csm, err = reader.Read()
	assert.Nil(t, err)
	for _, cs := range csm {
		t.Log("Results: ", cs)
		assert.Equal(t, cs.Len(), 10)
		assert.Equal(t, cs.GetEpoch()[9], row.Epoch)
		nanos, _ := cs.GetByName("Nanoseconds").([]int32)
		assert.True(t, math.Abs(float64(nanos[9]-600000000)) < 50., true)
		break
	}

	// Test first N query
	q.SetRowLimit(FIRST, 10)
	parsed, _ = q.Parse()
	reader, err = executor.NewReader(parsed)
	assert.Nil(t, err)
	csm, err = reader.Read()
	assert.Nil(t, err)
	for _, cs := range csm {
		t.Log("Results: ", cs)
		assert.Equal(t, cs.Len(), 10)
		assert.Equal(t, cs.GetEpoch()[9], row.Epoch)
		nanos, ok := cs.GetByName("Nanoseconds").([]int32)
		assert.True(t, ok)
		t.Log("Nanos: ", nanos)
		assert.True(t, math.Abs(float64(nanos[9]-505000000)) < 50., true)
		break
	}
}

func TestFileRead(t *testing.T) {
	_, itemsWritten, metadata := setup(t)

	q := NewQuery(metadata.CatalogDir)
	q.AddRestriction("Symbol", "NZDUSD")
	q.AddRestriction("AttributeGroup", "OHLC")
	q.AddRestriction("Timeframe", "1Min")
	q.SetRange(
		time.Date(2001, time.January, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2001, time.December, 31, 23, 59, 59, 0, time.UTC),
	)
	parsed, err := q.Parse()
	if err != nil {
		t.Fatalf("Failed to parse query: %s", err)
	}
	scanner, err := executor.NewReader(parsed)
	assert.Nil(t, err)
	// Sum up the total number of items in the query set for validation
	var nitems, recordlen int
	var minYear int16
	for _, iop := range scanner.IOPMap {
		assert.Len(t, iop.FilePlan, 1)
		for _, fp := range iop.FilePlan {
			year := int16(time.Unix(fp.BaseTime, 0).UTC().Year())
			if minYear == 0 {
				minYear = year
			} else if year < minYear {
				minYear = year
			}
			if year == 2001 {
				// t.Logf("File: %s Year: %d Number Written: %d\n", fp.FullPath, year, s.ItemsWritten[fp.FullPath])
				nitems += itemsWritten[fp.FullPath]
				recordlen = int(iop.RecordLen)
			}
		}
		assert.Equal(t, minYear, int16(2001))
		csm, _ := scanner.Read()
		/*
			for _, cs := range csm {
				epoch := cs.GetEpoch()
				t.Log("ResultSet Count, nitems, recordLen:", len(epoch), nitems, recordlen)
				printoutCandles(cs, 0, 0)
			}
		*/
		_, _ = csm, recordlen
	}
}

func TestDelete(t *testing.T) {
	_, _, metadata := setup(t)

	NY, _ := time.LoadLocation("America/New_York")
	// First write some data we can delete

	dataItemKey := "TEST-DELETE/OHLCV/1Min"
	dataItemPath := filepath.Join(metadata.CatalogDir.GetPath(), dataItemKey)
	dsv := NewDataShapeVector(
		[]string{"Open", "High", "Low", "Close"},
		[]EnumElementType{FLOAT32, FLOAT32, FLOAT32, FLOAT32},
	)
	tbi := NewTimeBucketInfo(*utils.TimeframeFromString("1Min"), dataItemPath, "Test item", 2018,
		dsv, FIXED)
	tbk := NewTimeBucketKey(dataItemKey)
	err := metadata.CatalogDir.AddTimeBucket(tbk, tbi)
	assert.Nil(t, err)

	writer, err := executor.NewWriter(metadata.CatalogDir, metadata.WALFile)
	assert.Nil(t, err)

	row := OHLCtest{0, 100., 200., 300., 400.}
	buffer, _ := Serialize([]byte{}, row)
	startTime := time.Date(2018, 12, 26, 9, 45, 0, 0, NY)
	ts := startTime
	tsA := make([]time.Time, 1000)
	for i := 0; i < 1000; i++ {
		minsToAdd := time.Duration(i)
		ts := ts.Add(minsToAdd * time.Minute)
		tsA[i] = ts
		buffer, _ = Serialize(buffer, row)
	}
	writer.WriteRecords(tsA, buffer, dsv, tbi)
	assert.Nil(t, err)
	metadata.WALFile.FlushToWAL()
	metadata.WALFile.CreateCheckpoint()

	endTime := tsA[len(tsA)-1]

	q := NewQuery(metadata.CatalogDir)
	q.AddTargetKey(tbk)
	q.SetRange(startTime.UTC(), endTime.UTC())
	parsed, err := q.Parse()
	if err != nil {
		t.Fatalf("Failed to parse query: %s", err)
	}

	// Read the data before delete
	r, err := executor.NewReader(parsed)
	assert.Nil(t, err)
	csm, err := r.Read()
	assert.Nil(t, err)
	for _, cs := range csm {
		if cs.Len() != 1000 {
			assert.Failf(t, "error: number of rows read back from write is incorrect",
				"should be: %d, was %d", 1000, cs.Len(),
			)
		}
		break
	}

	de, err := executor.NewDeleter(parsed)
	assert.Nil(t, err)
	err = de.Delete()
	asserter(t, err, true)
	err = de.Delete()
	asserter(t, err, true)

	// Read back the data, should have zero records
	csm, _ = r.Read()
	for _, cs := range csm {
		if cs.Len() != 0 {
			assert.Failf(t, "error: number of rows read back after delete is incorrect",
				"should be: %d, was %d", 0, cs.Len(),
			)
		}
		break
	}
}

func asserter(t *testing.T, err error, shouldBeNil bool) {
	t.Helper()

	if err != nil {
		t.Log("error: ", err.Error())
	}
	assert.Equal(t, err == nil, shouldBeNil)
}

func TestSortedFiles(t *testing.T) {
	_, itemsWritten, metadata := setup(t)

	q := NewQuery(metadata.CatalogDir)
	q.AddRestriction("Symbol", "NZDUSD")
	q.AddRestriction("AttributeGroup", "OHLC")
	//	q.AddRestriction("Symbol", "USDJPY")
	q.AddRestriction("Timeframe", "1Min")
	q.SetRange(
		time.Date(2001, time.January, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2003, time.January, 1, 0, 0, 0, 0, time.UTC),
	)
	parsed, err := q.Parse()
	if err != nil {
		t.Fatalf(fmt.Sprintf("Failed to parse query %s", err))
	}
	scanner, err := executor.NewReader(parsed)
	if err != nil {
		t.Log(err)
	}
	assert.Nil(t, err)
	// Sum up the total number of items in the query set for validation
	sortedFiles := executor.SortedFileList(parsed.QualifiedFiles)
	sort.Sort(sortedFiles)
	var nitems int
	for _, qf := range sortedFiles {
		if qf.File.Year >= 2001 && qf.File.Year <= 2002 {
			nitems += itemsWritten[qf.File.Path]
		}
	}
	assert.Len(t, sortedFiles, 3)
	assert.Equal(t, sortedFiles[0].File.Year, int16(2000))
	assert.Equal(t, sortedFiles[1].File.Year, int16(2001))
	assert.Equal(t, sortedFiles[2].File.Year, int16(2002))
	csm, err := scanner.Read()
	assert.Nil(t, err)
	for _, cs := range csm {
		epoch := cs.GetEpoch()
		assert.Len(t, epoch, nitems)
		// printoutCandles(cs, 0, 0)
	}

	// Test Limit Query - First N
	q = NewQuery(metadata.CatalogDir)
	q.AddRestriction("Symbol", "NZDUSD")
	q.AddRestriction("AttributeGroup", "OHLC")
	q.AddRestriction("Symbol", "USDJPY")
	q.AddRestriction("Timeframe", "1Min")
	q.SetRowLimit(FIRST, 200)
	parsed, err = q.Parse()
	if err != nil {
		t.Fatalf(fmt.Sprintf("Failed to parse query %s", err))
	}
	scanner, err = executor.NewReader(parsed)
	assert.Nil(t, err)
	csm, err = scanner.Read()
	assert.Nil(t, err)
	for _, cs := range csm {
		epoch := cs.GetEpoch()

		// printoutCandles(cs, 0, 0)
		// length := len(epoch)
		// printoutCandles(cs, length-1, length-1)

		// t.Logf("Length: %d\n", length)
		assert.Len(t, epoch, 200)
	}

	// Test Limit Query - Last N
	q = NewQuery(metadata.CatalogDir)
	q.AddRestriction("Symbol", "NZDUSD")
	q.AddRestriction("AttributeGroup", "OHLC")
	//	q.AddRestriction("Symbol", "USDJPY")
	q.AddRestriction("Timeframe", "1Min")
	q.SetRowLimit(LAST, 200)
	parsed, err = q.Parse()
	if err != nil {
		t.Fatalf(fmt.Sprintf("Failed to parse query %s", err))
	}
	scanner, err = executor.NewReader(parsed)
	assert.Nil(t, err)
	csm, err = scanner.Read()
	assert.Nil(t, err)
	for _, cs := range csm {
		epoch := cs.GetEpoch()
		assert.Len(t, epoch, 200)
	}

	// Test data range query - 5 min interval
	q = NewQuery(metadata.CatalogDir)
	q.AddRestriction("Symbol", "NZDUSD")
	q.AddRestriction("AttributeGroup", "OHLC")
	q.AddRestriction("Timeframe", "5Min")
	q.SetRange(
		time.Date(2001, time.January, 15, 12, 0, 0, 0, time.UTC),
		time.Date(2001, time.January, 15, 12, 5, 0, 0, time.UTC),
	)
	parsed, err = q.Parse()
	assert.Nil(t, err)
	scanner, err = executor.NewReader(parsed)
	assert.Nil(t, err)
	csm, err = scanner.Read()
	assert.Nil(t, err)
	for _, cs := range csm {
		epoch := cs.GetEpoch()
		// printoutCandles(cs, -1, -1)
		assert.Len(t, epoch, 2)
	}
}

func TestCrossYear(t *testing.T) {
	_, _, metadata := setup(t)

	// Test data range query - across year
	q := NewQuery(metadata.CatalogDir)
	q.AddRestriction("AttributeGroup", "OHLC")
	q.AddRestriction("Symbol", "USDJPY")
	q.AddRestriction("Timeframe", "5Min")
	startDate := time.Date(2001, time.October, 15, 12, 0, 0, 0, time.UTC)
	endDate := time.Date(2002, time.October, 15, 12, 5, 0, 0, time.UTC)
	q.SetRange(startDate, endDate)
	parsed, _ := q.Parse()
	scanner, err := executor.NewReader(parsed)
	assert.Nil(t, err)
	csm, _ := scanner.Read()
	for _, cs := range csm {
		epoch := cs.GetEpoch()
		// printoutCandles(cs, -1, 1)
		assert.Equal(t, time.Unix(epoch[0], 0).UTC(), startDate)
		assert.Equal(t, time.Unix(epoch[len(epoch)-1], 0).UTC(), endDate)
	}

	// Test Last N over year boundary
	forwardBackwardScan(t, 366, metadata.CatalogDir)
}

func TestLastN(t *testing.T) {
	_, _, metadata := setup(t)

	q := NewQuery(metadata.CatalogDir)
	q.AddRestriction("Symbol", "NZDUSD")
	q.AddRestriction("AttributeGroup", "OHLC")
	q.AddRestriction("Timeframe", "1Min")
	q.SetRange(
		time.Date(2001, time.January, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2001, time.December, 31, 23, 59, 59, 0, time.UTC),
	)
	q.SetRowLimit(LAST, 100)
	parsed, _ := q.Parse()
	scanner, err := executor.NewReader(parsed)
	assert.Nil(t, err)
	csm, _ := scanner.Read()
	for _, cs := range csm {
		epoch := cs.GetEpoch()
		//	printoutCandles(OHLCSlice, 0, -1)
		assert.Len(t, epoch, 100)
		endTime := time.Date(2001, time.December, 31, 23, 59, 0, 0, time.UTC)
		assert.Equal(t, epoch[99], endTime.Unix())
	}

	forwardBackwardScan(t, 201, metadata.CatalogDir)

	// Test with no data in bounding query
	/*
		//  Zero out the first day of data in the 2000.bin file inside NZDUSD/OHLC/1Min:
		filename := filepath.Join(RootDir, "NZDUSD", "1Min", "OHLC", "2000.bin")
		fmt.Println("Filename: ", filename)
		fp, err := os.OpenFile(filename, os.O_RDWR, 0600)
		c.Assert(err == nil, Equals, true)
		fp.Seek(io.Headersize, io.SeekStart)
		OneDayOfMinutes := int(24 * 60 * 24)
		buffer = make([]byte, OneDayOfMinutes)
		n, _ := fp.Write(buffer)
		c.Assert(n, Equals, OneDayOfMinutes)
		fp.Sync()
		fp.Close()
	*/

	// Query data with an end date of 1/1 asking for the last 10 rows
	q = NewQuery(metadata.CatalogDir)
	q.AddRestriction("Symbol", "NZDUSD")
	q.AddRestriction("AttributeGroup", "OHLC")
	q.AddRestriction("Timeframe", "1Min")
	q.SetRange(
		time.Date(1999, time.January, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC),
	)
	q.SetRowLimit(LAST, 10)
	parsed, _ = q.Parse()
	scanner, err = executor.NewReader(parsed)
	assert.Nil(t, err)
	csm, _ = scanner.Read()
	for _, cs := range csm {
		epoch := cs.GetEpoch()
		t.Log(epoch)
		assert.Len(t, epoch, 1)
	}

	// Query data with an end date of 12/31 asking for last 10 rows
	q = NewQuery(metadata.CatalogDir)
	q.AddRestriction("Symbol", "NZDUSD")
	q.AddRestriction("AttributeGroup", "OHLC")
	q.AddRestriction("Timeframe", "1Min")
	q.SetRange(
		time.Date(1999, time.January, 1, 0, 0, 0, 0, time.UTC),
		time.Date(1999, time.December, 23, 59, 0, 0, 0, time.UTC),
	)
	q.SetRowLimit(LAST, 10)
	parsed, _ = q.Parse()
	scanner, err = executor.NewReader(parsed)
	assert.Nil(t, err)
	csm, _ = scanner.Read()
	for _, cs := range csm {
		epoch := cs.GetEpoch()
		t.Log(epoch)
		assert.Len(t, epoch, 0)
	}

	// Query data with an end date of 1/1 asking for the last 10 rows
	q = NewQuery(metadata.CatalogDir)
	q.AddRestriction("Symbol", "NZDUSD")
	q.AddRestriction("AttributeGroup", "OHLC")
	q.AddRestriction("Timeframe", "1Min")
	q.SetRange(
		time.Date(1999, time.January, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2000, time.January, 1, 0, 1, 0, 0, time.UTC),
	)
	q.SetRowLimit(LAST, 10)
	parsed, _ = q.Parse()
	scanner, err = executor.NewReader(parsed)
	assert.Nil(t, err)
	csm, err = scanner.Read()
	assert.Nil(t, err)
	assert.False(t, csm.IsEmpty())
	for _, cs := range csm {
		epoch := cs.GetEpoch()
		t.Log(epoch)
		// printoutCandles(cs, 0, -1)
		assert.Len(t, epoch, 2)
	}
}

func TestAddSymbolThenWrite(t *testing.T) {
	_, _, metadata := setup(t)

	dataItemKey := "TEST/1Min/OHLCV"
	dataItemPath := filepath.Join(metadata.CatalogDir.GetPath(), dataItemKey)
	dsv := NewDataShapeVector(
		[]string{"Open", "High", "Low", "Close", "Volume"},
		[]EnumElementType{FLOAT32, FLOAT32, FLOAT32, FLOAT32, INT32},
	)
	tbinfo := NewTimeBucketInfo(*utils.TimeframeFromString("1Min"),
		dataItemPath,
		"Test item",
		2016,
		dsv, FIXED)
	tbk := NewTimeBucketKey(dataItemKey)
	err := metadata.CatalogDir.AddTimeBucket(tbk, tbinfo)
	assert.Nil(t, err)

	q := NewQuery(metadata.CatalogDir)
	q.AddRestriction("Symbol", "TEST")
	_, err = q.Parse()
	require.Nil(t, err)
	tbi, err := metadata.CatalogDir.GetLatestTimeBucketInfoFromKey(tbk)
	require.Nil(t, err)
	w, err := executor.NewWriter(metadata.CatalogDir, metadata.WALFile)
	require.Nil(t, err)
	ts := time.Now().UTC()
	row := OHLCVtest{0, 100., 200., 300., 400., 1000}
	buffer, _ := Serialize([]byte{}, row)
	err = w.WriteRecords([]time.Time{ts}, buffer, dsv, tbi)
	require.Nil(t, err)
	err = metadata.WALFile.FlushToWAL()
	assert.Nil(t, err)

	q = NewQuery(metadata.CatalogDir)
	q.AddRestriction("Symbol", "TEST")
	pr, _ := q.Parse()
	rd, err := executor.NewReader(pr)
	assert.Nil(t, err)
	columnSeries, err := rd.Read()
	assert.Nil(t, err)
	assert.True(t, len(columnSeries) != 0)
	for _, cs := range columnSeries {
		open, _ := cs.GetByName("Open").([]float32)
		high, _ := cs.GetByName("High").([]float32)
		low, _ := cs.GetByName("Low").([]float32)
		clos, _ := cs.GetByName("Close").([]float32)
		volume, _ := cs.GetByName("Volume").([]int32)
		assert.Equal(t, open[0], row.Open)
		assert.Equal(t, high[0], row.High)
		assert.Equal(t, low[0], row.Low)
		assert.Equal(t, clos[0], row.Close)
		assert.Equal(t, volume[0], row.Volume)
	}
}

func TestWriter(t *testing.T) {
	_, _, metadata := setup(t)

	dataItemKey := "TEST/1Min/OHLCV"
	tbk := NewTimeBucketKey(dataItemKey)
	dataItemPath := filepath.Join(metadata.CatalogDir.GetPath(), dataItemKey)
	dsv := NewDataShapeVector(
		[]string{"Open", "High", "Low", "Close", "Volume"},
		[]EnumElementType{FLOAT32, FLOAT32, FLOAT32, FLOAT32, INT32},
	)
	tbi := NewTimeBucketInfo(*utils.TimeframeFromString("1Min"),
		dataItemPath,
		"Test item",
		2016,
		dsv, FIXED)

	// needs to create a directory before writing data by WriteRecords function
	err := metadata.CatalogDir.AddTimeBucket(tbk, tbi)
	require.Nil(t, err)

	writer, err := executor.NewWriter(metadata.CatalogDir, metadata.WALFile)
	assert.Nil(t, err)
	ts := time.Now().UTC()
	row := OHLCtest{0, 100., 200., 300., 400.}
	buffer, _ := Serialize([]byte{}, row)
	err = writer.WriteRecords([]time.Time{ts}, buffer, tbi.GetDataShapes(), tbi)
	require.Nil(t, err)
	err = metadata.WALFile.FlushToWAL()
	require.Nil(t, err)
	err = metadata.WALFile.CreateCheckpoint()
	require.Nil(t, err)
}

/*
	===================== Helper Functions =================================
*/

func forwardBackwardScan(t *testing.T, numRecs int, d *Directory) {
	t.Helper()

	// First we grab records forward from a date
	endDate := time.Date(2002, time.December, 31, 1, 0, 0, 0, time.UTC)
	startDate := endDate.AddDate(0, 0, -numRecs+1)

	refColumnSet := NewColumnSeriesMap()

	q := NewQuery(d)
	q.AddRestriction("AttributeGroup", "OHLC")
	q.AddRestriction("Symbol", "USDJPY")
	q.AddRestriction("Timeframe", "1D")
	q.SetRange(startDate, endDate)
	q.SetRowLimit(FIRST, numRecs)
	parsed, _ := q.Parse()
	scanner, err := executor.NewReader(parsed)
	assert.Nil(t, err)
	csm, err := scanner.Read()
	for key, cs := range csm {
		assert.Nil(t, err)
		refColumnSet[key] = cs
		epoch := cs.GetEpoch()
		// fmt.Println("Total number of rows: ", len(epoch))
		assert.Len(t, epoch, numRecs)
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
	scanner, err = executor.NewReader(parsed)
	assert.Nil(t, err)
	csm, err = scanner.Read()
	for key, cs := range csm {
		assert.Nil(t, err)
		epoch := cs.GetEpoch()
		// fmt.Println("Total number of rows: ", len(epoch))
		assert.Len(t, epoch, numRecs)
		if !isEqual(refColumnSet[key], cs) {
			epoch, refEpoch := cs.GetEpoch(), refColumnSet[key].GetEpoch()
			for i, refTimestamp := range refEpoch {
				tstamp1 := time.Unix(refTimestamp, 0).UTC().Format(time.UnixDate)
				tstamp2 := time.Unix(epoch[i], 0).UTC().Format(time.UnixDate)
				t.Log("Should be: ", tstamp1, " Is: ", tstamp2)
			}
		}
	}
}

func isEqual(left, right *ColumnSeries) bool {
	if left.GetNumColumns() != right.GetNumColumns() {
		return false
	}

	for key, leftColumn := range left.GetColumns() {
		rightColumn := right.GetColumns()[key]
		if !reflect.DeepEqual(leftColumn, rightColumn) {
			return false
		}
	}
	return true
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

func nearestSecond(seconds int64, nanos int32) int64 {
	if nanos > 500000000 {
		return seconds + 1
	}
	return seconds
}
