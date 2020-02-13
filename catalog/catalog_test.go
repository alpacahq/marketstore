package catalog

import (
	"fmt"
	"path"
	"testing"

	. "gopkg.in/check.v1"

	"os"
	"path/filepath"

	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/io"
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
	//	s.Rootdir = "/tmp/LALTest"
	//	os.Mkdir(s.Rootdir, 0770)
	MakeDummyCurrencyDir(s.Rootdir, false, false)
	s.DataDirectory = NewDirectory(s.Rootdir)
}

func (s *TestSuite) TearDownSuite(c *C) {
	CleanupDummyDataDir(s.Rootdir)
}

func (s *TestSuite) TestGetDirectMap(c *C) {
	/*
		for key, _ := range s.DataDirectory.directMap {
			fmt.Println(key)
		}
	*/
	c.Assert(len(s.DataDirectory.directMap), Equals, 18)
}

func (s *TestSuite) TestGetCatList(c *C) {
	CatList := s.DataDirectory.GatherCategoriesFromCache()
	c.Assert(len(CatList), Equals, 4)
}
func (s *TestSuite) TestGetCatItemMap(c *C) {
	catList := s.DataDirectory.GatherCategoriesAndItems()
	/*
		for key, list := range catList {
			fmt.Printf("Category: %s: {", key)
			var i int
			for name, _ := range list {
				fmt.Printf("%s", name)
				if i != len(list)-1 {
					fmt.Printf(",")
				}
				i++
			}
			fmt.Printf("}\n")
		}
	*/
	c.Assert(len(catList), Equals, 4)
}
func (s *TestSuite) TestGetDirList(c *C) {
	dirList := s.DataDirectory.gatherDirectories()
	c.Assert(len(dirList), Equals, 40)
}
func (s *TestSuite) TestGatherFilePaths(c *C) {
	filePathList := s.DataDirectory.gatherFilePaths()
	//	for _, filePath := range filePathList {
	//		fmt.Printf("File Path: %s\n",filePath)
	//	}
	c.Assert(len(filePathList), Equals, 54)
}
func (s *TestSuite) TestGatherFileInfo(c *C) {
	fileInfoList := s.DataDirectory.GatherTimeBucketInfo()
	//for _, fileInfo := range fileInfoList {
	//	fmt.Printf("File Path: %s Year: %d\n",fileInfo.Path,fileInfo.Year)
	//}
	c.Assert(len(fileInfoList), Equals, 54)
}
func (s *TestSuite) TestPathToFileInfo(c *C) {
	fileInfo, err := s.DataDirectory.PathToTimeBucketInfo("nil")
	if err != nil {
		if _, ok := err.(NotFoundError); ok {
			c.Assert(fileInfo, Equals, (*io.TimeBucketInfo)(nil))
		} else {
			c.Fail()
		}
	}
	mypath := s.Rootdir + "/EURUSD/1Min/OHLC/2001.bin"
	fileInfo, err = s.DataDirectory.PathToTimeBucketInfo(mypath)
	if err != nil {
		fmt.Println(err)
	}
	c.Assert(err == nil, Equals, true)
	c.Assert(fileInfo.Path, Equals, mypath)
}
func (s *TestSuite) TestAddFile(c *C) {
	d := NewDirectory(s.Rootdir)
	// Get the owning subdirectory for a test file path
	filePathList := d.gatherFilePaths()
	filePath := filePathList[0]
	// fmt.Println(filePath)
	subDir, err := d.GetOwningSubDirectory(filePath)
	if err != nil {
		fmt.Println(err)
	}
	c.Assert(err == nil, Equals, true)
	// fmt.Println(subDir.GetPath())
	// Get the latest year file in subdir
	_, err = subDir.getLatestYearFile()
	c.Assert(err == nil, Equals, true)
	// latestFile, err := subDir.getLatestYearFile()
	// fmt.Println(latestFile.Path)
	if _, err = subDir.AddFile(int16(2016)); err != nil {
		fmt.Println(err)
	}
	c.Assert(err == nil, Equals, true)
	_, err = subDir.getLatestYearFile()
	c.Assert(err == nil, Equals, true)
	// fmt.Println("New Latest Year:", latestFile.Year, latestFile.Path)
}

func (s *TestSuite) TestAddAndRemoveDataItem(c *C) {
	d := NewDirectory(s.Rootdir)
	catKey := "Symbol/Timeframe/AttributeGroup"
	dataItemKey := "TEST/1Min/OHLCV"
	dataItemPath := filepath.Join(s.Rootdir, dataItemKey)
	dsv := io.NewDataShapeVector(
		[]string{"Open", "High", "Low", "Close", "Volume"},
		[]io.EnumElementType{io.FLOAT32, io.FLOAT32, io.FLOAT32, io.FLOAT32, io.INT32},
	)
	tbinfo := io.NewTimeBucketInfo(*utils.TimeframeFromString("1Min"), dataItemPath, "Test item", 2016,
		dsv, io.FIXED)

	tbk := io.NewTimeBucketKey(dataItemKey, catKey)
	err := d.AddTimeBucket(tbk, tbinfo)
	c.Assert(err, Equals, nil)
	catList := d.GatherCategoriesAndItems()
	_, ok := catList["Symbol"]["TEST"]
	c.Assert(ok, Equals, true)
	// Construct the known new path to this subdirectory so that we can verify it is in the catalog

	oldFilePath := path.Join(s.Rootdir, "EURUSD", "1Min", "OHLC", "2000.bin")
	_, err = d.GetOwningSubDirectory(oldFilePath)
	c.Assert(err == nil, Equals, true)

	newFilePath := path.Join(s.Rootdir, "TEST", "1Min", "OHLCV", "2016.bin")
	_, err = d.GetOwningSubDirectory(newFilePath)
	c.Assert(err == nil, Equals, true)

	err = d.RemoveTimeBucket(tbk)
	if err != nil {
		fmt.Println(err)
	}
	c.Assert(err == nil, Equals, true)
	catList = d.GatherCategoriesAndItems()
	_, ok = catList["Symbol"]["TEST"]
	c.Assert(ok, Equals, false)
	npath := newFilePath
	c.Assert(exists(npath), Equals, false)
	npath = path.Join(s.Rootdir, "TEST", "1Min", "OHLCV")
	c.Assert(exists(npath), Equals, false)
	npath = path.Join(s.Rootdir, "TEST", "1Min")
	c.Assert(exists(npath), Equals, false)
	npath = path.Join(s.Rootdir, "TEST")
	c.Assert(exists(npath), Equals, false)
	npath = path.Join(s.Rootdir, "EURUSD")
	c.Assert(exists(npath), Equals, true)

	/*
		Test using an empty root directory
	*/
	rootDir := c.MkDir()
	d = NewDirectory(rootDir)

	dataItemPath = filepath.Join(rootDir, dataItemKey)
	dsv = io.NewDataShapeVector(
		[]string{"Open", "High", "Low", "Close", "Volume"},
		[]io.EnumElementType{io.FLOAT32, io.FLOAT32, io.FLOAT32, io.FLOAT32, io.INT32},
	)
	tbinfo = io.NewTimeBucketInfo(*utils.TimeframeFromString("1Min"), dataItemPath, "Test item", 2016,
		dsv, io.FIXED)

	err = d.AddTimeBucket(tbk, tbinfo)
	c.Assert(err, Equals, nil)

	catList = d.GatherCategoriesAndItems()
	_, ok = catList["Symbol"]["TEST"]
	c.Assert(ok, Equals, true)
	newFilePath = path.Join(rootDir, "TEST", "1Min", "OHLCV", "2016.bin")
	npath = newFilePath
	c.Assert(exists(npath), Equals, true)
	npath = path.Join(rootDir, "TEST", "1Min", "OHLCV")
	c.Assert(exists(npath), Equals, true)
	npath = path.Join(rootDir, "TEST", "1Min")
	c.Assert(exists(npath), Equals, true)
	npath = path.Join(rootDir, "TEST")
	c.Assert(exists(npath), Equals, true)
	// fmt.Println(catList)

	/*
		Test ADD + ADD of the same symbol - should throw an error
	*/
	dataItemKey = "TEST2/1Min/OHLCV"
	dataItemPath = filepath.Join(rootDir, dataItemKey)
	dsv = io.NewDataShapeVector(
		[]string{"Open", "High", "Low", "Close", "Volume"},
		[]io.EnumElementType{io.FLOAT32, io.FLOAT32, io.FLOAT32, io.FLOAT32, io.INT32},
	)
	tbinfo = io.NewTimeBucketInfo(*utils.TimeframeFromString("1Min"), dataItemPath, "Test item", 2016,
		dsv, io.FIXED)
	tbk = io.NewTimeBucketKey(dataItemKey, catKey)
	err = d.AddTimeBucket(tbk, tbinfo)
	c.Assert(err, Equals, nil)
	catList = d.GatherCategoriesAndItems()
	_, ok = catList["Symbol"]["TEST2"]
	c.Assert(ok, Equals, true)

	// This should fail
	err = d.AddTimeBucket(tbk, tbinfo)
	c.Assert(err != nil, Equals, true)

	// Now let's remove the symbol and then re-add it - should work
	err = d.RemoveTimeBucket(tbk)
	if err != nil {
		fmt.Println(err)
	}
	c.Assert(err, Equals, nil)
	err = d.AddTimeBucket(tbk, tbinfo)
	c.Assert(err, Equals, nil)

	// Sometimes people may call AddTimeBucket with an empty directory, let's test that
	err = d.RemoveTimeBucket(tbk)
	if err != nil {
		fmt.Println(err)
	}
	c.Assert(err, Equals, nil)
	err = d.AddTimeBucket(tbk, tbinfo)
	c.Assert(err, Equals, nil)

	// Let's try two subsequent RemoveTimeBucket calls, the first should work, the second should err
	err = d.RemoveTimeBucket(tbk)
	if err != nil {
		fmt.Println(err)
	}
	c.Assert(err, Equals, nil)
	err = d.RemoveTimeBucket(tbk)
	if err != nil {
		fmt.Println(err)
	}
	c.Assert(err != nil, Equals, true)
}

func (s *TestSuite) TestCreateNewDirectory(c *C) {
	newRootDir := c.MkDir()

	d := NewDirectory(newRootDir)

	catKey := "Symbol/Timeframe/AttributeGroup"
	dataItemKey := "TEST/1Min/OHLCV"
	dataItemPath := filepath.Join(newRootDir, dataItemKey)
	dsv := io.NewDataShapeVector(
		[]string{"Open", "High", "Low", "Close", "Volume"},
		[]io.EnumElementType{io.FLOAT32, io.FLOAT32, io.FLOAT32, io.FLOAT32, io.INT32},
	)
	tbinfo := io.NewTimeBucketInfo(*utils.TimeframeFromString("1Min"), dataItemPath, "Test item", 2016,
		dsv, io.FIXED)

	tbk := io.NewTimeBucketKey(dataItemKey, catKey)
	err := d.AddTimeBucket(tbk, tbinfo)
	c.Assert(err, Equals, nil)
	catList := d.GatherCategoriesAndItems()
	_, ok := catList["Symbol"]["TEST"]
	c.Assert(ok, Equals, true)

	// Construct the known new path to this subdirectory so that we can verify it is in the catalog
	newFilePath := path.Join(newRootDir, "TEST", "1Min", "OHLCV", "2016.bin")
	_, err = d.GetOwningSubDirectory(newFilePath)
	c.Assert(err == nil, Equals, true)
}

func exists(path string) bool {
	_, err := os.Stat(path)
	if err == nil {
		return true
	}
	if os.IsNotExist(err) {
		return false
	}
	return true
}
