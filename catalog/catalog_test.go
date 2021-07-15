package catalog_test

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/alpacahq/marketstore/v4/catalog"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/test"
)

func setup(t *testing.T, testName string,
) (tearDown func(), rootDir string, catalogDir *catalog.Directory) {
	t.Helper()

	rootDir, _ = ioutil.TempDir("", fmt.Sprintf("catalog_test-%s", testName))
	test.MakeDummyCurrencyDir(rootDir, false, false)
	catalogDir, err := catalog.NewDirectory(rootDir)
	if err != nil {
		t.Fatal("failed to create a catalog dir.err=" + err.Error())
	}

	return func() { test.CleanupDummyDataDir(rootDir) }, rootDir, catalogDir
}

func TestGetDirectMap(t *testing.T) {
	tearDown, _, catalogDir := setup(t, "TestGetDirectMap")
	defer tearDown()

	assert.Len(t, catalogDir.DirectMap, 18)
}

func TestGetCatList(t *testing.T) {
	tearDown, _, catalogDir := setup(t, "TestGetCatList")
	defer tearDown()

	CatList := catalogDir.GatherCategoriesFromCache()
	assert.Len(t, CatList, 4)
}
func TestGetCatItemMap(t *testing.T) {
	tearDown, _, catalogDir := setup(t, "TestGetCatItemMap")
	defer tearDown()

	catList := catalogDir.GatherCategoriesAndItems()
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
	assert.Len(t, catList, 4)
}
func TestGetDirList(t *testing.T) {
	tearDown, _, catalogDir := setup(t, "TestGetDirList")
	defer tearDown()

	dirList := catalogDir.GatherDirectories()
	assert.Len(t, dirList, 40)
}
func TestGatherFilePaths(t *testing.T) {
	tearDown, _, catalogDir := setup(t, "TestGatherFilePaths")
	defer tearDown()

	filePathList := catalogDir.GatherFilePaths()
	//	for _, filePath := range filePathList {
	//		fmt.Printf("File Path: %s\n",filePath)
	//	}
	assert.Len(t, filePathList, 54)
}
func TestGatherFileInfo(t *testing.T) {
	tearDown, _, catalogDir := setup(t, "TestGatherFileInfo")
	defer tearDown()

	fileInfoList := catalogDir.GatherTimeBucketInfo()
	//for _, fileInfo := range fileInfoList {
	//	fmt.Printf("File Path: %s Year: %d\n",fileInfo.Path,fileInfo.Year)
	//}
	assert.Len(t, fileInfoList, 54)
}
func TestPathToFileInfo(t *testing.T) {
	tearDown, rootDir, catalogDir := setup(t, "TestPathToFileInfo")
	defer tearDown()

	fileInfo, err := catalogDir.PathToTimeBucketInfo("nil")
	if err != nil {
		if _, ok := err.(catalog.NotFoundError); ok {
			assert.Equal(t, fileInfo, (*io.TimeBucketInfo)(nil))
		} else {
			t.Fail()
		}
	}
	mypath := rootDir + "/EURUSD/1Min/OHLC/2001.bin"
	fileInfo, err = catalogDir.PathToTimeBucketInfo(mypath)
	if err != nil {
		fmt.Println(err)
	}
	assert.Nil(t, err)
	assert.Equal(t, fileInfo.Path, mypath)
}
func TestAddFile(t *testing.T) {
	tearDown, _, catalogDir := setup(t, "TestAddFile")
	defer tearDown()

	// Get the owning subdirectory for a test file path
	filePathList := catalogDir.GatherFilePaths()
	filePath := filePathList[0]
	// fmt.Println(filePath)
	subDir, err := catalogDir.GetOwningSubDirectory(filePath)
	if err != nil {
		fmt.Println(err)
	}
	assert.Nil(t, err)
	// fmt.Println(subDir.GetPath())
	// Get the latest year file in subdir
	_, err = subDir.GetLatestYearFile()
	assert.Nil(t, err)
	// latestFile, err := subDir.GetLatestYearFile()
	// fmt.Println(latestFile.Path)
	if _, err = subDir.AddFile(int16(2016)); err != nil {
		fmt.Println(err)
	}
	assert.Nil(t, err)
	_, err = subDir.GetLatestYearFile()
	assert.Nil(t, err)
	// fmt.Println("New Latest Year:", latestFile.Year, latestFile.Path)
}

func TestAddAndRemoveDataItem(t *testing.T) {
	tearDown, rootDir, catalogDir := setup(t, "TestPathToFileInfo")
	defer tearDown()

	catKey := "Symbol/Timeframe/AttributeGroup"
	dataItemKey := "TEST/1Min/OHLCV"
	dataItemPath := filepath.Join(rootDir, dataItemKey)
	dsv := io.NewDataShapeVector(
		[]string{"Open", "High", "Low", "Close", "Volume"},
		[]io.EnumElementType{io.FLOAT32, io.FLOAT32, io.FLOAT32, io.FLOAT32, io.INT32},
	)
	tbinfo := io.NewTimeBucketInfo(*utils.TimeframeFromString("1Min"), dataItemPath, "Test item", 2016,
		dsv, io.FIXED)

	tbk := io.NewTimeBucketKey(dataItemKey, catKey)
	err := catalogDir.AddTimeBucket(tbk, tbinfo)
	assert.Nil(t, err)

	catList := catalogDir.GatherCategoriesAndItems()
	_, ok := catList["Symbol"]["TEST"]
	assert.True(t, ok)
	// Construct the known new path to this subdirectory so that we can verify it is in the catalog

	oldFilePath := path.Join(rootDir, "EURUSD", "1Min", "OHLC", "2000.bin")
	_, err = catalogDir.GetOwningSubDirectory(oldFilePath)
	assert.Nil(t, err)

	newFilePath := path.Join(rootDir, "TEST", "1Min", "OHLCV", "2016.bin")
	_, err = catalogDir.GetOwningSubDirectory(newFilePath)
	assert.Nil(t, err)

	err = catalogDir.RemoveTimeBucket(tbk)
	if err != nil {
		fmt.Println(err)
	}
	assert.Nil(t, err)
	catList = catalogDir.GatherCategoriesAndItems()
	_, ok = catList["Symbol"]["TEST"]
	assert.False(t, ok)
	npath := newFilePath
	assert.False(t, exists(npath))
	npath = path.Join(rootDir, "TEST", "1Min", "OHLCV")
	assert.False(t, exists(npath))
	npath = path.Join(rootDir, "TEST", "1Min")
	assert.False(t, exists(npath))
	npath = path.Join(rootDir, "TEST")
	assert.False(t, exists(npath))
	npath = path.Join(rootDir, "EURUSD")
	assert.True(t, exists(npath))
}

func TestAddAndRemoveDataItemFromEmptyDirectory(t *testing.T) {
	rootDir, _ := ioutil.TempDir("", fmt.Sprintf("catalog_test-TestAddAndRemoveDataItemFromEmptyDirectory"))
	catalogDir, err := catalog.NewDirectory(rootDir)
	var e *catalog.ErrCategoryFileNotFound
	if err != nil && !errors.As(err, &e) {
		t.Fatal("failed to create a catalog dir.err=" + err.Error())
		return
	}

	defer test.CleanupDummyDataDir(rootDir)

	catKey := "Symbol/Timeframe/AttributeGroup"
	dataItemKey := "TEST/1Min/OHLCV"
	tbk := io.NewTimeBucketKey(dataItemKey, catKey)

	dataItemPath := filepath.Join(rootDir, dataItemKey)
	dsv := io.NewDataShapeVector(
		[]string{"Open", "High", "Low", "Close", "Volume"},
		[]io.EnumElementType{io.FLOAT32, io.FLOAT32, io.FLOAT32, io.FLOAT32, io.INT32},
	)
	tbinfo := io.NewTimeBucketInfo(*utils.TimeframeFromString("1Min"), dataItemPath, "Test item", 2016,
		dsv, io.FIXED)

	err = catalogDir.AddTimeBucket(tbk, tbinfo)
	assert.Nil(t, err)

	catList := catalogDir.GatherCategoriesAndItems()
	_, ok := catList["Symbol"]["TEST"]
	assert.True(t, ok)
	newFilePath := path.Join(rootDir, "TEST", "1Min", "OHLCV", "2016.bin")
	npath := newFilePath
	assert.True(t, exists(npath))
	npath = path.Join(rootDir, "TEST", "1Min", "OHLCV")
	assert.True(t, exists(npath))
	npath = path.Join(rootDir, "TEST", "1Min")
	assert.True(t, exists(npath))
	npath = path.Join(rootDir, "TEST")
	assert.True(t, exists(npath))
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
	err = catalogDir.AddTimeBucket(tbk, tbinfo)
	assert.Nil(t, err)
	catList = catalogDir.GatherCategoriesAndItems()
	_, ok = catList["Symbol"]["TEST2"]
	assert.True(t, ok)

	// This should fail
	err = catalogDir.AddTimeBucket(tbk, tbinfo)
	assert.NotNil(t, err)

	// Now let's remove the symbol and then re-add it - should work
	err = catalogDir.RemoveTimeBucket(tbk)
	if err != nil {
		fmt.Println(err)
	}
	assert.Nil(t, err)
	err = catalogDir.AddTimeBucket(tbk, tbinfo)
	assert.Nil(t, err)

	// Sometimes people may call AddTimeBucket with an empty directory, let's test that
	err = catalogDir.RemoveTimeBucket(tbk)
	if err != nil {
		fmt.Println(err)
	}
	assert.Nil(t, err)
	err = catalogDir.AddTimeBucket(tbk, tbinfo)
	assert.Nil(t, err)

	// Let's try two subsequent RemoveTimeBucket calls, the first should work, the second should err
	err = catalogDir.RemoveTimeBucket(tbk)
	if err != nil {
		fmt.Println(err)
	}
	assert.Nil(t, err)
	err = catalogDir.RemoveTimeBucket(tbk)
	if err != nil {
		fmt.Println(err)
	}
	assert.NotNil(t, err)
}

func TestCreateNewDirectory(t *testing.T) {
	tearDown, rootDir, catalogDir := setup(t, "TestPathToFileInfo")
	defer tearDown()

	catKey := "Symbol/Timeframe/AttributeGroup"
	dataItemKey := "TEST/1Min/OHLCV"
	dataItemPath := filepath.Join(rootDir, dataItemKey)
	dsv := io.NewDataShapeVector(
		[]string{"Open", "High", "Low", "Close", "Volume"},
		[]io.EnumElementType{io.FLOAT32, io.FLOAT32, io.FLOAT32, io.FLOAT32, io.INT32},
	)
	tbinfo := io.NewTimeBucketInfo(*utils.TimeframeFromString("1Min"), dataItemPath, "Test item", 2016,
		dsv, io.FIXED)

	tbk := io.NewTimeBucketKey(dataItemKey, catKey)
	err := catalogDir.AddTimeBucket(tbk, tbinfo)
	assert.Nil(t, err)
	catList := catalogDir.GatherCategoriesAndItems()
	_, ok := catList["Symbol"]["TEST"]
	assert.True(t, ok)

	// Construct the known new path to this subdirectory so that we can verify it is in the catalog
	newFilePath := path.Join(rootDir, "TEST", "1Min", "OHLCV", "2016.bin")
	_, err = catalogDir.GetOwningSubDirectory(newFilePath)
	assert.Nil(t, err)
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
