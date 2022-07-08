package catalog_test

import (
	"errors"
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

const test1MinBucket = "TEST/1Min/OHLCV"

func setup(t *testing.T) (rootDir string, catalogDir *catalog.Directory) {
	t.Helper()

	rootDir = t.TempDir()
	test.MakeDummyCurrencyDir(rootDir, false, false)
	catalogDir, err := catalog.NewDirectory(rootDir)
	if err != nil {
		t.Fatal("failed to create a catalog dir.err=" + err.Error())
	}

	return rootDir, catalogDir
}

func TestGetCatList(t *testing.T) {
	_, catalogDir := setup(t)

	CatList, err := catalogDir.GatherCategoriesFromCache()
	assert.Nil(t, err)
	assert.Len(t, CatList, 4)
}

func TestGetCatItemMap(t *testing.T) {
	_, catalogDir := setup(t)

	catList, err := catalogDir.GatherCategoriesAndItems()
	assert.Nil(t, err)
	/*
		for key, list := range categorySet {
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
	_, catalogDir := setup(t)

	dirList, err := catalogDir.GatherDirectories()
	assert.Nil(t, err)
	assert.Len(t, dirList, 40)
}

func TestGatherFilePaths(t *testing.T) {
	_, catalogDir := setup(t)

	filePathList, err := catalogDir.GatherFilePaths()
	assert.Nil(t, err)
	//	for _, filePath := range filePathList {
	//		fmt.Printf("File Path: %s\n",filePath)
	//	}
	assert.Len(t, filePathList, 54)
}

func TestGatherFileInfo(t *testing.T) {
	_, catalogDir := setup(t)

	fileInfoList, err := catalogDir.GatherTimeBucketInfo()
	assert.Nil(t, err)
	// for _, fileInfo := range fileInfoList {
	//	fmt.Printf("File Path: %s Year: %d\n",fileInfo.Path,fileInfo.Year)
	// }
	assert.Len(t, fileInfoList, 54)
}

func TestPathToFileInfo(t *testing.T) {
	rootDir, catalogDir := setup(t)

	fileInfo, err := catalogDir.PathToTimeBucketInfo("nil")
	var targetErr catalog.NotFoundError
	if ok := errors.As(err, &targetErr); ok {
		assert.Equal(t, fileInfo, (*io.TimeBucketInfo)(nil))
	}

	mypath := rootDir + "/EURUSD/1Min/OHLC/2001.bin"
	fileInfo, err = catalogDir.PathToTimeBucketInfo(mypath)
	if err != nil {
		t.Log(err)
	}
	assert.Nil(t, err)
	assert.Equal(t, fileInfo.Path, mypath)
}

func TestAddFile(t *testing.T) {
	_, catalogDir := setup(t)

	// Get the owning subdirectory for a test file path
	filePathList, err := catalogDir.GatherFilePaths()
	assert.Nil(t, err)

	filePath := filePathList[0]
	// fmt.Println(filePath)
	subDir, err := catalogDir.GetOwningSubDirectory(filePath)
	if err != nil {
		t.Log(err)
	}
	assert.Nil(t, err)
	// fmt.Println(subDir.GetPath())
	// Get the latest year file in subdir
	_, err = subDir.GetLatestYearFile()
	assert.Nil(t, err)
	// latestFile, err := subDir.GetLatestYearFile()
	// fmt.Println(latestFile.Path)
	if _, err = subDir.AddFile(int16(2016)); err != nil {
		t.Log(err)
	}
	assert.Nil(t, err)
	_, err = subDir.GetLatestYearFile()
	assert.Nil(t, err)
	// fmt.Println("New Latest Year:", latestFile.Year, latestFile.Path)
}

func TestAddAndRemoveDataItem(t *testing.T) {
	rootDir, catalogDir := setup(t)

	catKey := io.DefaultTimeBucketSchema
	dataItemKey := test1MinBucket
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

	catList, err := catalogDir.GatherCategoriesAndItems()
	assert.Nil(t, err)
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
		t.Log(err)
	}
	assert.Nil(t, err)
	catList, err = catalogDir.GatherCategoriesAndItems()
	assert.Nil(t, err)
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
	rootDir := t.TempDir()
	catalogDir, err := catalog.NewDirectory(rootDir)
	var e catalog.ErrCategoryFileNotFound
	if err != nil && !errors.As(err, &e) {
		t.Fatal("failed to create a catalog dir.err=" + err.Error())
		return
	}

	catKey := io.DefaultTimeBucketSchema
	dataItemKey := test1MinBucket
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

	catList, err := catalogDir.GatherCategoriesAndItems()
	assert.Nil(t, err)
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
	// fmt.Println(categorySet)

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
	catList, err = catalogDir.GatherCategoriesAndItems()
	assert.Nil(t, err)
	_, ok = catList["Symbol"]["TEST2"]
	assert.True(t, ok)

	// This should fail
	err = catalogDir.AddTimeBucket(tbk, tbinfo)
	assert.NotNil(t, err)

	// Now let's remove the symbol and then re-add it - should work
	err = catalogDir.RemoveTimeBucket(tbk)
	if err != nil {
		t.Log(err)
	}
	assert.Nil(t, err)
	err = catalogDir.AddTimeBucket(tbk, tbinfo)
	assert.Nil(t, err)

	// Sometimes people may call AddTimeBucket with an empty directory, let's test that
	err = catalogDir.RemoveTimeBucket(tbk)
	if err != nil {
		t.Log(err)
	}
	assert.Nil(t, err)
	err = catalogDir.AddTimeBucket(tbk, tbinfo)
	assert.Nil(t, err)

	// Let's try two subsequent RemoveTimeBucket calls, the first should work, the second should err
	err = catalogDir.RemoveTimeBucket(tbk)
	if err != nil {
		t.Log(err)
	}
	assert.Nil(t, err)
	err = catalogDir.RemoveTimeBucket(tbk)
	if err != nil {
		t.Log(err)
	}
	assert.NotNil(t, err)
}

func TestCreateNewDirectory(t *testing.T) {
	rootDir, catalogDir := setup(t)

	catKey := io.DefaultTimeBucketSchema
	dataItemKey := test1MinBucket
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
	catList, err := catalogDir.GatherCategoriesAndItems()
	assert.Nil(t, err)
	_, ok := catList["Symbol"]["TEST"]
	assert.True(t, ok)

	// Construct the known new path to this subdirectory so that we can verify it is in the catalog
	newFilePath := path.Join(rootDir, "TEST", "1Min", "OHLCV", "2016.bin")
	_, err = catalogDir.GetOwningSubDirectory(newFilePath)
	assert.Nil(t, err)
}

func exists(fp string) bool {
	_, err := os.Stat(fp)
	if err == nil {
		return true
	}
	if os.IsNotExist(err) {
		return false
	}
	return true
}
