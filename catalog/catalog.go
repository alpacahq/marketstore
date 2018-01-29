package catalog

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/alpacahq/marketstore/utils/io"
)

type LevelFunc func(*Directory, interface{}) // Function for use in recursing into directories

type Directory struct {
	itemName, pathToItemName, category string
	/*
		itemName: instance of the category, e.g. itemName: "AAPL", category: "Symbol"
		pathToItemName: directory path to this item, e.g. pathToItemName: "/project/data", itemName: "AAPL"
	*/
	directMap, subDirs *sync.Map
	/*
		directMap[Key]: Key is the directory path, including the rootPath and excluding filename
		subDirs[Key]: Key is the name of the directory, aka "ItemName" which is an instance of the category
	*/
	catList  *sync.Map
	dataFile *sync.Map
	/*
		datafile[Key]: Key is the fully specified path to the datafile, including rootPath and filename
	*/
}

func NewDirectory(rootpath string) *Directory {
	d := &Directory{
		// Directmap will point to each directory node using a composite key
		directMap: &sync.Map{},
	}
	if err := d.load(rootpath); err != nil {
		panic(err)
	}
	return d
}

func (dRoot *Directory) AddTimeBucket(tbk *io.TimeBucketKey, f *io.TimeBucketInfo) (err error) {
	/*
		Adds a (possibly) new data item to a rootpath. Takes an existing catalog directory and
		adds the new data item to that data directory.
	*/
	exists := func(path string) bool {
		_, err := os.Stat(path)
		if err == nil {
			return true
		}
		if os.IsNotExist(err) {
			return false
		}
		return true
	}
	writeCatName := func(catName, dirName string) error {
		catNameFile := filepath.Join(dirName, "category_name")
		if !exists(catNameFile) {
			fp, err := os.OpenFile(catNameFile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0770)
			defer fp.Close()
			if err != nil {
				return fmt.Errorf(io.GetCallerFileContext(0) + err.Error())
			}
			if _, err = fp.WriteString(catName); err != nil {
				return fmt.Errorf(io.GetCallerFileContext(0) + err.Error())
			}
		} else {
			buffer, err := ioutil.ReadFile(catNameFile)
			if err != nil {
				return err
			}
			catNameFromFile := string(buffer)
			if catNameFromFile != catName {
				return fmt.Errorf("Category name does not match on-disk name")
			}
		}
		return nil
	}

	catkeySplit := tbk.GetCategories()
	datakeySplit := tbk.GetItems()

	dirname := dRoot.GetPath()
	for i, dataDirName := range datakeySplit {
		subdirname := filepath.Join(dirname, dataDirName)
		if !exists(subdirname) {
			if err = os.Mkdir(subdirname, 0770); err != nil {
				return fmt.Errorf(io.GetCallerFileContext(0) + err.Error())
			}
		}
		if err = writeCatName(catkeySplit[i], dirname); err != nil {
			return fmt.Errorf(io.GetCallerFileContext(0) + err.Error())
		}
		dirname = subdirname
	}
	// Write the last implied catName "Year"
	if err = writeCatName("Year", dirname); err != nil {
		return fmt.Errorf(io.GetCallerFileContext(0) + err.Error())
	}

	// Create a new data file using the TimeBucketInfo
	if err = newTimeBucketInfoFromTemplate(f); err != nil {
		return err
	}

	/*
		Check to see if this is an empty top level directory, if so - we need to set
		the top level category in the catalog entry
	*/
	if len(dRoot.category) == 0 {
		dRoot.category = catkeySplit[0]
	}

	/*
		Add this child directory tree to the parent top node's tree
	*/
	childNodeName := datakeySplit[0]
	childNodePath := filepath.Join(dRoot.GetPath(), childNodeName)
	childDirectory := NewDirectory(childNodePath)
	dRoot.addSubdir(childDirectory, childNodeName)

	return nil
}

func (dRoot *Directory) RemoveTimeBucket(tbk *io.TimeBucketKey) (err error) {
	/*
		Deletes the item at the last level specified in the dataItemKey
		Also removes empty directories at the higher levels after the delete
	*/
	if dRoot == nil {
		return fmt.Errorf(io.GetCallerFileContext(0) + ": Directory called from is nil")
	}

	datakeySplit := tbk.GetItems()

	tree := make([]*Directory, len(datakeySplit))
	current := dRoot
	for i := 0; i < len(datakeySplit); i++ {
		itemName := datakeySplit[i]
		// Descend from the current directory to find the first directory with the item name
		if tree[i] = current.GetSubDirWithItemName(itemName); tree[i] == nil {
			return fmt.Errorf("Unable to find level item: " + itemName + " in directory")
		}
		current = tree[i]
	}
	deleteMap := make([]bool, len(datakeySplit))
	end := len(datakeySplit) - 1
	for i := end; i >= 0; i-- {
		if i == end {
			removeDirFiles(tree[i])
			deleteMap[i] = true // This dir was deleted, we'll remove it from the parent's subdir list later
		} else {
			if deleteMap[i+1] {
				tree[i].removeSubDir(tree[i+1].itemName, dRoot.directMap)
			}
		}
		if !tree[i].DirHasSubDirs() {
			removeDirFiles(tree[i])
			deleteMap[i] = true // This dir was deleted, we'll remove it from the parent's subdir list later
		}
	}
	if deleteMap[0] {
		removeDirFiles(tree[0])
		if dRoot != nil {
			dRoot.removeSubDir(tree[0].itemName, dRoot.directMap)
		}
	}
	return nil
}

func (d *Directory) GetTimeBucketInfoSlice() (tbinfolist []*io.TimeBucketInfo) {
	// Returns a list of fileinfo for all datafiles in this directory or nil if there are none
	if d.dataFile == nil {
		return nil
	}
	tbinfolist = make([]*io.TimeBucketInfo, 0)
	d.dataFile.Range(func(k, v interface{}) bool {
		tbinfolist = append(tbinfolist, v.(*io.TimeBucketInfo))
		return true
	})
	return tbinfolist
}

func (d *Directory) GetTimeBucketInfos() *sync.Map {
	return d.dataFile
}

func (d *Directory) GatherTimeBucketInfo() []*io.TimeBucketInfo {
	// Locates a path in the directory and returns the TimeBucketInfo for that path or error if it isn't there
	// Must be thread-safe for READ access
	fileInfoFunc := func(d *Directory, i_list interface{}) {
		p_list := i_list.(*([]*io.TimeBucketInfo))
		if d.dataFile != nil {
			d.dataFile.Range(func(k, v interface{}) bool {
				*p_list = append(*p_list, v.(*io.TimeBucketInfo))
				return true
			})
		}
	}
	fileInfoList := make([]*io.TimeBucketInfo, 0)
	d.recurse(&fileInfoList, fileInfoFunc)
	return fileInfoList
}

func (d *Directory) GetLatestTimeBucketInfoFromKey(key *io.TimeBucketKey) (fi *io.TimeBucketInfo, err error) {
	path := key.GetPathToYearFiles(d.pathToItemName)
	fullFilePath := path + "/1970.bin" // Put a dummy file at the end of the path
	subDir, err := d.GetOwningSubDirectory(fullFilePath)
	if err != nil {
		return nil, err
	}
	return subDir.getLatestYearFile()
}

func (d *Directory) PathToTimeBucketInfo(path string) (*io.TimeBucketInfo, error) {
	/*
		Finds the TimeBucketInfo file in this directory based on a full file path argument
	*/
	// Must be thread-safe for READ access
	var tbinfo *io.TimeBucketInfo
	findTimeBucketInfo := func(d *Directory, _ interface{}) {
		if tbinfo != nil {
			// We have already found our fileinfo match
			return
		}
		if d.dataFile != nil {
			d.dataFile.Range(func(k, v interface{}) bool {
				dFile := v.(*io.TimeBucketInfo)
				if dFile.Path == path {
					tbinfo = dFile.GetDeepCopy()
					return false
				}
				return true
			})
		}
	}
	d.recurse(tbinfo, findTimeBucketInfo)
	if tbinfo == nil {
		return nil, NotFoundError("")
	}
	return tbinfo, nil
}

func (d *Directory) GetDataShapes(key *io.TimeBucketKey) (dsv []io.DataShape, err error) {
	fi, err := d.GetLatestTimeBucketInfoFromKey(key)
	if err != nil {
		return nil, err
	}
	return fi.GetDataShapes(), nil
}

func (subDir *Directory) AddFile(newYear int16) (finfo_p *io.TimeBucketInfo, err error) {
	// Must be thread-safe for WRITE access
	/*
	 Adds a new primary storage file for the provided year to this directory
	 Returns an error if this directory does not already contain a primary storage file

	 !!! NOTE !!! This should be called from the subdirectory that "owns" the file
	*/
	if subDir.dataFile == nil {
		return nil, SubdirectoryDoesNotContainFiles(subDir.pathToItemName)
	}

	var finfoTemplate *io.TimeBucketInfo
	subDir.dataFile.Range(func(k, v interface{}) bool {
		finfoTemplate = v.(*io.TimeBucketInfo)
		return false
	})

	newFileInfo := finfoTemplate.GetDeepCopy()
	newFileInfo.Year = newYear
	// Create a new filename for the new file
	newFileInfo.Path = path.Join(subDir.pathToItemName, strconv.Itoa(int(newYear))+".bin")
	if err = newTimeBucketInfoFromTemplate(newFileInfo); err != nil && err != FileAlreadyExists("Can not overwrite file") {
		return nil, err
	}
	// Locate the directory in the catalog
	subDir.dataFile.Store(newFileInfo.Path, newFileInfo)
	return newFileInfo, nil
}

func (d *Directory) DirHasDataFiles() bool {
	return d.dataFile != nil
}

func (d *Directory) GetName() string {
	return d.itemName
}

func (d *Directory) GetPath() string {
	return d.pathToItemName
}

func (d *Directory) GetOwningSubDirectory(fullFilePath string) (subDir *Directory, err error) {
	// Must be thread-safe for READ access
	dirPath := path.Dir(fullFilePath)
	if dir, ok := d.directMap.Load(dirPath); ok {
		return dir.(*Directory), nil
	}
	return nil, fmt.Errorf("Directory path %s not found in catalog", fullFilePath)
}

func (d *Directory) GetListOfSubDirs() (subDirList []*Directory) {
	// For a single directory, return a list of subdirectories it contains
	if d.subDirs == nil {
		return nil
	}
	subDirList = make([]*Directory, 0)
	d.subDirs.Range(func(k, v interface{}) bool {
		subDirList = append(subDirList, v.(*Directory))
		return true
	})
	return subDirList
}

func (d *Directory) SubDirectories() *sync.Map {
	return d.subDirs
}

func (d *Directory) GetSubDirWithItemName(itemName string) (subDir *Directory) {
	// For a single directory, return a subdirectory that matches the name "itemName"
	if d.subDirs == nil {
		return nil
	}

	if v, ok := d.subDirs.Load(itemName); ok && v != nil {
		return v.(*Directory)
	}
	return nil
}

func (d *Directory) DirHasSubDirs() bool {
	// Returns true if this directory has subdirectories
	if d.subDirs == nil {
		return false
	}
	count := 0
	d.subDirs.Range(func(k, v interface{}) bool {
		count++
		return false
	})
	return count > 0
}

func (d *Directory) GetCategory() string {
	return d.category
}

func (d *Directory) GatherCategoriesFromCache() *sync.Map {
	// Must be thread-safe for WRITE access
	// Provides a map of categories contained within and below this directory. Will create the list cache if nil.
	needToUpdate := (d.catList == nil)
	if needToUpdate {
		d.gatherCategoriesUpdateCache()
	}
	return d.catList
}
func (d *Directory) GatherCategoriesAndItems() *sync.Map {
	// Must be thread-safe for READ access
	// Provides a map of categories and items within and below this directory
	catListFunc := func(d *Directory, i_list interface{}) {
		list := i_list.(*sync.Map)
		if v, loaded := list.LoadOrStore(d.category, &sync.Map{}); loaded {
			if v == nil {
				list.Store(d.category, &sync.Map{})
			}
		}
		if d.subDirs != nil {
			d.subDirs.Range(func(k, v interface{}) bool {
				val, _ := list.Load(d.category)
				m := val.(*sync.Map)
				m.Store(v.(*Directory).itemName, 0)
				return true
			})
		}
		if d.dataFile != nil {
			d.dataFile.Range(func(k, v interface{}) bool {
				val, _ := list.Load(d.category)
				m := val.(*sync.Map)
				m.Store(strconv.Itoa(int(v.(*io.TimeBucketInfo).Year)), 0)
				return true
			})
		}
	}
	catList := &sync.Map{}
	d.recurse(catList, catListFunc)
	return catList
}

func (d *Directory) String() string {
	// Must be thread-safe for READ access
	printstring := "Node: " + d.itemName
	printstring += ", Category: " + d.category
	printstring += ", Subdirs: "
	d.subDirs.Range(func(k, v interface{}) bool {
		printstring += v.(*Directory).itemName + ":"
		return true
	})
	return printstring[:len(printstring)-1]
}

func (d *Directory) gatherDirectories() []string {
	// Must be thread-safe for READ access
	dirListFunc := func(d *Directory, i_list interface{}) {
		p_list := i_list.(*[]string)
		*p_list = append(*p_list, d.itemName)
	}
	dirList := make([]string, 0)
	d.recurse(&dirList, dirListFunc)
	return dirList
}
func (d *Directory) gatherFilePaths() []string {
	// Must be thread-safe for READ access
	filePathListFunc := func(d *Directory, i_list interface{}) {
		p_list := i_list.(*[]string)
		if d.dataFile != nil {
			d.dataFile.Range(func(k, v interface{}) bool {
				*p_list = append(*p_list, v.(*io.TimeBucketInfo).Path)
				return true
			})
		}
	}
	filePathList := make([]string, 0)
	d.recurse(&filePathList, filePathListFunc)
	return filePathList
}

func (d *Directory) gatherCategoriesUpdateCache() *sync.Map {
	// Must be thread-safe for WRITE access
	// Note that this should be called whenever catalog structure is modified to update the cache
	catListFunc := func(d *Directory, i_list interface{}) {
		i_list.(*sync.Map).Store(d.category, 0)
	}
	newCatList := &sync.Map{}
	d.recurse(newCatList, catListFunc)
	d.catList = newCatList
	return newCatList
}

func (d *Directory) getLatestYearFile() (latestFile *io.TimeBucketInfo, err error) {
	// Must be thread-safe for READ access
	if d.dataFile == nil {
		return nil, SubdirectoryDoesNotContainFiles("getLatestYearFile")
	}
	var year int16
	d.dataFile.Range(func(k, v interface{}) bool {
		fp := v.(*io.TimeBucketInfo)
		if year < fp.Year || year == 0 {
			year = fp.Year
			latestFile = fp
		}
		return true
	})
	return latestFile, nil
}
func (d *Directory) pathToKey(fullPath string) (key string) {
	dirPath := path.Dir(fullPath)
	key = strings.Replace(dirPath, d.pathToItemName, "", 1)
	key = strings.TrimLeft(key, "/")
	return key
}
func (d *Directory) addSubdir(subDir *Directory, subDirItemName string) {
	subDir.itemName = subDirItemName
	d.catList = nil // Reset the category list
	if d.subDirs == nil {
		d.subDirs = &sync.Map{}
	}
	d.subDirs.Store(subDirItemName, subDir)
	subDir.directMap.Range(func(k, v interface{}) bool {
		d.directMap.Store(k, v)
		return true
	})
	subDir.directMap = nil
}
func (d *Directory) removeSubDir(subDirItemName string, directMap *sync.Map) {
	if v, ok := d.subDirs.Load(subDirItemName); ok {
		subDir := v.(*Directory)
		directMap.Delete(subDir.pathToItemName)
	}
	d.subDirs.Delete(subDirItemName)
	count := 0
	d.subDirs.Range(func(k, v interface{}) bool {
		count++
		return false
	})
	if count == 0 {
		d.subDirs = nil
	}
}

func (d *Directory) recurse(elem interface{}, levelFunc LevelFunc) {
	// Must be thread-safe for READ access
	// Recurse will recurse through a directory, calling levelfunc. Elem is used to pass along a variable.
	levelFunc(d, elem)
	if d.subDirs != nil {
		d.subDirs.Range(func(k, v interface{}) bool {
			v.(*Directory).recurse(elem, levelFunc)
			return true
		})
	}
}

func (d *Directory) load(rootPath string) error {
	// Load is single thread compatible - no concurrent access is anticipated
	rootDmap := d.directMap
	var loader func(d *Directory, subPath, rootPath string) error
	loader = func(d *Directory, subPath, rootPath string) error {
		relPath, _ := filepath.Rel(rootPath, subPath)
		d.itemName = filepath.Base(relPath)
		d.pathToItemName = filepath.Clean(subPath)
		// Read the category name for the child directory items
		catFilePath := subPath + "/" + "category_name"
		catname, err := ioutil.ReadFile(catFilePath)
		if err != nil {
			// it's a fresh directory
			if subPath == rootPath {
				return nil
			}
			return fmt.Errorf(io.GetCallerFileContext(0) + err.Error())
		}
		d.category = string(catname)

		// Load up the child directories
		d.subDirs = &sync.Map{}
		dirlist, err := ioutil.ReadDir(subPath)
		for _, dirname := range dirlist {
			leafPath := path.Clean(subPath + "/" + dirname.Name())
			if dirname.IsDir() && dirname.Name() != "metadata.db" {
				itemName := dirname.Name()
				d.subDirs.Store(
					itemName,
					&Directory{
						itemName:       itemName,
						pathToItemName: subPath,
					},
				)
				d.dataFile = nil
				v, _ := d.subDirs.Load(itemName)
				if err := loader(v.(*Directory), leafPath, rootPath); err != nil {
					return fmt.Errorf(io.GetCallerFileContext(0) + err.Error())
				}
			} else if filepath.Ext(leafPath) == ".bin" {
				rootDmap.Store(d.pathToItemName, d)
				if d.dataFile == nil {
					d.dataFile = &sync.Map{}
				}
				// Mark this as a pending Fileinfo reference
				yearFileBase := filepath.Base(leafPath)
				yearString := yearFileBase[:len(yearFileBase)-4]
				yearInt, err := strconv.Atoi(yearString)
				if err != nil {
					return fmt.Errorf(io.GetCallerFileContext(0) + err.Error())
				}
				d.dataFile.Store(
					leafPath,
					&io.TimeBucketInfo{
						Path: leafPath,
						Year: int16(yearInt),
					},
				)
			}
		}
		return nil
	}
	return loader(d, rootPath, rootPath)
}

func removeDirFiles(td *Directory) {
	os.RemoveAll(td.pathToItemName)
}

func newTimeBucketInfoFromTemplate(newTimeBucketInfo *io.TimeBucketInfo) (err error) {
	if newTimeBucketInfo == nil {
		return fmt.Errorf("Null fileinfo")
	}

	// If file already exists in this directory, return an error
	if _, err := os.Stat(newTimeBucketInfo.Path); err == nil {
		return FileAlreadyExists("Can not overwrite file")
	}
	// Create the file
	fp, err := os.OpenFile(newTimeBucketInfo.Path, os.O_CREATE|os.O_RDWR, 0600)
	defer fp.Close()
	if err != nil {
		return UnableToCreateFile(err.Error())
	}
	if err = io.WriteHeader(fp, newTimeBucketInfo); err != nil {
		return UnableToWriteHeader(err.Error())
	}
	if err = fp.Truncate(io.FileSize(newTimeBucketInfo.GetIntervals(), int(newTimeBucketInfo.Year), int(newTimeBucketInfo.GetRecordLength()))); err != nil {
		return UnableToCreateFile(err.Error())
	}

	return nil
}
