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

	"github.com/alpacahq/marketstore/v4/utils/io"
)

type Directory struct {
	sync.RWMutex

	// itemName is the instance of the category. e.g. itemName: "AAPL", category: "Symbol"
	itemName string
	// pathToItemName is the directory path to this item. e.g. pathToItemName: "/project/data", itemName: "AAPL"
	pathToItemName string
	category       string

	// directMap[Key]: Key is the directory path, including the rootPath and excluding filename
	directMap *sync.Map
	// subDirs[Key]: Key is the name of the directory, aka "ItemName" which is an instance of the category
	subDirs map[string]*Directory

	catList map[string]int8
	// datafile[Key]: Key is the fully specified path to the datafile, including rootPath and filename
	datafile map[string]*io.TimeBucketInfo
}

// NewDirectory scans files under the rootPath and return a new Directory struct.
// - returns ErrCategoryFileNotFound when "category_name" file is not found under each subdirectory,
// - returns an error in other unexpected cases.
func NewDirectory(rootPath string) (*Directory, error) {
	d := &Directory{
		// Directmap will point to each directory node using a composite key
		directMap: &sync.Map{},
	}

	// Load is single thread compatible - no concurrent access is anticipated
	err := load(d.directMap, d, rootPath, rootPath)

	return d, err
}

func load(rootDmap *sync.Map, d *Directory, subPath, rootPath string) error {
	relPath, _ := filepath.Rel(rootPath, subPath)
	d.itemName = filepath.Base(relPath)
	d.pathToItemName = filepath.Clean(subPath)
	// Read the category name for the child directory items
	catFilePath := subPath + "/" + "category_name"
	catname, err := ioutil.ReadFile(catFilePath)
	if err != nil {
		return &ErrCategoryFileNotFound{filePath: catFilePath, msg: io.GetCallerFileContext(0) + err.Error()}
	}
	d.category = string(catname)

	// Load up the child directories
	d.subDirs = make(map[string]*Directory)
	dirlist, err := ioutil.ReadDir(subPath)
	if err != nil {
		return fmt.Errorf("read dir %s: %w", subPath, err)
	}
	for _, dirname := range dirlist {
		leafPath := path.Clean(subPath + "/" + dirname.Name())
		if dirname.IsDir() && dirname.Name() != "metadata.db" {
			itemName := dirname.Name()
			d.subDirs[itemName] = &Directory{
				itemName:       itemName,
				pathToItemName: subPath,
			}

			d.datafile = nil
			if err := load(rootDmap, d.subDirs[itemName], leafPath, rootPath); err != nil {
				return fmt.Errorf(io.GetCallerFileContext(0) + ", " + err.Error())
			}
		} else if filepath.Ext(leafPath) == ".bin" {
			rootDmap.Store(d.pathToItemName, d)
			if d.datafile == nil {
				d.datafile = make(map[string]*io.TimeBucketInfo)
			}
			// Mark this as a pending Fileinfo reference
			d.datafile[leafPath] = new(io.TimeBucketInfo)
			d.datafile[leafPath].IsRead = false
			d.datafile[leafPath].Path = leafPath
			yearFileBase := filepath.Base(leafPath)
			yearString := yearFileBase[:len(yearFileBase)-4]
			yearInt, err := strconv.Atoi(yearString)
			if err != nil {
				return fmt.Errorf(io.GetCallerFileContext(0) + err.Error())
			}
			d.datafile[leafPath].Year = int16(yearInt)
			/*
				if d.datafile[leafPath], err = ReadHeader(leafPath); err != nil {
					return err
				}
			*/
		}
	}
	return nil
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	if err == nil {
		return true
	}
	if os.IsNotExist(err) {
		return false
	}
	return true
}

func writeCategoryNameFile(catName, dirName string) error {
	catNameFile := filepath.Join(dirName, "category_name")

	if fileExists(catNameFile) {
		buffer, err := ioutil.ReadFile(catNameFile)
		if err != nil {
			return err
		}
		catNameFromFile := string(buffer)
		if catNameFromFile != catName {
			return fmt.Errorf("Category name does not match on-disk name")
		}
		return nil
	}

	fp, err := os.OpenFile(catNameFile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0770)
	if err != nil {
		return fmt.Errorf(io.GetCallerFileContext(0) + err.Error())
	}
	defer fp.Close()
	if _, err = fp.WriteString(catName); err != nil {
		return fmt.Errorf(io.GetCallerFileContext(0) + err.Error())
	}
	return nil
}

func (dRoot *Directory) AddTimeBucket(tbk *io.TimeBucketKey, f *io.TimeBucketInfo) (err error) {
	/*
		Adds a (possibly) new data item to a rootpath. Takes an existing catalog directory and
		adds the new data item to that data directory.
	*/
	dRoot.Lock()
	defer dRoot.Unlock()

	catkeySplit := tbk.GetCategories()
	datakeySplit := tbk.GetItems()

	dirname := dRoot.GetPath()
	for i, dataDirName := range datakeySplit {
		subdirname := filepath.Join(dirname, dataDirName)
		if !fileExists(subdirname) {
			if err = os.Mkdir(subdirname, 0770); err != nil {
				return fmt.Errorf(io.GetCallerFileContext(0) + err.Error())
			}
		}
		if err = writeCategoryNameFile(catkeySplit[i], dirname); err != nil {
			return fmt.Errorf(io.GetCallerFileContext(0) + err.Error())
		}
		dirname = subdirname
	}
	// Write the last implied catName "Year"
	if err = writeCategoryNameFile("Year", dirname); err != nil {
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
	childDirectory, err := NewDirectory(childNodePath)
	if err != nil {
		return err
	}
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
	d.RLock()
	defer d.RUnlock()
	if d.datafile == nil {
		return nil
	}
	tbinfolist = make([]*io.TimeBucketInfo, 0)
	for _, finfo_p := range d.datafile {
		tbinfolist = append(tbinfolist, finfo_p)
	}
	return tbinfolist
}

func (d *Directory) GatherTimeBucketInfo() []*io.TimeBucketInfo {
	// Locates a path in the directory and returns the TimeBucketInfo for that path or error if it isn't there
	// Must be thread-safe for READ access
	fileInfoFunc := func(d *Directory, i_list interface{}) {
		p_list := i_list.(*([]*io.TimeBucketInfo))
		if d.datafile != nil {
			for _, dfile := range d.datafile {
				*p_list = append(*p_list, dfile)
			}
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
	return subDir.GetLatestYearFile()
}

func (d *Directory) GetLatestTimeBucketInfoFromFullFilePath(fullFilePath string) (fi *io.TimeBucketInfo, err error) {
	subDir, err := d.GetOwningSubDirectory(fullFilePath)
	if err != nil {
		return nil, err
	}
	return subDir.GetLatestYearFile()
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
		if d.datafile != nil {
			for _, dfile := range d.datafile {
				if dfile.Path == path {
					tbinfo = dfile.GetDeepCopy()
					return
				}
			}
		}
	}
	d.recurse(tbinfo, findTimeBucketInfo)
	if tbinfo == nil {
		return nil, NotFoundError("")
	} else {
		return tbinfo, nil
	}
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
	 Returns:
	  - error if the directory does not contain a single year file (time bucket not initialized)
	  - *TimeBucketInfo whether the year file already existed or if a new one is made
	 Creates:
	  - a new year file if one is not there already

	 !!! NOTE !!! This should be called from the subdirectory that "owns" the file
	*/
	subDir.RLock()
	if subDir.datafile == nil {
		subDir.RUnlock()
		return nil, SubdirectoryDoesNotContainFiles(subDir.pathToItemName)
	}

	var finfoTemplate *io.TimeBucketInfo
	for _, fi := range subDir.datafile {
		finfoTemplate = fi
		break
	}
	subDir.RUnlock()

	newFileInfo := finfoTemplate.GetDeepCopy()
	newFileInfo.Year = newYear
	// Create a new filename for the new file
	subDir.RLock()
	newFileInfo.Path = path.Join(subDir.pathToItemName, strconv.Itoa(int(newYear))+".bin")
	subDir.RUnlock()
	if err = newTimeBucketInfoFromTemplate(newFileInfo); err != nil {
		if _, ok := err.(FileAlreadyExists); ok {
			return newFileInfo, nil
		}
		return nil, err
	}
	// Locate the directory in the catalog
	subDir.Lock()
	subDir.datafile[newFileInfo.Path] = newFileInfo
	subDir.Unlock()

	return newFileInfo, nil
}

func (d *Directory) DirHasDataFiles() bool {
	d.RLock()
	defer d.RUnlock()
	return d.datafile != nil
}

func (d *Directory) GetName() string {
	d.RLock()
	defer d.RUnlock()
	return d.itemName
}

func (d *Directory) GetPath() string {
	return d.pathToItemName
}

func (d *Directory) GetSubDirectoryAndAddFile(fullFilePath string, year int16) (*io.TimeBucketInfo, error) {
	d.Lock()
	defer d.Unlock()
	dirPath := path.Dir(fullFilePath)
	if d.directMap != nil {
		if dir, ok := d.directMap.Load(dirPath); ok {
			return dir.(*Directory).AddFile(year)
		}
	}
	return nil, fmt.Errorf("Directory path %s not found in catalog", fullFilePath)
}

func (d *Directory) GetOwningSubDirectory(fullFilePath string) (subDir *Directory, err error) {
	// Must be thread-safe for READ access
	dirPath := path.Dir(fullFilePath)
	d.RLock()
	defer d.RUnlock()
	if d.directMap != nil {
		if dir, ok := d.directMap.Load(dirPath); ok {
			return dir.(*Directory), nil
		}
	}
	return nil, fmt.Errorf("Directory path %s not found in catalog", fullFilePath)
}

func (d *Directory) GetListOfSubDirs() (subDirList []*Directory) {
	// For a single directory, return a list of subdirectories it contains
	d.RLock()
	defer d.RUnlock()
	if d.subDirs == nil {
		return nil
	}
	subDirList = make([]*Directory, 0)
	for _, subdir := range d.subDirs {
		subDirList = append(subDirList, subdir)
	}
	return subDirList
}

func (d *Directory) GetSubDirWithItemName(itemName string) (subDir *Directory) {
	// For a single directory, return a subdirectory that matches the name "itemName"
	d.RLock()
	defer d.RUnlock()
	if d.subDirs == nil {
		return nil
	}
	if _, ok := d.subDirs[itemName]; ok {
		return d.subDirs[itemName]
	}
	return nil
}

func (d *Directory) DirHasSubDirs() bool {
	// Returns true if this directory has subdirectories
	d.RLock()
	defer d.RUnlock()
	if d.subDirs == nil {
		return false
	}
	if len(d.subDirs) == 0 {
		return false
	}
	return true
}

func (d *Directory) GetCategory() string {
	d.RLock()
	defer d.RUnlock()
	return d.category
}

func (d *Directory) GatherCategoriesFromCache() (catList map[string]int8) {
	// Must be thread-safe for WRITE access
	// Provides a map of categories contained within and below this directory. Will create the list cache if nil.
	d.RLock()
	catList = d.catList
	d.RUnlock()
	if catList == nil {
		return d.gatherCategoriesUpdateCache()
	}
	return catList
}

func (d *Directory) GatherCategoriesAndItems() map[string]map[string]int {
	// Must be thread-safe for READ access
	// Provides a map of categories and items within and below this directory
	catListFunc := func(d *Directory, i_list interface{}) {
		list := i_list.(map[string]map[string]int)
		if list[d.category] == nil {
			list[d.category] = make(map[string]int, 0)
		}
		if d.subDirs != nil {
			for _, subdir := range d.subDirs {
				list[d.category][subdir.itemName] = 0
			}
		}
		if d.datafile != nil {
			for _, file := range d.datafile {
				list[d.category][strconv.Itoa(int(file.Year))] = 0
			}
		}
	}
	catList := make(map[string]map[string]int, 0)
	d.recurse(catList, catListFunc)
	return catList
}

// ListTimeBucketKeyNames returns the list of TimeBucket keys
// in "{symbol}/{timeframe}/{atrributeGroup}" format.
func ListTimeBucketKeyNames(d *Directory) []string {
	tbkMap := map[string]struct{}{}

	d.RLock()
	defer d.RUnlock()
	// look up symbol->timeframe->attributeGroup directory recursively
	// (e.g. "AAPL" -> "1Min" -> "Tick",  and store "AAPL/1Min/Tick" )
	for symbol, symbolDir := range d.subDirs {
		if symbolDir == nil {
			continue
		}
		for timeframe, timeframeDir := range symbolDir.subDirs {
			if timeframeDir == nil {
				continue
			}
			for attributeGroup := range timeframeDir.subDirs {
				tbkMap[fmt.Sprintf("%s/%s/%s", symbol, timeframe, attributeGroup)] = struct{}{}
			}
		}
	}

	// convert Map keys to a string slice
	i := 0
	result := make([]string, len(tbkMap))
	for tbk := range tbkMap {
		result[i] = tbk
		i++
	}

	return result
}

func (d *Directory) String() string {
	// Must be thread-safe for READ access
	printstring := "Node: " + d.itemName
	printstring += ", Category: " + d.category
	printstring += ", Subdirs: "
	d.RLock()
	for _, subdir := range d.subDirs {
		subdir.RLock()
		printstring += subdir.itemName + ":"
		subdir.RUnlock()
	}
	d.RUnlock()
	return printstring[:len(printstring)-1]
}

func (d *Directory) GatherDirectories() []string {
	// Must be thread-safe for READ access
	dirListFunc := func(d *Directory, i_list interface{}) {
		p_list := i_list.(*[]string)
		*p_list = append(*p_list, d.itemName)
	}
	dirList := make([]string, 0)
	d.recurse(&dirList, dirListFunc)
	return dirList
}
func (d *Directory) GatherFilePaths() []string {
	// Must be thread-safe for READ access
	filePathListFunc := func(d *Directory, i_list interface{}) {
		p_list := i_list.(*[]string)
		if d.datafile != nil {
			for _, dfile := range d.datafile {
				*p_list = append(*p_list, dfile.Path)
			}
		}
	}
	filePathList := make([]string, 0)
	d.recurse(&filePathList, filePathListFunc)
	return filePathList
}

func (d *Directory) gatherCategoriesUpdateCache() map[string]int8 {
	// Must be thread-safe for WRITE access
	// Note that this should be called whenever catalog structure is modified to update the cache
	catListFunc := func(d *Directory, i_list interface{}) {
		list := i_list.(map[string]int8)
		list[d.category] = 0
	}
	newCatList := make(map[string]int8, 0)
	d.recurse(newCatList, catListFunc)
	d.Lock()
	d.catList = newCatList
	d.Unlock()
	return newCatList
}

func (d *Directory) getOwningSubDirectoryByRecursion(filePath string) (subDir *Directory, err error) {
	// Locates the directory in the catalog that matches the path - note that this is O(N)
	// Must be thread-safe for READ access
	dirPath := path.Dir(filePath)
	findDirectory := func(d *Directory, _ interface{}) {
		if subDir != nil {
			// We have already found our directory match
			return
		}
		if d.pathToItemName == dirPath {
			subDir = d
			return
		}
	}

	d.recurse(subDir, findDirectory)
	if subDir == nil {
		return nil, NotFoundError("")
	} else {
		return subDir, nil
	}
}
func (d *Directory) GetLatestYearFile() (latestFile *io.TimeBucketInfo, err error) {
	// Must be thread-safe for READ access
	d.RLock()
	defer d.RUnlock()
	if d.datafile == nil {
		return nil, SubdirectoryDoesNotContainFiles("getLatestYearFile")
	}
	var year int16
	for _, fp := range d.datafile {
		if year < fp.Year || year == 0 {
			year = fp.Year
			latestFile = fp
		}
	}
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
		d.subDirs = make(map[string]*Directory)
	}
	d.subDirs[subDirItemName] = subDir
	if subDir.directMap != nil {
		subDir.directMap.Range(func(key interface{}, val interface{}) bool {
			d.directMap.Store(key, val)
			return true
		})
	}
	subDir.directMap = nil
}
func (d *Directory) removeSubDir(subDirItemName string, directMap *sync.Map) {
	d.Lock()
	defer d.Unlock()
	if _, ok := d.subDirs[subDirItemName]; ok {
		// Note that this is a NoOp for all but the leaf node of the tree, but it's a harmless NoOp
		subdir := d.subDirs[subDirItemName]
		directMap.Delete(subdir.pathToItemName)
	}
	delete(d.subDirs, subDirItemName)
	if len(d.subDirs) == 0 {
		d.subDirs = nil
	}
}

type levelFunc func(*Directory, interface{}) // Function for use in recursing into directories

func (d *Directory) recurse(elem interface{}, levelFunc levelFunc) {
	// Must be thread-safe for READ access
	// Recurse will recurse through a directory, calling levelfunc. Elem is used to pass along a variable.
	d.RLock()
	defer d.RUnlock()
	levelFunc(d, elem)
	if d.subDirs != nil {
		for _, pd := range d.subDirs {
			pd.recurse(elem, levelFunc)
		}
	}
}

func removeDirFiles(td *Directory) {
	td.Lock()
	defer td.Unlock()

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
	if err != nil {
		return fmt.Errorf("open new time bucket info file %s: %w", newTimeBucketInfo.Path, err)
	}
	defer fp.Close()
	if err != nil {
		return UnableToCreateFile(err.Error())
	}
	if err = io.WriteHeader(fp, newTimeBucketInfo); err != nil {
		return UnableToWriteHeader(err.Error())
	}

	fileSize := io.FileSize(
		newTimeBucketInfo.GetTimeframe(),
		int(newTimeBucketInfo.Year),
		int(newTimeBucketInfo.GetRecordLength()),
	)
	if err = fp.Truncate(fileSize); err != nil {
		return UnableToCreateFile(err.Error())
	}

	return nil
}
