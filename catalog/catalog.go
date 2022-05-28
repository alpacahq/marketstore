package catalog

import (
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"sync"

	"go.uber.org/zap"

	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

type Directory struct {
	sync.RWMutex

	// itemName is the instance of the category. e.g. itemName: "AAPL", category: "Symbol"
	itemName string
	// pathToItemName is the directory path to this item. e.g. pathToItemName: "/project/data", itemName: "AAPL"
	pathToItemName string
	// category is a string that represents what is listed up under this directory.
	// stored in "category_name" file under each directory.
	// e.g. "Symbol", "Timeframe", "AttributeGroup", "Year".
	category string

	// directMap[Key]: Key is the directory path, including the rootPath and excluding filename
	directMap *sync.Map
	// subDirs[Key]: Key is the name of the directory, aka "ItemName" which is an instance of the category
	subDirs map[string]*Directory

	categorySet map[string]struct{}
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
	catname, err := os.ReadFile(catFilePath)
	if err != nil {
		return ErrCategoryFileNotFound{filePath: catFilePath, msg: io.GetCallerFileContext(0) + err.Error()}
	}
	d.category = string(catname)

	// Load up the child directories
	d.subDirs = make(map[string]*Directory)
	dirlist, err := os.ReadDir(subPath)
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
				var e ErrCategoryFileNotFound
				if errors.As(err, &e) {
					log.Warn(fmt.Sprintf("category_name file not found under the directory."+
						"%s will be ignored:%v", leafPath, err.Error()))
				} else {
					return errors.New(io.GetCallerFileContext(0) + ", " + err.Error())
				}
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
				return errors.New(io.GetCallerFileContext(0) + err.Error())
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

func fileExists(path2 string) bool {
	_, err := os.Stat(path2)
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
		buffer, err := os.ReadFile(catNameFile)
		if err != nil {
			return err
		}
		catNameFromFile := string(buffer)
		if catNameFromFile != catName {
			return fmt.Errorf("category name does not match on-disk name")
		}
		return nil
	}

	fp, err := os.OpenFile(catNameFile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o770)
	if err != nil {
		return errors.New(io.GetCallerFileContext(0) + err.Error())
	}
	defer func() {
		if err2 := fp.Close(); err2 != nil {
			log.Error("failed to close category name file", zap.Error(err2),
				zap.String("filepath", catNameFile),
			)
		}
	}()
	if _, err = fp.WriteString(catName); err != nil {
		return errors.New(io.GetCallerFileContext(0) + err.Error())
	}
	return nil
}

// AddTimeBucket adds a (possibly) new data item to a rootpath. Takes an existing catalog directory and
// adds the new data item to that data directory. This is used only for a root category directory.
func (d *Directory) AddTimeBucket(tbk *io.TimeBucketKey, f *io.TimeBucketInfo) (err error) {
	d.Lock()
	defer d.Unlock()

	catkeySplit := tbk.GetCategories()
	datakeySplit := tbk.GetItems()

	dirname := d.GetPath()
	for i, dataDirName := range datakeySplit {
		subdirname := filepath.Join(dirname, dataDirName)
		if !fileExists(subdirname) {
			if err = os.Mkdir(subdirname, 0o770); err != nil {
				return errors.New(io.GetCallerFileContext(0) + err.Error())
			}
		}
		if err = writeCategoryNameFile(catkeySplit[i], dirname); err != nil {
			return errors.New(io.GetCallerFileContext(0) + err.Error())
		}
		dirname = subdirname
	}
	// Write the last implied catName "Year"
	if err = writeCategoryNameFile("Year", dirname); err != nil {
		return errors.New(io.GetCallerFileContext(0) + err.Error())
	}

	// Create a new data file using the TimeBucketInfo
	if err2 := newTimeBucketInfoFromTemplate(f); err2 != nil {
		return err2
	}

	/*
		Check to see if this is an empty top level directory, if so - we need to set
		the top level category in the catalog entry
	*/
	if d.category == "" {
		d.category = catkeySplit[0]
	}

	/*
		Add this child directory tree to the parent top node's tree
	*/
	childNodeName := datakeySplit[0]
	childNodePath := filepath.Join(d.GetPath(), childNodeName)
	childDirectory, err := NewDirectory(childNodePath)
	if err != nil {
		return err
	}
	d.addSubdir(childDirectory, childNodeName)
	return nil
}

// RemoveTimeBucket deletes the item at the last level specified in the dataItemKey
// Also removes empty directories at the higher levels after the delete. This is used for a root catalog directory.
func (d *Directory) RemoveTimeBucket(tbk *io.TimeBucketKey) (err error) {
	if d == nil {
		return errors.New(io.GetCallerFileContext(0) + ": Directory called from is nil")
	}

	datakeySplit := tbk.GetItems()

	tree := make([]*Directory, len(datakeySplit))
	current := d
	for i := 0; i < len(datakeySplit); i++ {
		itemName := datakeySplit[i]
		// Descend from the current directory to find the first directory with the item name
		if tree[i] = current.GetSubDirWithItemName(itemName); tree[i] == nil {
			return errors.New("Unable to find level item: " + itemName + " in directory")
		}
		current = tree[i]
	}
	deleteMap := make([]bool, len(datakeySplit))
	end := len(datakeySplit) - 1
	for i := end; i >= 0; i-- {
		if i == end {
			if err2 := removeDirFiles(tree[i]); err2 != nil {
				return err2
			}
			deleteMap[i] = true // This dir was deleted, we'll remove it from the parent's subdir list later
		} else if deleteMap[i+1] {
			tree[i].removeSubDir(tree[i+1].itemName, d.directMap)
		}
		if !tree[i].DirHasSubDirs() {
			if err2 := removeDirFiles(tree[i]); err2 != nil {
				return err2
			}
			deleteMap[i] = true // This dir was deleted, we'll remove it from the parent's subdir list later
		}
	}
	if deleteMap[0] {
		if err2 := removeDirFiles(tree[0]); err2 != nil {
			return err2
		}
		d.removeSubDir(tree[0].itemName, d.directMap)
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
	for _, finfoPtr := range d.datafile {
		tbinfolist = append(tbinfolist, finfoPtr)
	}
	return tbinfolist
}

func (d *Directory) GatherTimeBucketInfo() ([]*io.TimeBucketInfo, error) {
	// Locates a path in the directory and returns the TimeBucketInfo for that path or error if it isn't there
	// Must be thread-safe for READ access
	fileInfoFunc := func(d *Directory, iList interface{}) error {
		if d == nil {
			return errors.New("nil directory is passed")
		}
		pList, ok := iList.(*[]*io.TimeBucketInfo)
		if !ok {
			return fmt.Errorf("failed to cast to timeBucketInfo: %v", iList)
		}
		for _, dfile := range d.datafile {
			*pList = append(*pList, dfile)
		}
		return nil
	}
	fileInfoList := make([]*io.TimeBucketInfo, 0)
	err := d.recurse(&fileInfoList, fileInfoFunc)
	if err != nil {
		return nil, fmt.Errorf("recurse dir to gather time bucket info: %w", err)
	}
	return fileInfoList, nil
}

func (d *Directory) GetLatestTimeBucketInfoFromKey(key *io.TimeBucketKey) (fi *io.TimeBucketInfo, err error) {
	path2 := key.GetPathToYearFiles(d.pathToItemName)
	fullFilePath := path2 + "/1970.bin" // Put a dummy file at the end of the path
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

func (d *Directory) PathToTimeBucketInfo(path2 string) (*io.TimeBucketInfo, error) {
	/*
		Finds the TimeBucketInfo file in this directory based on a full file path argument
	*/
	// Must be thread-safe for READ access
	var tbinfo *io.TimeBucketInfo
	findTimeBucketInfo := func(d *Directory, _ interface{}) error {
		if tbinfo != nil {
			// We have already found our fileinfo match
			return nil
		}
		if d.datafile != nil {
			for _, dfile := range d.datafile {
				if dfile.Path == path2 {
					tbinfo = dfile.GetDeepCopy()
					return nil
				}
			}
		}
		return nil
	}
	err := d.recurse(tbinfo, findTimeBucketInfo)
	if err != nil {
		return nil, fmt.Errorf("find time bucket info from dir: %w", err)
	}
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

func (d *Directory) AddFile(newYear int16) (fInfoPtr *io.TimeBucketInfo, err error) { // d should be a subdirectory
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
	d.RLock()
	if d.datafile == nil {
		d.RUnlock()
		return nil, SubdirectoryDoesNotContainFiles(d.pathToItemName)
	}

	var finfoTemplate *io.TimeBucketInfo
	for _, fi := range d.datafile {
		finfoTemplate = fi
		break
	}
	d.RUnlock()

	newFileInfo := finfoTemplate.GetDeepCopy()
	newFileInfo.Year = newYear
	// Create a new filename for the new file
	d.RLock()
	newFileInfo.Path = path.Join(d.pathToItemName, strconv.Itoa(int(newYear))+".bin")
	d.RUnlock()
	if err = newTimeBucketInfoFromTemplate(newFileInfo); err != nil {
		var targetErr FileAlreadyExists
		if ok := errors.As(err, &targetErr); ok {
			return newFileInfo, nil
		}
		return nil, err
	}
	// Locate the directory in the catalog
	d.Lock()
	d.datafile[newFileInfo.Path] = newFileInfo
	d.Unlock()

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
	return nil, fmt.Errorf("directory path %s not found in catalog", fullFilePath)
}

func (d *Directory) GetOwningSubDirectory(fullFilePath string) (subDir *Directory, err error) {
	// Must be thread-safe for READ access
	dirPath := path.Dir(fullFilePath)
	d.RLock()
	defer d.RUnlock()
	if d.directMap != nil {
		if dir, ok := d.directMap.Load(dirPath); ok {
			directory, ok := dir.(*Directory)
			if !ok {
				return nil, fmt.Errorf("cast directory type: %v", dir)
			}
			return directory, nil
		}
	}
	return nil, fmt.Errorf("directory path %s not found in catalog", fullFilePath)
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

func (d *Directory) GatherCategoriesFromCache() (catSet map[string]struct{}, err error) {
	// Must be thread-safe for WRITE access
	// Provides a map of categories contained within and below this directory. Will create the list cache if nil.
	d.RLock()
	catSet = d.categorySet
	d.RUnlock()
	if catSet == nil {
		return d.gatherCategoriesUpdateCache()
	}
	return catSet, nil
}

func (d *Directory) GatherCategoriesAndItems() (map[string]map[string]int, error) {
	// Must be thread-safe for READ access
	// Provides a map of categories and items within and below this directory
	catList := make(map[string]map[string]int)
	err := d.recurse(catList, catalogListFunc)
	if err != nil {
		return nil, fmt.Errorf("recurse to gather category and items: %w", err)
	}
	return catList, nil
}

func catalogListFunc(d *Directory, itemList interface{}) error {
	// key: category_name(e.g. "Symbol", "Timeframe", "AttribtueGroup", "Year")
	// value: {
	//    key: category value (e.g. "AAPL" if category is "Symbol"}
	//    value: 0
	// }
	list, ok := itemList.(map[string]map[string]int)
	if !ok {
		return fmt.Errorf("unexpected itemList type: %v", itemList)
	}

	if list[d.category] == nil {
		list[d.category] = make(map[string]int)
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
	return nil
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

func (d *Directory) GatherDirectories() ([]string, error) {
	// Must be thread-safe for READ access
	dirListFunc := func(d *Directory, iList interface{}) error {
		pList, ok := iList.(*[]string)
		if !ok {
			return fmt.Errorf("unexpected iList type: %v", iList)
		}
		*pList = append(*pList, d.itemName)
		return nil
	}
	dirList := make([]string, 0)
	err := d.recurse(&dirList, dirListFunc)
	if err != nil {
		return nil, fmt.Errorf("recurse to list directories: %w", err)
	}
	return dirList, nil
}

func (d *Directory) GatherFilePaths() ([]string, error) {
	// Must be thread-safe for READ access
	filePathListFunc := func(d *Directory, iList interface{}) error {
		pList, ok := iList.(*[]string)
		if !ok {
			return fmt.Errorf("unexpected iList type: %v", iList)
		}
		if d.datafile != nil {
			for _, dfile := range d.datafile {
				*pList = append(*pList, dfile.Path)
			}
		}
		return nil
	}
	filePathList := make([]string, 0)
	err := d.recurse(&filePathList, filePathListFunc)
	if err != nil {
		return nil, fmt.Errorf("list file paths: %w", err)
	}
	return filePathList, nil
}

func (d *Directory) gatherCategoriesUpdateCache() (map[string]struct{}, error) {
	// Must be thread-safe for WRITE access
	// Note that this should be called whenever catalog structure is modified to update the cache
	categorySetFunc := func(d *Directory, iList interface{}) error {
		list, ok := iList.(map[string]struct{})
		if !ok {
			return fmt.Errorf("unexpected iList type: %v", iList)
		}
		list[d.category] = struct{}{}
		return nil
	}
	newCategorySet := make(map[string]struct{})
	err := d.recurse(newCategorySet, categorySetFunc)
	if err != nil {
		return nil, fmt.Errorf("gather categories from dir: %w", err)
	}
	d.Lock()
	d.categorySet = newCategorySet
	d.Unlock()

	return newCategorySet, nil
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

func (d *Directory) addSubdir(subDir *Directory, subDirItemName string) {
	subDir.itemName = subDirItemName
	d.categorySet = nil // Reset the category list
	if d.subDirs == nil {
		d.subDirs = make(map[string]*Directory)
	}
	d.subDirs[subDirItemName] = subDir
	if subDir.directMap != nil {
		subDir.directMap.Range(func(key, val interface{}) bool {
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

type levelFunc func(*Directory, interface{}) error // Function for use in recursing into directories

func (d *Directory) recurse(elem interface{}, levelFunc levelFunc) error {
	// Must be thread-safe for READ access
	// Recurse will recurse through a directory, calling levelfunc. Elem is used to pass along a variable.
	d.RLock()
	defer d.RUnlock()

	if err := levelFunc(d, elem); err != nil {
		return fmt.Errorf("execute levelFunc in directory: %w", err)
	}
	if d.subDirs != nil {
		for _, pd := range d.subDirs {
			if err := pd.recurse(elem, levelFunc); err != nil {
				return fmt.Errorf("recurse directory: %w", err)
			}
		}
	}
	return nil
}

func removeDirFiles(td *Directory) error {
	td.Lock()
	defer td.Unlock()

	if err := os.RemoveAll(td.pathToItemName); err != nil {
		return fmt.Errorf("failed to remove directory files under %s: %w", td.pathToItemName, err)
	}
	return nil
}

func newTimeBucketInfoFromTemplate(newTimeBucketInfo *io.TimeBucketInfo) (err error) {
	if newTimeBucketInfo == nil {
		return fmt.Errorf("null fileinfo")
	}

	// If file already exists in this directory, return an error
	if _, err2 := os.Stat(newTimeBucketInfo.Path); err2 == nil {
		return FileAlreadyExists("Can not overwrite file")
	}
	// Create the file
	fp, err := os.OpenFile(newTimeBucketInfo.Path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("open new time bucket info file %s: %w", newTimeBucketInfo.Path, err)
	}
	defer func() {
		if err2 := fp.Close(); err2 != nil {
			log.Error("failed to close time bucket info file: %w", err2)
		}
	}()
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
