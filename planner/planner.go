package planner

import (
	"fmt"
	"math"
	"time"

	"strings"

	. "github.com/alpacahq/marketstore/v4/catalog"
	"github.com/alpacahq/marketstore/v4/utils"
	. "github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

type TimeQualFunc func(epoch int64) bool
type RestrictionList map[string][]string                     //Key is category, items list is target
func (r RestrictionList) GetRestrictionMap() RestrictionList { return r }
func (r RestrictionList) AddRestriction(category string, item string) {
	r[category] = append(r[category], item)
}
func (r RestrictionList) getItemList(category string) []string {
	if p_list, ok := r[category]; ok {
		return p_list
	} else {
		return nil
	}
}
func NewRestrictionList() RestrictionList {
	return make(RestrictionList)
}

type DateRange struct {
	Start, End time.Time
}

var MinTime = time.Unix(0, 0)
var MaxTime = time.Unix(1<<63-62135596801, 999999999)

func NewDateRange() *DateRange {
	return &DateRange{
		Start: MinTime,
		End:   MaxTime,
	}
}

type RowLimit struct {
	Number int32
	// -1 backward, 1 forward
	Direction DirectionEnum
}

func NewRowLimit() *RowLimit {
	r := RowLimit{math.MaxInt32, FIRST}
	return &r
}

type QualifiedFile struct {
	Key  TimeBucketKey
	File *TimeBucketInfo
}

type ParseResult struct {
	QualifiedFiles  []QualifiedFile
	Limit           *RowLimit
	Range           *DateRange
	IntervalsPerDay int64
	RootDir         string
	TimeQuals       []TimeQualFunc
}

func NewParseResult() *ParseResult {
	return new(ParseResult)
}

func (pr *ParseResult) GetRecordType() (rt map[TimeBucketKey]EnumRecordType) {
	rt = make(map[TimeBucketKey]EnumRecordType)
	for _, qf := range pr.QualifiedFiles {
		rt[qf.Key] = qf.File.GetRecordType()
	}
	return rt
}

func (pr *ParseResult) GetDataShapes() (dsv map[TimeBucketKey][]DataShape) {
	dsv = make(map[TimeBucketKey][]DataShape)
	for _, qf := range pr.QualifiedFiles {
		/*
			Obtain the dataShapes for the DB columns
		*/
		/*
			Prepend the Epoch column info, as it is not present in the file info but it is in the query data
		*/
		names := []string{"Epoch"}
		types := []EnumElementType{INT64}
		names = append(names, qf.File.GetElementNames()...)
		types = append(types, qf.File.GetElementTypes()...)
		dsv[qf.Key] = NewDataShapeVector(names, types)
	}
	return dsv
}

func (pr *ParseResult) GetRowLen() (rlenMap map[TimeBucketKey]int) {
	rlenMap = make(map[TimeBucketKey]int)
	for _, qf := range pr.QualifiedFiles {
		switch qf.File.GetRecordType() {
		case FIXED:
			rlenMap[qf.Key] = int(qf.File.GetRecordLength())
		case VARIABLE:
			rlenMap[qf.Key] = int(qf.File.GetVariableRecordLength())
		}
	}
	return rlenMap
}

func ElementsEqual(left, right []EnumElementType) (isEqual bool) {
	if len(left) != len(right) {
		return false
	}
	for i, el := range left {
		if el != right[i] {
			return false
		}
	}
	return true
}

func NamesMatch(src, candidates []string) (match bool) {
	srcMap := make(map[string]int)
	for _, el := range src {
		key := strings.ToLower(el)
		srcMap[key] = 0
	}
	for _, el := range candidates {
		key := strings.ToLower(el)
		if _, ok := srcMap[key]; !ok {
			return false
		}
	}
	return true
}

type query struct {
	Range       *DateRange
	Restriction RestrictionList
	Limit       *RowLimit
	DataDir     *Directory
	TimeQuals   []TimeQualFunc
}

func NewQuery(d *Directory) *query {
	if d == nil {
		log.Error("Failed to query - catalog not initialized.")
		return nil
	}
	q := new(query)
	q.DataDir = d
	q.Restriction = NewRestrictionList()
	q.Range = NewDateRange()
	q.Limit = NewRowLimit()
	return q
}

func (q *query) SetRowLimit(direction DirectionEnum, rowLimit int) {
	q.Limit = NewRowLimit()
	q.Limit.Number = int32(rowLimit)
	q.Limit.Direction = direction
}

func (q *query) SetRange(start, end time.Time) {
	q.Range = new(DateRange)
	q.SetStart(start)
	q.SetEnd(end)
}

func (q *query) SetStart(start time.Time) {
	if q.Range == nil {
		q.Range = NewDateRange()
	}
	q.Range.Start = start
}

func (q *query) SetEnd(end time.Time) {
	if q.Range == nil {
		q.Range = NewDateRange()
	}
	q.Range.End = end
}

func (q *query) AddRestriction(category string, item string) {
	q.Restriction.AddRestriction(category, item)
}

func (q *query) AddTargetKey(key *TimeBucketKey) {
	for _, cat := range key.GetCategories() {
		items := key.GetMultiItemInCategory(cat)
		for _, item := range items {
			q.Restriction.AddRestriction(cat, item)
		}
	}
}

func (q *query) AddTimeQual(timeQual TimeQualFunc) {
	q.TimeQuals = append(q.TimeQuals, timeQual)
}

func (q *query) Parse() (pr *ParseResult, err error) {
	// Check to see that the categories in the query are present in the DB directory
	CatList := q.DataDir.GatherCategoriesFromCache()
	for key := range q.Restriction.GetRestrictionMap() {
		if _, ok := CatList[key]; !ok {
			return nil, fmt.Errorf("category: %s not in catalog", key)
		}
	}

	// RootDir
	// rootDir := q.DataDir
	// fmt.Printf("Catlist %v, Root %v\n", CatList, rootDir)
	// fmt.Printf("Range %v\n", q.Range)

	// This method conditionally recurses the directory looking for restricted matches
	// We can not use the simple Directory.Recurse() because of the conditional descent...
	var getFileList func(*Directory, *[]QualifiedFile, string, string)
	getFileList = func(d *Directory, f *[]QualifiedFile, itemKey, categoryKey string) {
		var latestKey *TimeBucketKey
		if d.DirHasSubDirs() {
			//			if p_list, ok := (*q.Restriction)[d.Category]; ok {
			categoryKey += d.GetCategory() + "/"
			list := q.Restriction.getItemList(d.GetCategory())
			// fmt.Printf("-----CategoryKey %v, list %v\n", categoryKey, list)

			if list != nil {
				// Load subdirs matching restriction
				for _, itemName := range list {
					subdirWithItemName := d.GetSubDirWithItemName(itemName)
					if subdirWithItemName != nil {
						getFileList(subdirWithItemName, f, itemKey+itemName+"/", categoryKey)
					}
				}
			} else {
				// Load all subdirs
				for _, subdir := range d.GetListOfSubDirs() {
					getFileList(subdir, f, itemKey+subdir.GetName()+"/", categoryKey)
				}
			}
		} else if itemKey != "" && categoryKey != "" {
			/*
				If there are no subdirs and it's not the root directory, emit the category and item keys
			*/
			itemKey = itemKey[:len(itemKey)-1]
			categoryKey = categoryKey[:len(categoryKey)-1]
			latestKey = NewTimeBucketKey(itemKey, categoryKey)
			// fmt.Println("+++++latestKey:", latestKey)
		}
		// Add all data files - do not limit based on date range here
		if d.DirHasDataFiles() {
			if f == nil {
				f = &([]QualifiedFile{})
			}
			// d.TmpRoot = rootDir
			for _, file := range d.GetTimeBucketInfoSlice() {
				*f = append(*f, QualifiedFile{*latestKey, file})
			}
		}
	}

	// Parse the query in the first pass by finding qualified files
	pr = NewParseResult()
	pr.RootDir = q.DataDir.GetPath()
	/*
		Recurse the directory to produce the QualifiedFiles set
	*/
	getFileList(q.DataDir, &pr.QualifiedFiles, "", "")
	if len(pr.QualifiedFiles) == 0 {
		return pr, fmt.Errorf("No files returned from query parse")
	}

	/*
		Obtain the Timeframe from the qualified files and validate that the files all share the same timeframe
		This is necessary because the IO plan will use timeeframe / interval information to target the data
		location directly
	*/
	for i, qf := range pr.QualifiedFiles {
		if i == 0 {
			pr.IntervalsPerDay = qf.File.GetIntervals()
		}
		if pr.IntervalsPerDay != qf.File.GetIntervals() {
			return pr, fmt.Errorf("Timeframe not the same in result set - File: %v", qf.File.Path)
		}
	}

	// Set the time ranges for the parsed result
	pr.Range = q.Range
	pr.Limit = q.Limit
	// If the query expressed no time range, set the parsed result to include all years in the qualified files
	//timeRange := (q.Range.Start != time.Time{} && q.Range.End != MaxTime)
	timeRange := q.Range.Start != MinTime || q.Range.End != MaxTime
	if !timeRange {
		var startYear, endYear int16
		for i, qf := range pr.QualifiedFiles {
			if i == 0 {
				startYear = qf.File.Year
				endYear = qf.File.Year
				continue
			}
			if qf.File.Year < startYear {
				startYear = qf.File.Year
			}
			if qf.File.Year > endYear {
				endYear = qf.File.Year
			}
		}
		pr.Range.Start = time.Date(
			int(startYear),
			time.January,
			1, 0, 0, 0, 0,
			utils.InstanceConfig.Timezone)
		pr.Range.End = time.Date(
			int(pr.Range.End.Year()),
			time.December,
			31, 23, 59, 59, 999999999,
			utils.InstanceConfig.Timezone)
	}
	pr.TimeQuals = q.TimeQuals
	return pr, nil
}
