package io

import (
	"fmt"
	"github.com/alpacahq/marketstore/v4/utils/log"
	"github.com/pkg/errors"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"
)

//go:generate ./generateMethods.sh generatedMethods.go

/*
ColumnSeries stores query results using the following keys:
- Key1: Metadata key for filesystem
- Key2: Data column name
- Interface: Data for each key
Ex:
	mymap["AAPL/1Min/OHLC"]["open"] = []byte{}
*/
type ColumnInterface interface {
	GetColumn(string) interface{}
	GetDataShapes() []DataShape
	Len() int
	GetTime() ([]time.Time, error)
}

type ColumnSeries struct {
	ColumnInterface

	columns          map[string]interface{}
	orderedNames     []string
	nameIncrement    map[string]int
}

func NewColumnSeries() *ColumnSeries {
	cs := new(ColumnSeries)
	cs.columns = make(map[string]interface{})
	cs.nameIncrement = make(map[string]int)
	return cs
}

func (cs *ColumnSeries) GetColumn(name string) interface{} {
	return cs.GetByName(name)
}

func (cs *ColumnSeries) GetDataShapes() (ds []DataShape) {
	var et []EnumElementType
	for _, name := range cs.orderedNames {
		// fmt.Printf("name %v, type %v\n", name, GetElementType(cs.columns[name]))
		et = append(et, GetElementType(cs.columns[name]))
	}
	return NewDataShapeVector(cs.orderedNames, et)
}

func (cs *ColumnSeries) Len() int {
	if len(cs.orderedNames) == 0 {
		return 0
	}
	i_col := cs.GetByName(cs.orderedNames[0])
	return reflect.ValueOf(i_col).Len()
}

func (cs *ColumnSeries) GetTime() ([]time.Time, error) {
	ep, ok := cs.GetColumn("Epoch").([]int64)
	if !ok {
		return nil, errors.New("unexpected data type for Epoch column.")
	}

	ts := make([]time.Time, len(ep))
	nsi := cs.GetColumn("Nanoseconds")
	if nsi == nil {
		for i, secs := range ep {
			ts[i] = ToSystemTimezone(time.Unix(secs, 0))
		}
	} else {
		ns, ok := nsi.([]int32)
		if !ok {
			return nil, errors.New("unexpected data type for Nanoseconds column.")
		}
		for i, secs := range ep {
			ts[i] = ToSystemTimezone(time.Unix(secs, int64(ns[i])))
		}
	}
	return ts, nil
}

func (cs *ColumnSeries) GetColumnNames() (columnNames []string) {
	return cs.orderedNames
}

func (cs *ColumnSeries) GetColumns() map[string]interface{} {
	return cs.columns
}

func (cs *ColumnSeries) AddColumn(name string, columnData interface{}) (outname string) {
	if _, ok := cs.columns[name]; ok {
		// Name collision, make the name unique
		if _, ok := cs.nameIncrement[name]; !ok {
			cs.nameIncrement[name] = 0
		} else {
			cs.nameIncrement[name]++
		}
		name = name + strconv.Itoa(cs.nameIncrement[name])
	}
	cs.orderedNames = append(cs.orderedNames, name)
	cs.columns[name] = columnData
	return name
}
func (cs *ColumnSeries) IsEmpty() bool {
	return len(cs.orderedNames) == 0
}
func (cs *ColumnSeries) GetNumColumns() (length int) {
	if cs.IsEmpty() {
		return 0
	}
	return len(cs.orderedNames)
}
func (cs *ColumnSeries) Rename(newName, oldName string) error {
	/*
		Renames one column named "targetName" for another named "srcName"
	*/
	oldColumn := cs.GetByName(oldName)
	if oldColumn == nil {
		return fmt.Errorf("Error: Source column named %s does not exist\n", oldName)
	}

	/*
		If the new name already exists in the source, remove it first
	*/
	if cs.Exists(newName) {
		cs.Remove(newName)
	}

	/*
		Put new name in same place as old name in a new name list
	*/
	var newNames []string
	for _, name := range cs.orderedNames {
		if name == oldName {
			newNames = append(newNames, newName)
		} else {
			newNames = append(newNames, name)
		}
	}

	cs.AddColumn(newName, oldColumn)
	cs.Remove(oldName)
	cs.orderedNames = newNames
	return nil
}

func (cs *ColumnSeries) Replace(targetName string, col interface{}) error {
	if err := cs.Remove(targetName); err != nil {
		return err
	}
	cs.AddColumn(targetName, col)
	return nil
}
func (cs *ColumnSeries) Remove(targetName string) error {
	if !cs.Exists(targetName) {
		return fmt.Errorf("Error: Source column named %s does not exist\n", targetName)
	}
	var newNames []string
	for _, name := range cs.orderedNames {
		if !strings.EqualFold(name, targetName) {
			newNames = append(newNames, name)
		}
	}
	cs.orderedNames = newNames
	delete(cs.columns, targetName)
	return nil
}
func (cs *ColumnSeries) Project(keepList []string) error {
	newCols := make(map[string]interface{})
	var newNames []string
	for _, name := range keepList {
		col := cs.GetByName(name)
		if col == nil {
			return fmt.Errorf("Column named: %s not found", name)
		}
		newCols[name] = col
		newNames = append(newNames, name)
	}
	cs.columns = newCols
	cs.orderedNames = newNames
	return nil
}

/*
RestrictLength applies a FIRST/LAST length restriction to this series
*/
func (cs *ColumnSeries) RestrictLength(newLen int, direction DirectionEnum) (err error) {
	for key, col := range cs.columns {
		cs.columns[key], err = DownSizeSlice(col, newLen, direction)
		if err != nil {
			return err
		}
	}
	return nil
}

func (cs *ColumnSeries) Exists(targetName string) bool {
	if _, ok := cs.columns[targetName]; !ok {
		return false
	}
	return true
}

func (cs *ColumnSeries) GetByName(name string) interface{} {
	if !cs.Exists(name) {
		return nil
	} else {
		return cs.columns[name]
	}
}

func (cs *ColumnSeries) GetEpoch() []int64 {
	col := cs.GetByName("Epoch")
	if col == nil {
		return nil
	} else {
		return col.([]int64)
	}
}

func (cs *ColumnSeries) ToRowSeries(itemKey TimeBucketKey, alignData bool) (rs *RowSeries) {
	dsv := cs.GetDataShapes()
	data, recordLen := SerializeColumnsToRows(cs, dsv, alignData)
	rs = NewRowSeries(itemKey, data, dsv, recordLen, NOTYPE)
	return rs
}

func (cs *ColumnSeries) AddNullColumn(ds DataShape) {
	cs.AddColumn(ds.Name, ds.Type.SliceOf(cs.Len()))
}

// ApplyTimeQual takes a function that determines whether or
// not a given epoch time is valid, and applies that function
// to the ColumnSeries, removing invalid entries.
func (cs *ColumnSeries) ApplyTimeQual(tq func(epoch int64) bool) *ColumnSeries {
	indexes := []int{}

	out := &ColumnSeries{
		orderedNames:     cs.orderedNames,
		nameIncrement:    cs.nameIncrement,
		columns:          map[string]interface{}{},
	}

	for i, epoch := range cs.GetEpoch() {
		if tq(epoch) {
			indexes = append(indexes, i)
		}
	}

	for name, col := range cs.columns {
		iv := reflect.ValueOf(col)
		slc := reflect.MakeSlice(reflect.TypeOf(col), 0, 0)

		for _, index := range indexes {
			slc = reflect.Append(slc, iv.Index(index))
		}

		out.columns[name] = slc.Interface()
	}

	return out
}

// SliceColumnSeriesByEpoch slices the column series by the provided epochs,
// returning a new column series with only records occurring
// between the two provided epoch times. If only one is provided,
// only one is used to slice and all remaining records are also
// returned.
func SliceColumnSeriesByEpoch(cs ColumnSeries, start, end *int64) (slc ColumnSeries, err error) {
	slc = ColumnSeries{
		orderedNames:     cs.orderedNames,
		nameIncrement:    cs.nameIncrement,
		columns:          map[string]interface{}{},
	}

	for name, col := range cs.columns {
		slc.columns[name] = col
	}

	epochs := slc.GetEpoch()

	var index int

	if start != nil {
		for ; index < len(epochs); index++ {
			if epochs[index] >= *start {
				if err = slc.RestrictLength(len(epochs)-index, LAST); err != nil {
					return
				}
				break
			}
		}
	}

	if end != nil {
		epochs = slc.GetEpoch()
		for index = len(epochs) - 1; index >= 0; index-- {
			if epochs[index] < *end {
				if err = slc.RestrictLength(index+1, FIRST); err != nil {
					return
				}
				break
			}
		}
	}

	return
}

func max(x, y int) int {
	if x > y {
		return x
	}
	return y
}

// ColumnSeriesUnion takes to column series and creates a union
// and returns another column series. The values in the union
// are unique, and right values overwrite left values in when
// epochs are duplicated.
func ColumnSeriesUnion(left, right *ColumnSeries) *ColumnSeries {
	out := NewColumnSeries()

	out.orderedNames = left.orderedNames
	out.nameIncrement = make(map[string]int, len(left.nameIncrement))
	for k, v := range left.nameIncrement {
		out.nameIncrement[k] = v
	}

	type entry struct {
		epoch     int64
		index     int
		refSeries *ColumnSeries
	}

	m := map[int64]*entry{}

	for i, epoch := range left.GetEpoch() {
		m[epoch] = &entry{epoch: epoch, index: i, refSeries: left}
	}

	for i, epoch := range right.GetEpoch() {
		m[epoch] = &entry{epoch: epoch, index: i, refSeries: right}
	}

	entries := make([]*entry, len(m))
	i := 0

	for _, entry := range m {
		entries[i] = entry
		i++
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].epoch < entries[j].epoch })

	for _, entry := range entries {
		rs := entry.refSeries
		for name, col := range rs.columns {
			iv := reflect.ValueOf(col)

			if _, ok := out.columns[name]; !ok {
				slc := reflect.MakeSlice(reflect.TypeOf(col), 0, 0)
				slc = reflect.Append(slc, iv.Index(entry.index))
				out.columns[name] = slc.Interface()
			} else {
				outCol := out.columns[name]
				ov := reflect.ValueOf(outCol)
				ov = reflect.Append(ov, iv.Index(entry.index))
				out.columns[name] = ov.Interface()
			}
		}
	}

	return out
}

type ColumnSeriesMap map[TimeBucketKey]*ColumnSeries

func NewColumnSeriesMap() ColumnSeriesMap {
	return make(ColumnSeriesMap)
}

func (csm ColumnSeriesMap) IsEmpty() bool {
	return len(csm) == 0
}
func (csm ColumnSeriesMap) GetMetadataKeys() (keys []TimeBucketKey) {
	keys = make([]TimeBucketKey, 0)
	for key := range csm {
		keys = append(keys, key)
	}
	return keys
}

func (csm ColumnSeriesMap) AddColumnSeries(key TimeBucketKey, cs *ColumnSeries) {
	for _, name := range cs.orderedNames {
		csm.AddColumn(key, name, cs.columns[name])
	}
}
func (csm ColumnSeriesMap) AddColumn(key TimeBucketKey, name string, columnData interface{}) {
	if _, ok := csm[key]; !ok {
		csm[key] = NewColumnSeries()
	}
	csm[key].AddColumn(name, columnData)
}

func (csm ColumnSeriesMap) ToRowSeriesMap(dataShapesMap map[TimeBucketKey][]DataShape, alignData bool) (rsMap map[TimeBucketKey]*RowSeries) {
	rsMap = make(map[TimeBucketKey]*RowSeries)
	for key, columns := range csm {
		rsMap[key] = columns.ToRowSeries(key, alignData)
	}
	return rsMap
}

// FilterColumns removes columns other than the specified columns from all ColumnSeries in a ColumnSeriesMap.
func (csm *ColumnSeriesMap) FilterColumns(columns []string) {
	if len(columns) == 0 {
		return
	}

	// index columns (=Epoch and Nanoseconds) are always necessary and Epoch should be the first column
	keepColumns := []string{"Epoch"}
	keepColumns = append(keepColumns, columns...)
	keepColumns = append(keepColumns, "Nanoseconds")

	for _, cs := range *csm {
		// filter out unnecessary columns
		err := cs.Project(keepColumns)
		if err != nil {
			log.Error("failed to filter out columns", keepColumns)
		}
	}
}

func GetNamesFromDSV(dataShapes []DataShape) (out []string) {
	for _, shape := range dataShapes {
		out = append(out, shape.Name)
	}
	return out
}
func GetDSVFromInterface(i_dsv interface{}) (out []DataShape) {
	if i_dsv != nil {
		if _, ok := i_dsv.([]DataShape); ok {
			return i_dsv.([]DataShape)
		}
	}
	return nil
}
func GetStringSliceFromInterface(i_ss interface{}) (out []string) {
	if i_ss != nil {
		if _, ok := i_ss.([]string); ok {
			return i_ss.([]string)
		}
	}
	return nil
}

func ExtractDatashapesByNames(dsv []DataShape, names []string) (out []DataShape) {
	dsm := make(map[string]DataShape)
	for _, shape := range dsv {
		dsm[shape.Name] = shape
	}
	for _, name := range names {
		if _, ok := dsm[name]; ok {
			out = append(out, dsm[name])
		}
	}
	return out
}

func GetMissingAndTypeCoercionColumns(requiredDSV, availableDSV []DataShape) (missing,
	coercion []DataShape) {
	/*
		We need to find out which columns are missing and which are present,
		but of the wrong type (Type Mismatch).
		- For Type Mismatch cols, we will attempt to coerce their type to match
		- For missing cols, we will add columns with null data of the correct
		  type
	*/
	availableDSVSet, _ := NewAnySet(availableDSV)
	if availableDSVSet.Contains(requiredDSV) {
		return nil, nil
	} else {
		// The required datashapes are not found in the cols
		requiredDSVSet, _ := NewAnySet(requiredDSV)
		// missingDSV reflects both missing columns and ones with incorrect type
		i_missingDSV := requiredDSVSet.Subtract(availableDSV)
		missingDSV := GetDSVFromInterface(i_missingDSV)

		// Find the missing column names
		requiredNamesSet, _ := NewAnySet(GetNamesFromDSV(requiredDSV))
		i_allMissingNames := requiredNamesSet.Subtract(GetNamesFromDSV(availableDSV))
		allMissingNames := GetStringSliceFromInterface(i_allMissingNames)
		/*
			If the number of missing (name+types) is not the same as the missing names
			then we know that there are more (name+types) than names missing, so
			we will have to isolate missing columns from those that need type coercion
		*/
		switch {
		case len(missingDSV) == len(allMissingNames):
			return ExtractDatashapesByNames(requiredDSV, allMissingNames), nil
		case len(missingDSV) != len(allMissingNames):
			//We have to coerce types
			missingDSVNamesSet, _ := NewAnySet(GetNamesFromDSV(missingDSV))
			i_needCoercionCols := missingDSVNamesSet.Subtract(allMissingNames)
			needCoercionCols := GetStringSliceFromInterface(i_needCoercionCols)
			return ExtractDatashapesByNames(requiredDSV, allMissingNames),
				ExtractDatashapesByNames(requiredDSV, needCoercionCols)
		}
	}
	return nil, nil
}

func SerializeColumnsToRows(cs *ColumnSeries, dataShapes []DataShape, align64 bool) (data []byte, recordLen int) {
	/*
		The columns data shapes may or may not contain the Epoch column
	*/
	var shapesContainsEpoch bool

	// Find out how much of the required datashapes are contained in the Column Series
	missing, needcoercion := GetMissingAndTypeCoercionColumns(
		dataShapes,
		cs.GetDataShapes(),
	)

	// Add in the null columns needed to complete the set
	for _, shape := range missing {
		cs.AddNullColumn(shape)
	}
	// Coerce column types as needed
	for _, shape := range needcoercion {
		cs.CoerceColumnType(shape)
	}

	/*
		Generate an ordered array from the map of columns, ordered by the data shapes
	*/
	colInBytesList := make([][]byte, 0, len(dataShapes))
	for _, shape := range dataShapes {
		colName := shape.Name
		if strings.EqualFold(colName, "Epoch") {
			shapesContainsEpoch = true
		}
		columnData := cs.columns[colName]
		colInBytes := SwapSliceData(columnData, byte(0)).([]byte)
		colInBytesList = append(colInBytesList, colInBytes)
	}
	if !shapesContainsEpoch {
		return nil, 0
	}

	/*
		Calculate the resulting recordLen
	*/
	for _, shape := range dataShapes {
		recordLen += shape.Type.Size()
	}
	var padbuf []byte
	if align64 {
		alignedRecLen := AlignedSize(recordLen)
		padding := alignedRecLen - recordLen
		recordLen = alignedRecLen
		padbuf = make([]byte, padding)
	}

	epochCol := cs.columns["Epoch"].([]int64)
	data = make([]byte, 0, recordLen*len(epochCol))
	for i, epoch := range epochCol {
		data, _ = Serialize(data, epoch)
		for j, shape := range dataShapes {
			if strings.EqualFold(shape.Name, "Epoch") {
				continue
			}
			word := shape.Type.SliceInBytesAt(colInBytesList[j], i)
			data = append(data, word...)
		}

		if align64 {
			if len(padbuf) > 0 {
				data = append(data, padbuf...)
			}
		}
	}

	// data is a concatenation of the byte representation of each row, including Epoch column.
	// e.g. the records are ([
	//	(\x01\x02\x03\x04\x05\x06\x07\x08, \x09\x0A\x0B\x0C\x0D\x0E\x0F\x10), // 2 Epochs
	//  (\x11\x12, \x13\x14)												  // 2 Asks
	//  ], ("Epoch", "Ask")),
	// => data = \x01\x02\x03\x04\x05\x06\x07\x08\x11\x12\x09\x0A\x0B\x0C\x0D\x0E\x0F\x10\x13\x14
	// (Epoch1-Ask1-Epoch2-Ask2)
	return data, recordLen
}
