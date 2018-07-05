package csv

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/alpacahq/marketstore/executor"
	"github.com/alpacahq/marketstore/utils/io"
	. "github.com/alpacahq/marketstore/utils/log"

	"gopkg.in/yaml.v2"
)

type Configuration struct {
	FirstRowHasColumnNames bool     `yaml:"firstRowHasColumnNames"`
	TimeFormat             string   `yaml:"timeFormat"`
	Timezone               string   `yaml:"timeZone"`
	ColumnNameMap          []string `yaml:"columnNameMap"`
}

func NewConfiguration() *Configuration {
	return new(Configuration)
}

func ReadMetadata(dataFD, controlFD *os.File, dataShapes []io.DataShape) (columnIndex []int, csvReader *csv.Reader, conf *Configuration, err error) {
	conf = NewConfiguration()
	conf.TimeFormat = "1/2/2006 3:04:05 PM" // a default
	conf.Timezone = "UTC"

	var inputColNames []string
	if dataFD == nil {
		fmt.Println("Failed to open data file for loading")
		return nil, nil, nil, err
	}
	if controlFD != nil {
		// We have a loader control file, read the contents
		defer controlFD.Close()

		fs, _ := controlFD.Stat()
		yamfileLen := fs.Size()
		fmt.Printf("Reading control file %s with size %d bytes\n", fs.Name(), yamfileLen)
		yamfile := make([]byte, yamfileLen)
		_, err = controlFD.Read(yamfile)
		err = yaml.Unmarshal(yamfile, conf)
		if err != nil {
			return nil, nil, nil, err
		}
	}

	/*
		Valid row name cases:
			firstRowHasColumnNames	bool
			columnNameMap		[]string

			true:Nil
			1) Column names in the first row, no columnNameMap from the configuration
				- We expect to find all DB column names in the first row names list

			true:ValidList
			2) Column names in the first row, columnNameMap from the configuration
				- Certain column names are renamed in the columnNameMap
				- DB column names will be found in the remapped column names plus the original names

			false:ValidList
			3) No column names in the first row, columnNameMap from the configuration
				- All column names are named in the columnNameMap

			false:Nil
			4) Invalid case - no place is available to find DB column names
	*/
	if !conf.FirstRowHasColumnNames && conf.ColumnNameMap == nil {
		return nil, nil, nil, fmt.Errorf("Not enough info to map DB column names to csv file")
	}

	csvReader = csv.NewReader(dataFD)
	if conf.FirstRowHasColumnNames {
		inputColNames, err = csvReader.Read() // Read the column names
		if err != nil {
			fmt.Println("Error reading first row of column names from data file: " + err.Error())
			return nil, nil, nil, err
		}
	}

	/*
		Setup the column name map
	*/
	switch {
	case conf.FirstRowHasColumnNames && conf.ColumnNameMap == nil:
	case !conf.FirstRowHasColumnNames && conf.ColumnNameMap != nil:
		/*
			We are obtaining column names from user input
			Set the inputColNames to equal the ColumnNameMap
		*/
		inputColNames = make([]string, len(conf.ColumnNameMap))
		for i, name := range conf.ColumnNameMap {
			inputColNames[i] = name
		}
	case conf.FirstRowHasColumnNames && conf.ColumnNameMap != nil:
		/*
			Implement column renaming
		*/
		if len(conf.ColumnNameMap) > len(inputColNames) {
			err = fmt.Errorf("Error: ColumnNameMap from conf file has more entries than the column names from the input file")
			fmt.Println(err.Error())
			return nil, nil, nil, err
		}
		for i, name := range conf.ColumnNameMap {
			if len(name) > 0 {
				inputColNames[i] = name
			}
		}
	}

	/*
		Look for the columns needed in the input file by name (case independent)
	*/

	columnIndex = make([]int, len(dataShapes)) // Maps each DB datum to the input file column number
	for i := range columnIndex {
		columnIndex[i] = -1
	}
	for j, ds := range dataShapes {
		colName := ds.Name
		//		fmt.Println("Name: ", colName)
		for i, inputName := range inputColNames {
			//			fmt.Println("Input Name: ", inputName)
			if strings.EqualFold(colName, inputName) {
				columnIndex[j] = i
			}
		}
	}
	//	fmt.Println("Column Index:", columnIndex)

	var fail bool
	for i := 2; i < len(columnIndex); i++ {
		if columnIndex[i] == -1 {
			fail = true
			fmt.Printf("Unable to find a matching csv column for \"%s\"\n", dataShapes[i].Name)
		}
	}
	if fail {
		return nil, nil, nil, fmt.Errorf("Unable to match all csv file columns to DB columns")
	}

	return columnIndex, csvReader, conf, nil
}

func ReadTimeColumns(csvData [][]string, columnIndex []int, conf *Configuration) (epochCol []int64, nanosCol []int32) {
	var err error
	epochCol = make([]int64, len(csvData))
	nanosCol = make([]int32, len(csvData))
	/*
		Now we can calculate which fields are present that define the Epoch - we either have a pre-defined Epoch
		or we must compose one from a date and time
	*/
	mustComposeEpoch := columnIndex[2] == -1
	if mustComposeEpoch {
		if columnIndex[0] == -1 || columnIndex[1] == -1 {
			fmt.Println("Unable to build Epoch time from mapping - need both a date and time")
			return nil, nil
		}
	}

	/*
		Obtain the time index values
	*/
	// var tzLoc *time.Location
	var tzLoc *time.Location
	if len(conf.Timezone) != 0 {
		tzLoc, err = time.LoadLocation(conf.Timezone)
		if err != nil {
			fmt.Printf("Unable to parse timezone %s: %s\n", conf.Timezone, err.Error())
			return nil, nil
		}
	}

	var dateTime string
	var rowTime time.Time
	firstParse := true
	var formatAdj int
	for i, row := range csvData {
		if mustComposeEpoch {
			rowDateIdx := columnIndex[0]
			rowTimeIdx := columnIndex[1]
			dateTime = row[rowDateIdx] + " " + row[rowTimeIdx]
		} else {
			dateTime = row[columnIndex[2]]
		}
		rowTime, err = parseTime(conf.TimeFormat, dateTime, tzLoc, formatAdj)
		if firstParse && err != nil {
			// Attempt to "tune" the time format
			formatAdj = len(dateTime) - len(conf.TimeFormat)
			if formatAdj > 0 {
				rowTime, err = parseTime(conf.TimeFormat, dateTime, tzLoc, formatAdj)
			}
			firstParse = false
		}
		if err != nil {
			fmt.Printf("Error parsing Epoch column(s) from input data file: %s\n", err.Error())
			return nil, nil
		}
		epochCol[i] = rowTime.UTC().Unix()
		nanosCol[i] = int32(rowTime.UTC().Nanosecond())
	}

	return epochCol, nanosCol
}

func NewWriter(tbk *io.TimeBucketKey, start, end *time.Time) (writer *csv.Writer) {
	var err error
	//var file *os.File
	Log(ERROR, "Failed to create csv file - Error: %v", err)
	// if *_OutputDir != "" && toCsv {
	// 	if end != nil {
	// 		file, err = os.Create(
	// 			fmt.Sprintf("%v_%v_%v.csv",
	// 				tbk.String(),
	// 				start.Format("2006-01-02-15:04"),
	// 				end.Format("2006-01-02-15:04")))
	// 	} else {
	// 		file, err = os.Create(
	// 			fmt.Sprintf("%v_%v.csv", tbk.String(), *start))
	// 		defer file.Close()
	// 	}
	//
	// 	if err != nil {
	// 		Log(ERROR, "Failed to create csv file - Error: %v", err)
	// 		return
	// 	}
	// 	writer = csv.NewWriter(file)
	// }
	return writer
}

// WriteChunk word
func WriteChunk(dbWriter *executor.Writer, dataShapes []io.DataShape, dbKey io.TimeBucketKey, columnIndex []int, csvDataChunk [][]string, conf *Configuration) (start, end time.Time) {

	epochCol, nanosCol := ReadTimeColumns(csvDataChunk, columnIndex, conf)
	if epochCol == nil {
		fmt.Println("Error building time columns from csv data")
		return
	}

	csmInit := io.NewColumnSeriesMap()
	csmInit.AddColumn(dbKey, "Epoch", epochCol)
	csm := ColumnSeriesMapFromCSVData(csmInit, dbKey, csvDataChunk, columnIndex[2:], dataShapes)
	csmInit.AddColumn(dbKey, "Nanoseconds", nanosCol)

	dsMap := make(map[io.TimeBucketKey][]io.DataShape)
	dsMap[dbKey] = dataShapes
	rsMap := csm.ToRowSeriesMap(dsMap)
	rs := rsMap[dbKey]
	if rs.GetNumRows() != len(csvDataChunk) {
		fmt.Println("Error obtaining rows from CSV file - not enough rows converted")
		fmt.Println("Expected: ", len(csvDataChunk), " Got: ", rs.GetNumRows())
		for _, cs := range csm {
			fmt.Println("ColNames: ", cs.GetColumnNames())
		}
		return
	}
	fmt.Printf("beginning to write %d records...", rs.GetNumRows())
	indexTime := make([]time.Time, 0)
	for i := 0; i < rs.GetNumRows(); i++ {
		indexTime = append(indexTime, time.Unix(epochCol[i], int64(nanosCol[i])).UTC())
	}
	dbWriter.WriteRecords(indexTime, rs.GetData())

	executor.ThisInstance.WALFile.RequestFlush()
	fmt.Printf("Done.\n")

	start = time.Unix(epochCol[0], 0).UTC()
	end = time.Unix(epochCol[len(epochCol)-1], 0).UTC()

	return start, end
}

func parseTime(format, dateTime string, tzLoc *time.Location, formatFixupState int) (parsedTime time.Time, err error) {

	dateString := dateTime[:len(dateTime)-formatFixupState]
	if tzLoc != nil {
		parsedTime, err = time.ParseInLocation(format, dateString, tzLoc)
		if err != nil {
			return time.Time{}, err
		}
	} else {
		parsedTime, err = time.Parse(format, dateString)
		if err != nil {
			return time.Time{}, err
		}
	}
	/*
		Attempt to use the remainder of the time field if it fits a known pattern
	*/
	switch formatFixupState {
	case 3:
		remainder := dateTime[len(dateString):]
		millis, err := strconv.ParseInt(remainder, 10, 64)
		if err == nil {
			parsedTime = parsedTime.Add(time.Duration(millis) * time.Millisecond)
		}
	case 7:
		remainder := dateTime[len(dateString)+1:]
		micros, err := strconv.ParseInt(remainder, 10, 64)
		if err == nil {
			parsedTime = parsedTime.Add(time.Duration(micros) * time.Microsecond)
		}
	}
	return parsedTime, nil
}

func columnError(err error, name string) bool {
	if err != nil {
		fmt.Printf("Error obtaining column \"%s\" from csv data\n", name)
		return true
	}
	return false
}

func ColumnSeriesMapFromCSVData(csmInit io.ColumnSeriesMap, key io.TimeBucketKey, csvRows [][]string, columnIndex []int,
	dataShapes []io.DataShape) (csm io.ColumnSeriesMap) {

	if csmInit == nil {
		csm = io.NewColumnSeriesMap()
	} else {
		csm = csmInit
	}
	for i, shape := range dataShapes {
		index := columnIndex[i]
		if index != 0 {
			/*
				We skip the first column, as it's the Epoch and we parse that independently
			*/
			switch shape.Type {
			case io.FLOAT32:
				col, err := GetFloat32ColumnFromCSVRows(csvRows, index)
				if columnError(err, shape.Name) {
					return nil
				}
				csm.AddColumn(key, shape.Name, col)
			case io.FLOAT64:
				col, err := GetFloat64ColumnFromCSVRows(csvRows, index)
				if columnError(err, shape.Name) {
					return nil
				}
				csm.AddColumn(key, shape.Name, col)
			case io.INT32:
				col, err := GetInt32ColumnFromCSVRows(csvRows, index)
				if columnError(err, shape.Name) {
					return nil
				}
				csm.AddColumn(key, shape.Name, col)
			case io.INT64:
				col, err := GetInt64ColumnFromCSVRows(csvRows, index)
				if columnError(err, shape.Name) {
					return nil
				}
				csm.AddColumn(key, shape.Name, col)
			}
		}
	}
	return csm
}

func GetFloat32ColumnFromCSVRows(csvRows [][]string, index int) (col []float32, err error) {
	col = make([]float32, len(csvRows))
	for i, row := range csvRows {
		val, err := strconv.ParseFloat(row[index], 32)
		if err != nil {
			return nil, err
		}
		col[i] = float32(val)
	}
	return col, nil
}

func GetFloat64ColumnFromCSVRows(csvRows [][]string, index int) (col []float64, err error) {
	col = make([]float64, len(csvRows))
	for i, row := range csvRows {
		col[i], err = strconv.ParseFloat(row[index], 64)
		if err != nil {
			return nil, err
		}
	}
	return col, nil
}

func GetInt32ColumnFromCSVRows(csvRows [][]string, index int) (col []int32, err error) {
	col = make([]int32, len(csvRows))
	for i, row := range csvRows {
		val, err := strconv.ParseInt(row[index], 10, 32)
		if err != nil {
			return nil, err
		}
		col[i] = int32(val)
	}
	return col, nil
}

func GetInt64ColumnFromCSVRows(csvRows [][]string, index int) (col []int64, err error) {
	col = make([]int64, len(csvRows))
	for i, row := range csvRows {
		col[i], err = strconv.ParseInt(row[index], 10, 64)
		if err != nil {
			return nil, err
		}
	}
	return col, nil
}
