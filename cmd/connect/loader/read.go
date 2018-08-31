package loader

import (
	"encoding/csv"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/alpacahq/marketstore/utils/io"
	yaml "gopkg.in/yaml.v2"
)

// ReadMetadata returns formatting info about the csv file containing
// the data to be loaded into the database.
func ReadMetadata(dataFD, controlFD *os.File, dataShapes []io.DataShape) (columnIndex []int, csvReader *csv.Reader, conf *Configuration, err error) {
	// Defaults.
	conf = &Configuration{
		TimeFormat: "1/2/2006 3:04:05 PM",
		Timezone:   "UTC",
	}

	var inputColNames []string
	if dataFD == nil {
		fmt.Println("Failed to open data file for loading")
		return nil, nil, nil, err
	}

	if controlFD != nil {
		// We have a loader control file, read the contents
		defer controlFD.Close()

		fs, _ := controlFD.Stat()
		yamlfileLen := fs.Size()
		fmt.Printf("Reading control file %s with size %d bytes\n", fs.Name(), yamlfileLen)
		yamlfile := make([]byte, yamlfileLen)
		_, err = controlFD.Read(yamlfile)
		err = yaml.Unmarshal(yamlfile, conf)
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

// readTimeColumns retuns the epoch and nano columns of a csv file.
func readTimeColumns(csvData [][]string, columnIndex []int, conf *Configuration) (epochCol []int64, nanosCol []int32) {
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
