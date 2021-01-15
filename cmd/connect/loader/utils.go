package loader

import (
	"encoding/csv"
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v2"

	"github.com/alpacahq/marketstore/v4/utils/io"
)

// CSVConfig is constructed from the control file
// that specifies the formatting of the csv data.
type CSVConfig struct {
	FirstRowHasColumnNames bool     `yaml:"firstRowHasColumnNames"`
	TimeFormat             string   `yaml:"timeFormat"`
	Timezone               string   `yaml:"timeZone"`
	ColumnNameMap          []string `yaml:"columnNameMap"`
}

type CSVMetadata struct {
	Config      *CSVConfig     // Configuration of the CSV file, including the names of the columns
	DSV         []io.DataShape // Datashapes inside this CSV file
	ColumnIndex []int          // Maps the index of the columns in the CSV file to each time bucket in the DB
}

func CSVtoNumpyMulti(csvReader *csv.Reader, tbk io.TimeBucketKey, cvm *CSVMetadata, chunkSize int,
	isVariable bool) (npm *io.NumpyMultiDataset, endReached bool, err error) {

	fmt.Println("Beginning parse...")

	csvChunk := make([][]string, 0)
	var linesRead int
	for i := 0; i < chunkSize; i++ {
		row, err := csvReader.Read()
		if err != nil {
			endReached = true
			break
		}
		csvChunk = append(csvChunk, row)
		linesRead++
	}
	if len(csvChunk) == 0 {
		return nil, true, nil
	}
	fmt.Printf("Read next %d lines from CSV file...\n", linesRead)

	csm, err := convertCSVtoCSM(tbk, cvm, csvChunk)
	if err != nil {
		return nil, false, err
	}

	if !isVariable {
		csm[tbk].Remove("Nanoseconds")
	}

	np, err := io.NewNumpyDataset(csm[tbk])
	if err != nil {
		return nil, false, err
	}
	npm, err = io.NewNumpyMultiDataset(np, tbk)

	return npm, endReached, nil
}

// ReadMetadata returns formatting info about the csv file containing
// the data to be loaded into the database.
func ReadMetadata(dataFD, controlFD *os.File, dbDataShapes []io.DataShape) (csvReader *csv.Reader, cvm *CSVMetadata, err error) {
	fmt.Println("DB Data Shapes: ", dbDataShapes)

	cvm = &CSVMetadata{}

	/*
		We add a couple of fake data items to the beginning - these are optionally looked for as named columns in the CSV
		The fake columns are cut off after the mapping process, leaving only the single EPOCH column
	*/
	cvm.DSV = make([]io.DataShape, 0)
	cvm.DSV = append(cvm.DSV, io.DataShape{Name: "Epoch-date", Type: io.INT64})
	cvm.DSV = append(cvm.DSV, io.DataShape{Name: "Epoch-time", Type: io.INT64})
	for _, shape := range dbDataShapes {
		cvm.DSV = append(cvm.DSV, shape)
	}

	var inputColNames []string
	if dataFD == nil {
		fmt.Println("Failed to open data file for loading")
		return nil, nil, err
	}

	if controlFD != nil {
		// We have a loader control file, read the contents
		cvm.Config, err = readControlFile(controlFD)
		if err != nil {
			return nil, nil, err
		}
	} else {
		// Defaults.
		cvm.Config = &CSVConfig{
			TimeFormat: "1/2/2006 3:04:05 PM",
			Timezone:   "UTC",
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
	if !cvm.Config.FirstRowHasColumnNames && cvm.Config.ColumnNameMap == nil {
		return nil, nil, fmt.Errorf("Not enough info to map DB column names to csv file")
	}

	csvReader = csv.NewReader(dataFD)
	if cvm.Config.FirstRowHasColumnNames {
		inputColNames, err = csvReader.Read() // Read the column names
		if err != nil {
			fmt.Println("Error reading first row of column names from data file: " + err.Error())
			return nil, nil, err
		}
	}

	/*
		Setup the column name map
	*/
	switch {
	case cvm.Config.FirstRowHasColumnNames && cvm.Config.ColumnNameMap == nil:
		for i, name := range inputColNames {
			inputColNames[i] = strings.TrimSpace(name)
		}
	case !cvm.Config.FirstRowHasColumnNames && cvm.Config.ColumnNameMap != nil:
		/*
			We are obtaining column names from user input
			Set the inputColNames to equal the ColumnNameMap
		*/
		inputColNames = make([]string, len(cvm.Config.ColumnNameMap))
		for i, name := range cvm.Config.ColumnNameMap {
			inputColNames[i] = name
		}
	case cvm.Config.FirstRowHasColumnNames && cvm.Config.ColumnNameMap != nil:
		/*
			Implement column renaming
		*/
		if len(cvm.Config.ColumnNameMap) > len(inputColNames) {
			err = fmt.Errorf("Error: ColumnNameMap from conf file has more entries than the column names from the input file")
			fmt.Println(err.Error())
			return nil, nil, err
		}
		for i, name := range cvm.Config.ColumnNameMap {
			if len(name) > 0 {
				inputColNames[i] = name
			}
		}
	}

	/*
		Look for the columns needed in the input file by name (case independent)
	*/

	cvm.ColumnIndex = make([]int, len(cvm.DSV)) // Maps each DB datum to the input file column number
	for i := range cvm.ColumnIndex {
		cvm.ColumnIndex[i] = -1
	}
	for j, ds := range cvm.DSV {
		colName := ds.Name
		//		fmt.Println("Name: ", colName)
		for i, inputName := range inputColNames {
			//			fmt.Println("Input Name: ", inputName)
			if strings.EqualFold(colName, inputName) {
				cvm.ColumnIndex[j] = i
			}
		}
	}
	//	fmt.Println("Column Index:", columnIndex)

	/*
		Now we can remove the fake column names at the beginning of the DSV
	*/
	cvm.DSV = cvm.DSV[2:]

	var fail bool
	for i := 2; i < len(cvm.ColumnIndex); i++ {
		if cvm.ColumnIndex[i] == -1 {
			fail = true
			fmt.Printf("Unable to find a matching csv column for \"%s\"\n", cvm.DSV[i-2].Name)
		}
	}
	if fail {
		return nil, nil, fmt.Errorf("Unable to match all csv file columns to DB columns")
	}

	return csvReader, cvm, nil
}

func convertCSVtoCSM(tbk io.TimeBucketKey, cvm *CSVMetadata, csvDataChunk [][]string) (csm io.ColumnSeriesMap, err error) {
	epochCol, nanosCol := readTimeColumns(csvDataChunk, cvm.ColumnIndex, cvm.Config)
	if epochCol == nil {
		fmt.Println("Error building time columns from csv data")
		return
	}

	csmInit := io.NewColumnSeriesMap()
	csmInit.AddColumn(tbk, "Epoch", epochCol)
	csm = columnSeriesMapFromCSVData(csmInit, tbk, csvDataChunk, cvm.ColumnIndex[2:], cvm.DSV)
	csm.AddColumn(tbk, "Nanoseconds", nanosCol)

	return csm, err
}

func readControlFile(controlFD *os.File) (cf *CSVConfig, err error) {
	if controlFD == nil {
		return
	}
	// We have a loader control file, read the contents
	defer controlFD.Close()

	cf = &CSVConfig{}
	fs, _ := controlFD.Stat()
	yamlfileLen := fs.Size()
	yamlfile := make([]byte, yamlfileLen)
	_, err = controlFD.Read(yamlfile)
	if err != nil {
		return nil, err
	}
	err = yaml.Unmarshal(yamlfile, cf)
	if err != nil {
		return nil, err
	}

	return cf, nil
}
