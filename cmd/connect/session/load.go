package session

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/alpacahq/marketstore/cmd/connect/loader"
	"github.com/alpacahq/marketstore/executor"
	"github.com/alpacahq/marketstore/utils/io"
)

// load executes data loading into the DB from csv files.
func (c *Client) load(line string) {
	args := strings.Split(line, " ")
	args = args[1:]
	if len(args) == 0 {
		fmt.Println("Not enough arguments to load - try help")
		return
	}

	tbk, dataFD, loaderCtl, err := parseLoadArgs(args)
	if err != nil {
		fmt.Printf("Error while parsing arguments: %v\n", err)
		return
	}
	if dataFD != nil {
		defer dataFD.Close()
	}
	fmt.Println("Beginning parse...")
	tbi, err := executor.ThisInstance.CatalogDir.GetLatestTimeBucketInfoFromKey(tbk)
	if err != nil {
		fmt.Printf("Error while generating TimeBucketInfo: %v", err)
		return
	}
	/*
		Obtain a writer
	*/
	dbWriter, err := executor.NewWriter(tbi, executor.ThisInstance.TXNPipe, executor.ThisInstance.CatalogDir)
	if err != nil {
		fmt.Printf("Error return from query scanner: %v", err)
		return
	}

	/*
		Get the metadata key for the writer
	*/
	/*
		var dbKey string
		for _, key := range dbWriter.KeyPathByYear {
			dbKey = filepath.Dir(key)
			break
		}
	*/
	/*
		Obtain the dataShapes for the DB columns
	*/
	dataShapes := make([]io.DataShape, 0)
	/*
		We add a couple of fake data items to the beginning - these are optionally looked for as named columns in the CSV
	*/
	dataShapes = append(dataShapes, io.DataShape{Name: "Epoch-date", Type: io.INT64})
	dataShapes = append(dataShapes, io.DataShape{Name: "Epoch-time", Type: io.INT64})
	fmt.Printf("Column Names from Data Bucket: ")
	for _, shape := range tbi.GetDataShapes() {
		fmt.Printf("%s, ", shape.Name)
		dataShapes = append(dataShapes, shape) // Use the first shape vector in the result, as they should all be the same
	}
	fmt.Printf("\n")

	/*
		Read the metadata about the CSV file
	*/
	columnIndex, csvReader, conf, err := loader.ReadMetadata(dataFD, loaderCtl, dataShapes)
	if err != nil {
		fmt.Println("Error: ", err.Error())
		return
	}
	/*
		Now that the columns in the CSV file are mapped into the columnIndex, we can chop the fake column names off
	*/
	dataShapes = dataShapes[2:]

	var start, end time.Time
	var totalLinesWritten int
	for {
		csvChunk := make([][]string, 0)
		var linesRead int
		for i := 0; i < 1000000; i++ {
			row, err := csvReader.Read()
			if err != nil {
				break
			}
			csvChunk = append(csvChunk, row)
			linesRead++
		}
		if len(csvChunk) == 0 {
			break
		}
		fmt.Printf("Read next %d lines from CSV file...", linesRead)

		l_start, l_end := loader.WriteChunk(dbWriter, dataShapes, *tbk, columnIndex, csvChunk, conf)
		if start.IsZero() {
			start, end = l_start, l_end
		}
		if l_start.Before(start) {
			start = l_start
		}
		if l_end.After(end) {
			end = l_end
		}
		totalLinesWritten += linesRead
	}

	if !start.IsZero() {
		fmt.Printf("%d new lines written\n", totalLinesWritten)
		fmt.Printf("New data written from %v to %v\n", start, end)
	} else {
		fmt.Println("No new data written")
	}
}

func parseLoadArgs(args []string) (mk *io.TimeBucketKey, inputFD, controlFD *os.File, err error) {
	if len(args) < 2 {
		return nil, nil, nil, errors.New("Not enough arguments, see \"\\help load\"")
	}
	mk = io.NewTimeBucketKey(args[0])
	if mk == nil {
		return nil, nil, nil, errors.New("Key is not in proper format, see \"\\help load\"")
	}
	/*
		We need to read two file names that open successfully
	*/
	var first, second bool
	var tryFD *os.File
	for _, arg := range args[1:] {
		fmt.Printf("Opening %s as ", arg)
		tryFD, err = os.Open(arg)
		if err != nil {
			return nil, nil, nil, err
		}
		fs, err := tryFD.Stat()
		if err != nil {
			return nil, nil, nil, err
		}
		if fs.Size() != 0 {
			if first {
				second = true
				controlFD = tryFD
				fmt.Printf("loader control (yaml) file.\n")
				break
			} else {
				first = true
				inputFD = tryFD
				fmt.Printf("data file.\n")
			}
			continue
		} else {
			return nil, nil, nil, err
		}
	}

	if second {
		return mk, inputFD, controlFD, nil
	} else if first {
		return mk, inputFD, nil, nil
	}
	return nil, nil, nil, nil
}
