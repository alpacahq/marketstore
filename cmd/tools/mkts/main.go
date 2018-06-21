package main

import (
	"bytes"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"os/user"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/alpacahq/marketstore/SQLParser"
	"github.com/alpacahq/marketstore/cmd/tools/mkts/csvreader"
	"github.com/alpacahq/marketstore/executor"
	"github.com/alpacahq/marketstore/frontend"
	"github.com/alpacahq/marketstore/frontend/client"
	"github.com/alpacahq/marketstore/planner"
	"github.com/alpacahq/marketstore/utils"
	. "github.com/alpacahq/marketstore/utils/io"
	. "github.com/alpacahq/marketstore/utils/log"
	"github.com/chzyer/readline"
)

var _ConnectURL = flag.String("serverURL", "", "network connect to server at \"hostname:port\"")
var _RootDir = flag.String("rootDir", "", "input directory when used in local mode")
var _OutputDir = flag.String("outputDir", "", "output directory for csv files")
var toCsv bool = false
var timingForSQL bool = true // default is: print timing after SQL queries
var baseURL string           // built from the _ConnectURL
var localMode bool

func usage(w io.Writer) {
	io.WriteString(w, "commands:\n")
	io.WriteString(w, completer.Tree("    "))
}

var completer = readline.NewPrefixCompleter(
	readline.PcItem("\\show"),
	readline.PcItem("\\load"),
	readline.PcItem("\\create"),
	readline.PcItem("\\trim"),
	readline.PcItem("\\help"),
	readline.PcItem("\\exit"),
	readline.PcItem("\\quit"),
	readline.PcItem("\\q"),
	readline.PcItem("\\?"),
	readline.PcItem("\\stop"),
)

/*
	Mkts operates in one of two modes:
	remote) connects to a port on a marketstore server
	local) works on a local data directory
*/
func main() {
	flag.Parse()

	gracefulExitOnCTRLC()

	/*
		Determine what operating mode we're in
	*/
	switch {
	case len(*_ConnectURL) != 0 && len(*_RootDir) != 0: // Prefer network connection with ambiguous conditions
		fmt.Printf("Network connection to %s specified, ignoring rootDir %s\n",
			*_ConnectURL, *_RootDir)
		fallthrough
	case len(*_ConnectURL) != 0 && len(*_RootDir) == 0:
		splits := strings.Split(*_ConnectURL, ":")
		if len(splits) != 2 {
			fmt.Printf("Incorrect URL, need \"hostname:port\", have: %s\n", *_ConnectURL)
			os.Exit(1)
		}
		baseURL = "http://" + *_ConnectURL + "/"
		localMode = false
	case len(*_ConnectURL) == 0 && len(*_RootDir) != 0: // Local connection
		localMode = true
	default:
		fmt.Println("One of serverURL or rootDir must be specified")
		os.Exit(1)
	}

	if localMode {
		fmt.Printf("Running in local mode on directory: %s\n", *_RootDir)
		initCatalog, initWALCache, backgroundSync, WALBypass := true, true, false, true
		utils.InstanceConfig.WALRotateInterval = 5
		executor.NewInstanceSetup(*_RootDir, initCatalog, initWALCache, backgroundSync, WALBypass)
	} else {
		fmt.Printf("Running in network connection mode to: %s\n", *_ConnectURL)
	}

	rl, err := setupCommandHistory()
	if err != nil {
		panic(err)
	}
	defer rl.Close()

	/*
		Command processing loop
	*/
	for {
		line, err := rl.Readline()
		if err != nil { // io.EOF, readline.ErrInterrupt
			break
		}
		line = strings.Trim(line, " ")
		switch {
		case strings.HasPrefix(line, "\\timing"):
			timingForSQL = !timingForSQL
		case strings.HasPrefix(line, "\\show"):
			processQuery(line)
		case strings.HasPrefix(line, "\\trim"):
			processTrim(line)
		case strings.HasPrefix(line, "\\gaps"):
			processGapFinder(line)
		case strings.HasPrefix(line, "\\load"):
			processLoad(line)
		case strings.HasPrefix(line, "\\create"):
			processCreate(line)
		case strings.HasPrefix(line, "\\help") || strings.HasPrefix(line, "\\?"):
			processHelp(line)
		case line == "help":
			usage(rl.Stderr())
		case line == "\\stop", line == "\\quit", line == "\\q", line == "exit":
			os.Exit(0)
		case line == "", line == " ":
			continue
		default:
			/*
				SQL Statement - send it to the system
			*/
			processSQL(line)
		}
	}
}

func DataShapesFromInputString(inputStr string) (dsa []DataShape, err error) {
	splitString := strings.Split(inputStr, ":")
	dsa = make([]DataShape, 0)
	for _, group := range splitString {
		twoParts := strings.Split(group, "/")
		if len(twoParts) != 2 {
			err = fmt.Errorf("Error: %s: Data shape is not described by a list of column names followed by type.", group)
			fmt.Println(err.Error())
			return nil, err
		}
		elementNames := strings.Split(twoParts[0], ",")
		elementType := twoParts[1]
		eType := EnumElementTypeFromName(elementType)
		if eType == NONE {
			err = fmt.Errorf("Error: %s: Data type is not a supported type", group)
			fmt.Println(err.Error())
			return nil, err
		}
		for _, name := range elementNames {
			dsa = append(dsa, DataShape{Name: name, Type: eType})
		}
	}
	return dsa, nil
}

func processSQLLocal(line string) (cs *ColumnSeries, err error) {
	ast, err := SQLParser.NewAstBuilder(line)
	if err != nil {
		return nil, err
	}
	es, err := SQLParser.NewExecutableStatement(ast.Mtree)
	if err != nil {
		return nil, err
	}
	cs, err = es.Materialize()
	if err != nil {
		return nil, err
	}
	return cs, nil
}

func processSQLRemote(line string) (cs *ColumnSeries, err error) {
	req := frontend.QueryRequest{
		IsSQLStatement: true,
		SQLStatement:   line,
	}
	args := &frontend.MultiQueryRequest{Requests: []frontend.QueryRequest{req}}
	cl, err := client.NewClient(baseURL)
	if err != nil {
		return nil, err
	}
	resp, err := cl.DoRPC("Query", args)
	if err != nil {
		return nil, err
	}

	for _, sub := range *resp.(*ColumnSeriesMap) {
		cs = sub
		break
	}
	return cs, err
}

func processSQL(line string) {
	timeStart := time.Now()
	var err error
	var cs *ColumnSeries
	if localMode {
		cs, err = processSQLLocal(line)
	} else {
		cs, err = processSQLRemote(line)
	}

	if err != nil {
		fmt.Println(err)
		return
	}
	runTime := time.Since(timeStart)

	err = printResult(line, cs)
	if err != nil {
		fmt.Println(err.Error())
	}
	if timingForSQL {
		fmt.Printf("Elapsed query time: %5.3f ms\n", 1000*runTime.Seconds())
	}
}

func processCreate(line string) {
	args := strings.Split(line, " ")
	args = args[1:] // chop off the first word which should be "create"
	parts := strings.Split(args[0], ":")
	if len(parts) != 2 {
		fmt.Println("Key is not in proper format, see \"\\help create\" ")
		return
	}
	tbk := NewTimeBucketKey(parts[0], parts[1])
	if tbk == nil {
		fmt.Println("Key is not in proper format, see \"\\help create\" ")
		return
	}

	dsv, err := DataShapesFromInputString(args[1])
	if err != nil {
		return
	}

	rowType := args[2]
	switch rowType {
	case "fixed", "variable":
	default:
		fmt.Printf("Error: Record type \"%s\" is not one of fixed or variable\n", rowType)
		return
	}

	rootDir := executor.ThisInstance.RootDir
	year := int16(time.Now().Year())
	tf, err := tbk.GetTimeFrame()
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	rt := EnumRecordTypeByName(rowType)
	tbinfo := NewTimeBucketInfo(*tf, tbk.GetPathToYearFiles(rootDir), "Default", year, dsv, rt)

	err = executor.ThisInstance.CatalogDir.AddTimeBucket(tbk, tbinfo)
	if err != nil {
		err = fmt.Errorf("Error: Creation of new catalog entry failed: %s", err.Error())
		fmt.Println(err.Error())
		return
	}

	fmt.Printf("Successfully created a new catalog entry: %s\n", tbk.GetItemKey())
}

func processTrim(line string) {
	Log(INFO, "Trimming...")
	args := strings.Split(line, " ")
	if len(args) < 3 {
		fmt.Println("Not enough arguments - need \"trim key date\"")
		return
	}
	trimDate, err := parseTime(args[len(args)-1])
	if err != nil {
		Log(ERROR, "Failed to parse trim date - Error: %v", trimDate)
	}
	fInfos := executor.ThisInstance.CatalogDir.GatherTimeBucketInfo()
	for _, info := range fInfos {
		if info.Year == int16(trimDate.Year()) {
			offset := TimeToOffset(trimDate, info.GetTimeframe(), info.GetRecordLength())
			fp, err := os.OpenFile(info.Path, os.O_CREATE|os.O_RDWR, 0600)
			if err != nil {
				Log(ERROR, "Failed to open file %v - Error: %v", info.Path, err)
				continue
			}
			fp.Seek(offset, os.SEEK_SET)
			zeroes := make([]byte, FileSize(info.GetTimeframe(), int(info.Year), int(info.GetRecordLength()))-offset)
			fp.Write(zeroes)
			fp.Close()
		}
	}
}

func processLoad(line string) {
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
	dataShapes := make([]DataShape, 0)
	/*
		We add a couple of fake data items to the beginning - these are optionally looked for as named columns in the CSV
	*/
	dataShapes = append(dataShapes, DataShape{Name: "Epoch-date", Type: INT64})
	dataShapes = append(dataShapes, DataShape{Name: "Epoch-time", Type: INT64})
	fmt.Printf("Column Names from Data Bucket: ")
	for _, shape := range tbi.GetDataShapes() {
		fmt.Printf("%s, ", shape.Name)
		dataShapes = append(dataShapes, shape) // Use the first shape vector in the result, as they should all be the same
	}
	fmt.Printf("\n")

	/*
		Read the metadata about the CSV file
	*/
	columnIndex, csvReader, conf, err := csvreader.ReadCSVFileMetadata(dataFD, loaderCtl, dataShapes)
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

		l_start, l_end := writeCSVChunk(dbWriter, dataShapes, *tbk, columnIndex, csvChunk, conf)
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

func writeCSVChunk(dbWriter *executor.Writer, dataShapes []DataShape, dbKey TimeBucketKey, columnIndex []int, csvDataChunk [][]string, conf *csvreader.Configuration) (start, end time.Time) {
	epochCol, nanosCol := csvreader.TimeColumnsFromCSV(csvDataChunk, columnIndex, conf)
	if epochCol == nil {
		fmt.Println("Error building time columns from csv data")
		return
	}

	csmInit := NewColumnSeriesMap()
	csmInit.AddColumn(dbKey, "Epoch", epochCol)
	csm := csvreader.ColumnSeriesMapFromCSVData(csmInit, dbKey, csvDataChunk, columnIndex[2:], dataShapes)
	csmInit.AddColumn(dbKey, "Nanoseconds", nanosCol)

	dsMap := make(map[TimeBucketKey][]DataShape)
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

func parseLoadArgs(args []string) (mk *TimeBucketKey, inputFD, controlFD *os.File, err error) {
	if len(args) < 2 {
		return nil, nil, nil, errors.New("Not enough arguments, see \"\\help load\"")
	}
	mk = NewTimeBucketKey(args[0])
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

func processQueryLocal(tbk *TimeBucketKey, start, end *time.Time) (csm ColumnSeriesMap, err error) {
	query := planner.NewQuery(executor.ThisInstance.CatalogDir)
	query.AddTargetKey(tbk)

	if start == nil && end == nil {
		fmt.Println("No suitable date range supplied...")
		return
	} else if start != nil && end != nil {
		query.SetRange(start.Unix(), end.Unix())
	} else if end == nil {
		query.SetRange(start.Unix(), planner.MaxEpoch)
	}

	fmt.Printf("Query range: %v to %v\n", start, end)

	pr, err := query.Parse()
	if err != nil {
		fmt.Println("No results")
		Log(ERROR, "Parsing query: %v", err)
		return
	}

	scanner, err := executor.NewReader(pr)
	if err != nil {
		Log(ERROR, "Error return from query scanner: %v", err)
		return
	}
	csm, _, err = scanner.Read()
	if err != nil {
		Log(ERROR, "Error return from query scanner: %v", err)
		return
	}

	return csm, nil
}

func processQueryRemote(tbk *TimeBucketKey, start, end *time.Time) (csm ColumnSeriesMap, err error) {
	if end == nil {
		t := time.Unix(planner.MaxEpoch, 0)
		end = &t
	}
	epochStart := start.UTC().Unix()
	epochEnd := end.UTC().Unix()
	req := frontend.QueryRequest{
		IsSQLStatement: false,
		SQLStatement:   string(0),
		Destination:    tbk.String(),
		EpochStart:     &epochStart,
		EpochEnd:       &epochEnd,
	}
	args := &frontend.MultiQueryRequest{
		Requests: []frontend.QueryRequest{req},
	}

	cl, err := client.NewClient(baseURL)
	if err != nil {
		return nil, err
	}
	resp, err := cl.DoRPC("Query", args)
	if err != nil {
		return nil, err
	}

	return *resp.(*ColumnSeriesMap), nil
}

func processQuery(line string) {
	args := strings.Split(line, " ")
	args = args[1:]
	if !(len(args) >= 2) {
		fmt.Println("Not enough arguments, see \"\\help show\" ")
		return
	}
	toCsv = false
	tbk, start, end := parseQueryArgs(args)
	if tbk == nil {
		fmt.Println("Could not parse arguments, see \"\\help show\" ")
		return
	}

	timeStart := time.Now()
	var csm ColumnSeriesMap
	var err error
	if localMode {
		csm, err = processQueryLocal(tbk, start, end)
		if err != nil {
			fmt.Println(err)
			return
		}
	} else {
		csm, err = processQueryRemote(tbk, start, end)
		if err != nil {
			fmt.Println(err)
			return
		}
	}
	elapsedTime := time.Since(timeStart)
	/*
		Should only be one symbol / file in the result, so take the first
	*/
	if len(csm.GetMetadataKeys()) == 0 {
		fmt.Println("No results")
		return
	}
	key := csm.GetMetadataKeys()[0]
	if toCsv {
		writer := GetCSVWriter(tbk, start, end)
		printResult(line, csm[key], writer)

	} else {
		err = printResult(line, csm[key])
		if err != nil {
			fmt.Println(err.Error())
		}
	}

	fmt.Printf("Elapsed query time: %5.3f ms\n", 1000*elapsedTime.Seconds())
}

func processGapFinder(line string) {
	args := strings.Split(line, " ")
	args = args[1:]
	toCsv = false
	tbk, start, end := parseQueryArgs(args)

	query := planner.NewQuery(executor.ThisInstance.CatalogDir)
	query.AddTargetKey(tbk)

	if start != nil && end != nil {
		query.SetRange(start.Unix(), end.Unix())
	} else if end == nil {
		query.SetRange(start.Unix(), planner.MaxEpoch)
	}

	pr, err := query.Parse()
	if err != nil {
		Log(ERROR, "Parsing query: %v", err)
		os.Exit(1)
	}

	scanner, err := executor.NewReader(pr)
	if err != nil {
		Log(ERROR, "Error return from query scanner: %v", err)
		return
	}
	csm, _, err := scanner.Read()
	if err != nil {
		Log(ERROR, "Error return from query scanner: %v", err)
		return
	}

	/*
		For each of the symbols in the returned set, count the number of samples
	*/
	dataCountResults := make(map[string]int, len(csm))
	averageResult := float64(0)
	for key, cs := range csm {
		sym := key.GetItemInCategory("Symbol")
		epochs := cs.GetEpoch()
		dataCountResults[sym] = len(epochs)
		averageResult += float64(dataCountResults[sym])
	}
	averageResult /= float64(len(dataCountResults))

	fmt.Printf("The average number of records: %6.3f\n", averageResult)
	fmt.Printf("Following are the symbols that deviate from the average by more than 10 percent:\n")
	numZeros := 0
	for sym, count := range dataCountResults {
		if float64(count) <= 0.9*averageResult {
			if count == 0 {
				fmt.Printf("Sym: %s  Zero Count\n", sym)
				numZeros++
			} else {
				fmt.Printf("Sym: %s  Count: %d\n", sym, count)
			}
		}
	}

	fmt.Printf("Number of Zero data: %d\n", numZeros)
}

func parseQueryArgs(args []string) (tbk *TimeBucketKey, start, end *time.Time) {
	tbk = NewTimeBucketKey(args[0])
	if tbk == nil {
		fmt.Println("Key is not in proper format, see \"\\help show\" ")
		return
	}
	parsedTime := false
	for _, arg := range args[1:] {
		switch strings.ToLower(arg) {
		case "between":
		case "and":
		case "csv":
			toCsv = true
		default:
			if t, err := parseTime(arg); err != nil {
				Log(ERROR, "Invalid Symbol/Timeframe/recordFormat string %v", arg)
				fmt.Printf("Invalid time string %v\n", arg)
				return nil, nil, nil
			} else {
				if parsedTime {
					end = &t
				} else {
					start = &t
					parsedTime = true
				}
			}
		}
	}
	if parsedTime {
		return tbk, start, end
	} else {
		return nil, nil, nil
	}
}

func parseTime(t string) (t_out time.Time, err error) {
	/*
		Implements a variety of format choices that key on string length
	*/
	switch len(t) {
	case 0:
		return t_out, fmt.Errorf("Zero length time string")
	case 10:
		return time.Parse("2006-01-02", t)
	case 16:
		return time.Parse("2006-01-02T15:04", t)
	case 18:
		return time.Parse("20060102 150405999", t)
	default:
		return t_out, errors.New("Invalid time format")
	}
}

func printHeaderLine(cs *ColumnSeries) {
	fmt.Printf(GetFormatLine(cs, "="))
	fmt.Printf("\n")
}

func printColumnNames(colNames []string) {
	for i, name := range colNames {
		switch i {
		case 0:
			fmt.Printf("%29s  ", name)
		default:
			fmt.Printf("%-10s  ", name)
		}
	}
	fmt.Printf("\n")
}

func GetCSVWriter(tbk *TimeBucketKey, start, end *time.Time) (writer *csv.Writer) {
	var err error
	var file *os.File
	if *_OutputDir != "" && toCsv {
		if end != nil {
			file, err = os.Create(
				fmt.Sprintf("%v_%v_%v.csv",
					tbk.String(),
					start.Format("2006-01-02-15:04"),
					end.Format("2006-01-02-15:04")))
		} else {
			file, err = os.Create(
				fmt.Sprintf("%v_%v.csv", tbk.String(), *start))
			defer file.Close()
		}

		if err != nil {
			Log(ERROR, "Failed to create csv file - Error: %v", err)
			return
		}
		writer = csv.NewWriter(file)
	}
	return writer
}

func printResult(queryText string, cs *ColumnSeries, optional_writer ...*csv.Writer) (err error) {
	var writer *csv.Writer
	if len(optional_writer) != 0 {
		writer = optional_writer[0]
	}

	if cs == nil {
		fmt.Println("No results returned from query")
		return
	}
	/*
		Check if this is an EXPLAIN output
	*/
	i_explain := cs.GetByName("explain-output")
	if i_explain != nil {
		explain := i_explain.([]string)
		SQLParser.PrintExplain(queryText, explain)
		return
	}
	i_epoch := cs.GetByName("Epoch")
	if i_epoch == nil {
		return fmt.Errorf("Epoch column not present in output")
	}
	var epoch []int64
	var ok bool
	if epoch, ok = i_epoch.([]int64); !ok {
		return fmt.Errorf("Unable to convert Epoch column")
	}

	printHeaderLine(cs)
	printColumnNames(cs.GetColumnNames())
	printHeaderLine(cs)
	for i, ts := range epoch {
		row := []string{}
		var element string
		for _, name := range cs.GetColumnNames() {
			if strings.EqualFold(name, "Epoch") {
				fmt.Printf("%29s  ", ToSystemTimezone(time.Unix(ts, 0)).String()) // Epoch
				continue
			}
			col := cs.GetByName(name)
			colType := reflect.TypeOf(col).Elem().Kind()
			switch colType {
			case reflect.Float32:
				val := col.([]float32)[i]
				element = strconv.FormatFloat(float64(val), 'f', -1, 32)
			case reflect.Float64:
				val := col.([]float64)[i]
				element = strconv.FormatFloat(val, 'f', -1, 32)
			case reflect.Int32:
				val := col.([]int32)[i]
				element = strconv.FormatInt(int64(val), 10)
			case reflect.Int64:
				val := col.([]int64)[i]
				element = strconv.FormatInt(val, 10)
			case reflect.Uint8:
				val := col.([]byte)[i]
				element = strconv.FormatInt(int64(val), 10)
			}
			if writer != nil {
				row = append(row, element)
			} else {
				fmt.Printf("%-10s  ", element)
			}
		}
		fmt.Printf("\n")
		// write to csv
		if writer != nil {
			writer.Write(row)
			row = []string{}
		}
	}
	printHeaderLine(cs)
	return err
}

func GetFormatLine(cs *ColumnSeries, printChar string) (formatLine string) {
	var buffer bytes.Buffer
	appendChars := func(count int) {
		for i := 0; i < count; i++ {
			buffer.WriteString(printChar)
		}
		buffer.WriteString("  ")
	}
	for _, name := range cs.GetColumnNames() {
		if strings.EqualFold(name, "Epoch") {
			appendChars(29)
			continue
		}
		col := cs.GetByName(name)
		colType := reflect.TypeOf(col).Elem().Kind()
		switch colType {
		case reflect.Float32:
			appendChars(10)
		case reflect.Float64:
			appendChars(10)
		case reflect.Int32:
			appendChars(10)
		case reflect.Int64:
			appendChars(10)
		case reflect.Uint8:
			appendChars(10)
		}
	}
	return buffer.String()
}

func gracefulExitOnCTRLC() {
	sigChannel := make(chan os.Signal)
	go func() {
		for sig := range sigChannel {
			switch sig {
			case syscall.SIGINT:
				os.Exit(0)
			}
		}
	}()
	signal.Notify(sigChannel, syscall.SIGINT)
}

func setupCommandHistory() (rl *readline.Instance, err error) {
	usr, err := user.Current()
	if err != nil {
		fmt.Println("Unable to obtain home directory")
		os.Exit(1)
	}
	historyFile := filepath.Join(usr.HomeDir, ".marketstoreReaderHistory")
	rl, err = readline.NewEx(&readline.Config{
		Prompt:          "\033[31m»\033[0m ",
		HistoryFile:     historyFile,
		AutoComplete:    completer,
		InterruptPrompt: "^C",
		EOFPrompt:       "exit",
	})
	return rl, nil
}
