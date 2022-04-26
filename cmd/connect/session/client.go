// Package session
// This file is the hub of the `session` package. The `Client` struct defined here
// manages the database connection has the responsibility of interpreting user
// inputs.
package session

import (
	"bytes"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"os/user"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/chzyer/readline"

	"github.com/alpacahq/marketstore/v4/frontend"
	"github.com/alpacahq/marketstore/v4/sqlparser"
	dbio "github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

func NewClient(ac APIClient) *Client {
	return &Client{
		apiClient: ac,
	}
}

type Client struct {
	apiClient APIClient
	// output target - if empty, output to terminal, filename to output to file
	target string
	// timing flag determines to print query execution time.
	timing bool
}

//go:generate mockgen -destination mock/client.go -package=mock github.com/alpacahq/marketstore/v4/cmd/connect/session APIClient

type APIClient interface {
	// PrintConnectInfo prints connection information to stdout.
	PrintConnectInfo()
	// Create creates a new bucket in the marketstore server
	Create(reqs *frontend.MultiCreateRequest, responses *frontend.MultiServerResponse) error
	// Write executes a write operation to the marketstore server.
	Write(reqs *frontend.MultiWriteRequest, responses *frontend.MultiServerResponse) error
	// Destroy deletes a bucket from the marketstore server.
	Destroy(reqs *frontend.MultiKeyRequest, responses *frontend.MultiServerResponse) error
	// ProcessShow returns data stored in the marketstore server.
	Show(tbk *dbio.TimeBucketKey, start, end *time.Time) (csm dbio.ColumnSeriesMap, err error)
	// GetBucketInfo returns information(datashape, timeframe, record type, etc.) for the specified buckets.
	GetBucketInfo(reqs *frontend.MultiKeyRequest, responses *frontend.MultiGetInfoResponse) error
	// SQL executes the specified sql statement
	SQL(line string) (cs *dbio.ColumnSeries, err error)
}

// RPCClient is a marketstore API client interface.
type RPCClient interface {
	DoRPC(functionName string, args interface{}) (response interface{}, err error)
}

// Read kicks off the buffer reading process.
func (c *Client) Read() error {
	// Build reader.
	r, err := newReader()
	if err != nil {
		return err
	}
	defer r.Close()

	// Print connection information.
	c.apiClient.PrintConnectInfo()
	fmt.Fprintf(os.Stderr, "Type `\\help` to see command options\n")

	// User input evaluation loop.
EVAL:
	for {
		// Read input.
		line, err := r.Readline()

		// Terminate evaluation.
		if errors.Is(err, io.EOF) {
			break EVAL
		}

		// Printed interrupt prompt.
		if errors.Is(err, readline.ErrInterrupt) {
			continue
		}

		// Print error.
		if err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
			continue
		}

		// Remove leading/trailing spaces.
		line = strings.Trim(line, " ")

		// Evaulate.
		switch {
		// Flip timing flag.
		case strings.HasPrefix(line, `\o`):
			args := strings.Split(line, " ")
			if len(args) > 1 {
				c.target = args[1]
			} else {
				c.target = ""
			}
		case strings.HasPrefix(line, `\timing`):
			c.timing = !c.timing
		case strings.HasPrefix(line, `\show`):
			c.show(line)
		case strings.HasPrefix(line, `\trim`):
			c.trim(line)
		case strings.HasPrefix(line, `\load`):
			c.load(line)
		case strings.HasPrefix(line, `\create`):
			c.create(line)
		case strings.HasPrefix(line, `\destroy`):
			c.destroy(line)
		case strings.HasPrefix(line, `\getinfo`):
			c.getinfo(line)
		case strings.HasPrefix(line, `\help`) || strings.HasPrefix(line, `\?`):
			c.functionHelp(line)
		case line == "help":
			c.functionHelp(`\help`)
		// Quit.
		case line == `\stop`, line == `\quit`, line == `\q`, line == `exit`:
			break EVAL
			// Nothing to do.
		case line == "":
			continue EVAL
		// It was a sql stmt.
		default:
			c.sql(line)
		}
	}

	return nil
}

func newReader() (*readline.Instance, error) {
	// Determine history file path.
	usr, err := user.Current()
	if err != nil {
		return nil, errors.New("unable to obtain home directory")
	}
	history := filepath.Join(usr.HomeDir, ".marketstoreReaderHistory")

	// Register commands with autocompletion.
	autoComplete := readline.NewPrefixCompleter(
		readline.PcItem(`\show`),
		readline.PcItem(`\load`),
		readline.PcItem(`\create`),
		readline.PcItem(`\getinfo`),
		readline.PcItem(`\trim`),
		readline.PcItem(`\help`),
		readline.PcItem(`\exit`),
		readline.PcItem(`\quit`),
		readline.PcItem(`\q`),
		readline.PcItem(`\?`),
		readline.PcItem(`\stop`),
	)

	// Build config.
	config := &readline.Config{
		Prompt:          "\033[31m»\033[0m ",
		HistoryFile:     history,
		AutoComplete:    autoComplete,
		InterruptPrompt: "\nInterrupt, Press Ctrl+D to exit",
		EOFPrompt:       "exit",
	}

	// return reader.
	return readline.NewEx(config)
}

func printHeaderLine(cs *dbio.ColumnSeries) {
	// nolint:forbidigo // CLI output needs fmt.Println
	fmt.Print(formatHeader(cs, "="))
	// nolint:forbidigo // CLI output needs fmt.Println
	fmt.Print("\n")
}

func printColumnNames(cs *dbio.ColumnSeries) {
	for _, name := range cs.GetColumnNames() {
		col := cs.GetByName(name)
		l := columnFormatLength(name, col)

		if strings.EqualFold(name, "Epoch") {
			// nolint:forbidigo // CLI output needs fmt.Println
			fmt.Printf("%29s  ", name)
		} else {
			// if the column name is "Ask",
			// print a string like "        Ask  "
			var sb strings.Builder
			for i := 0; i < l-len([]rune(name)); i++ {
				sb.WriteString(" ")
			}
			sb.WriteString(name)
			sb.WriteString("  ")
			// nolint:forbidigo // CLI output needs fmt.Println
			fmt.Print(sb.String())
		}
	}
	// nolint:forbidigo // CLI output needs fmt.Println
	fmt.Printf("\n")
}

func printResult(queryText string, cs *dbio.ColumnSeries, optionalFile ...string) (err error) {
	const perm755 = 0o755
	var oFile string
	if len(optionalFile) != 0 {
		// Might be a real filename
		oFile = optionalFile[0]
	}
	var writer *csv.Writer
	if oFile != "" {
		var file *os.File
		file, err = os.OpenFile(oFile, os.O_CREATE|os.O_RDWR, perm755)
		if err != nil {
			return err
		}
		defer file.Close()
		writer = csv.NewWriter(file)
	}

	if cs == nil {
		log.Info("no results returned from query")
		return nil
	}
	/*
		Check if this is an EXPLAIN output
	*/
	iExplain := cs.GetByName("explain-output")
	if iExplain != nil {
		explain, ok := iExplain.([]string)
		if !ok {
			return fmt.Errorf("format explain-output column to string(val=%v)", iExplain)
		}
		sqlparser.PrintExplain(queryText, explain)
		return nil
	}
	epoch := cs.GetEpoch()
	if epoch == nil {
		return fmt.Errorf("epoch column not present in output")
	}

	if writer == nil {
		printHeaderLine(cs)
		printColumnNames(cs)
		printHeaderLine(cs)
	}
	const (
		decimal   = 10
		bitSize32 = 32
	)
	for i, ts := range epoch {
		row := []string{}
		var element string
		for _, name := range cs.GetColumnNames() {
			if strings.EqualFold(name, "Epoch") {
				element = fmt.Sprintf("%29s", dbio.ToSystemTimezone(time.Unix(ts, 0)).String()) // Epoch
			} else {
				col := cs.GetByName(name)
				colType := reflect.TypeOf(col).Elem().Kind()
				switch colType {
				case reflect.Float32:
					val := col.([]float32)[i]
					element = strconv.FormatFloat(float64(val), 'f', -1, bitSize32)
				case reflect.Float64:
					val := col.([]float64)[i]
					element = strconv.FormatFloat(val, 'f', -1, bitSize32)
				case reflect.Int8:
					val := col.([]int8)[i]
					element = strconv.FormatInt(int64(val), decimal)
				case reflect.Int16:
					val := col.([]int16)[i]
					element = strconv.FormatInt(int64(val), decimal)
				case reflect.Int32:
					val := col.([]int32)[i]
					element = strconv.FormatInt(int64(val), decimal)
				case reflect.Int64:
					val := col.([]int64)[i]
					element = strconv.FormatInt(val, decimal)
				case reflect.Uint8:
					val := col.([]uint8)[i]
					element = strconv.FormatUint(uint64(val), decimal)
				case reflect.Uint16:
					val := col.([]uint16)[i]
					element = strconv.FormatUint(uint64(val), decimal)
				case reflect.Uint32:
					val := col.([]uint32)[i]
					element = strconv.FormatUint(uint64(val), decimal)
				case reflect.Uint64:
					val := col.([]uint64)[i]
					element = strconv.FormatUint(val, decimal)
				case reflect.Bool:
					val := col.([]bool)[i]
					if val {
						element = "TRUE"
					} else {
						element = "FALSE"
					}
				case reflect.Array: // string type (e.g. [16]rune)
					runes := reflect.ValueOf(col).Index(i)
					element = strings.Trim(runesToString(runes), "\x00") // trim space
				default:
					return fmt.Errorf("unknown column type found: %s", colType)
				}
				// print column value in the format length
				l := columnFormatLength(name, col)
				var sb strings.Builder
				for i := 0; i < l-len([]rune(element)); i++ {
					sb.WriteString(" ")
				}
				sb.WriteString(element)
				element = sb.String()
			}

			if writer != nil {
				row = append(row, strings.TrimSpace(element))
			} else {
				// nolint:forbidigo // CLI output needs fmt.Println
				fmt.Printf("%s  ", element)
			}
		}
		if writer == nil {
			// nolint:forbidigo // CLI output needs fmt.Println
			fmt.Printf("\n")
		} else {
			writer.Write(row)
		}
	}
	if writer == nil {
		printHeaderLine(cs)
		// nolint:forbidigo // CLI output needs fmt.Println
		fmt.Printf("(%d rows * %d columns)\n", len(epoch), len(cs.GetColumnNames()))
	} else {
		writer.Flush()
	}
	return err
}

// runesToString converts rune array (not slice) in reflect.Value to string.
func runesToString(runes reflect.Value) string {
	length := runes.Len()

	runeSlice := make([]rune, length)
	for i := 0; i < length; i++ {
		runeSlice[i] = rune(runes.Index(i).Int())
	}

	return string(runeSlice)
}

func columnFormatLength(colName string, col interface{}) int {
	const (
		defaultColumnLength = 10
		// = len("2021-12-21 21:00:37 +0000 UTC") = 29
		epochColumnLength = 29
	)
	if strings.EqualFold(colName, "Epoch") {
		return epochColumnLength
	}

	colType := reflect.TypeOf(col).Elem().Kind()
	switch colType {
	case reflect.Float32, reflect.Float64, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint8,
		reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.String, reflect.Bool:
		return defaultColumnLength
	case reflect.Array:
		// e.g. STRING16 column has colType=[16]rune
		return reflect.TypeOf(col).Elem().Len()
	default:
		return defaultColumnLength
	}
}

func formatHeader(cs *dbio.ColumnSeries, printChar string) string {
	var buffer bytes.Buffer
	appendChars := func(count int) {
		for i := 0; i < count; i++ {
			buffer.WriteString(printChar)
		}
		buffer.WriteString("  ")
	}
	for _, name := range cs.GetColumnNames() {
		col := cs.GetByName(name)
		appendChars(columnFormatLength(name, col))
	}
	return buffer.String()
}
