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
	// printExecutionTime flag determines to print query execution time.
	printExecutionTime bool
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

func commandMap(c *Client) map[string]func(line string) error {
	return map[string]func(line string) error{
		`\o`:       c.setOutputTarget,
		`\timing`:  c.flipPrintTimeFlag,
		`\show`:    c.show,
		`\trim`:    c.trim,
		`\load`:    c.load,
		`\create`:  c.create,
		`\destroy`: c.destroy,
		`\getinfo`: c.getinfo,
		`help`:     c.functionHelp,
		`\help`:    c.functionHelp,
		`\?`:       c.functionHelp,
	}
}

func (c *Client) setOutputTarget(line string) error {
	args := strings.Split(line, " ")
	if len(args) > 1 {
		c.target = args[1]
	} else {
		c.target = ""
	}
	return nil
}

func (c *Client) flipPrintTimeFlag(_ string) error {
	c.printExecutionTime = !c.printExecutionTime
	return nil
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
	_, _ = fmt.Fprintf(os.Stderr, "Type `\\help` to see command options\n")

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
			_, _ = fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
			continue
		}

		// Remove leading/trailing spaces.
		line = strings.Trim(line, " ")

		// ----- Evaluate -----

		// Quit.
		if line == `\stop` || line == `\quit` || line == `\q` || line == `exit` {
			break EVAL
		}
		// Nothing to do.
		if line == "" {
			continue EVAL
		}

		for prefix, cmdFunc := range commandMap(c) {
			if strings.HasPrefix(line, prefix) {
				if err2 := cmdFunc(line); err2 != nil {
					_, _ = fmt.Fprintf(os.Stderr, "error: %s", err2.Error())
				}
				continue EVAL
			}
		}

		// No prefix matched, then it's a sql stmt.
		c.sql(line)
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
		col := cs.GetColumn(name)
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
	iExplain := cs.GetColumn("explain-output")
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
		var (
			row     []string
			element string
		)
		for _, name := range cs.GetColumnNames() {
			if strings.EqualFold(name, "Epoch") {
				element = fmt.Sprintf("%29s", dbio.ToSystemTimezone(time.Unix(ts, 0)).String()) // Epoch
			} else {
				icol := cs.GetColumn(name)
				// colType := reflect.TypeOf(icol).Elem().Kind()
				switch col := icol.(type) {
				case []float32:
					element = strconv.FormatFloat(float64(col[i]), 'f', -1, bitSize32)
				case []float64:
					element = strconv.FormatFloat(col[i], 'f', -1, bitSize32)
				case []int8:
					element = strconv.FormatInt(int64(col[i]), decimal)
				case []int16:
					element = strconv.FormatInt(int64(col[i]), decimal)
				case []int32:
					element = strconv.FormatInt(int64(col[i]), decimal)
				case []int64:
					element = strconv.FormatInt(col[i], decimal)
				case []uint8:
					element = strconv.FormatUint(uint64(col[i]), decimal)
				case []uint16:
					element = strconv.FormatUint(uint64(col[i]), decimal)
				case []uint32:
					element = strconv.FormatUint(uint64(col[i]), decimal)
				case []uint64:
					element = strconv.FormatUint(col[i], decimal)
				case []bool:
					val := col[i]
					if val {
						element = "TRUE"
					} else {
						element = "FALSE"
					}
				case [][16]rune:
					runes := reflect.ValueOf(icol).Index(i)
					element = strings.Trim(runesToString(runes), "\x00") // trim space
				default:
					return fmt.Errorf("unknown type of column found: col=%v", icol)
				}
				// print column value in the format length
				l := columnFormatLength(name, icol)
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
			if err2 := writer.Write(row); err2 != nil {
				return fmt.Errorf("failed to print row: %w", err2)
			}
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
		col := cs.GetColumn(name)
		appendChars(columnFormatLength(name, col))
	}
	return buffer.String()
}
