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

	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/frontend/client"
	"github.com/alpacahq/marketstore/v4/sqlparser"
	dbio "github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/chzyer/readline"
)

// mode is the client connection mode.
type mode int

const (
	local mode = iota
	remote
)

// Client represents an agent that manages a database
// connection and parses/executes the statements specified by a
// user in a command-line buffer.
type Client struct {
	// timing flag determines to print query execution time.
	timing bool
	// output target - if empty, output to terminal, filename to output to file
	target string
	// mode determines local or remote.
	mode mode
	// url is the optional address of a db instance on a different machine.
	url string
	// rc is the optional remote client.
	rc RPCClient
	// dir is the optional filesystem location of a local db instance.
	dir string
	// disableVariableCompression is an option if the compression is used to read & write data
	disableVariableCompression bool
	// enableLastKnown is an optimization to reduce the size of dara reading for query
	enableLastKnown bool
}

// RPCClient is a marketstore API client interface.
type RPCClient interface {
	DoRPC(functionName string, args interface{}) (response interface{}, err error)
}

// NewLocalClient builds a new client struct in local mode.
func NewLocalClient(dir string, disableVariableCompression bool) (c *Client, err error) {
	// Configure db settings.
	initCatalog, initWALCache, backgroundSync, WALBypass := true, true, false, true
	walRotateInterval := 5
	executor.NewInstanceSetup(dir,
		nil, walRotateInterval, initCatalog, initWALCache, backgroundSync, WALBypass,
	)
	return &Client{dir: dir, mode: local, disableVariableCompression: disableVariableCompression}, nil
}

// NewRemoteClient generates a new client struct.
func NewRemoteClient(url string, disableVariableCompression bool) (c *Client, err error) {
	// TODO: validate url using go core packages.
	splits := strings.Split(url, ":")
	if len(splits) != 2 {
		msg := fmt.Sprintf("incorrect URL, need \"hostname:port\", have: %s\n", url)
		return nil, errors.New(msg)
	}
	// build url.
	url = "http://" + url
	return &Client{url: url, mode: remote, disableVariableCompression: disableVariableCompression}, nil
}

// Connect initializes a client connection.
func (c *Client) Connect() error {
	if c.mode == local {
		// Nothing to do here yet..
		return nil
	}

	// Attempt connection to remote host.
	client, err := client.NewClient(c.url)
	if err != nil {
		return err
	}
	c.rc = client

	// Success.
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
	if c.mode == local {
		fmt.Fprintf(os.Stderr, "Connected to local instance at path: %v\n", c.dir)
	} else {
		fmt.Fprintf(os.Stderr, "Connected to remote instance at: %v\n", c.url)
	}
	fmt.Fprintf(os.Stderr, "Type `\\help` to see command options\n")

	// User input evaluation loop.
EVAL:
	for {
		// Read input.
		line, err := r.Readline()

		// Terminate evaluation.
		if err == io.EOF {
			break EVAL
		}

		// Printed interrupt prompt.
		if err == readline.ErrInterrupt {
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
	fmt.Printf(formatHeader(cs, "="))
	fmt.Printf("\n")
}
func printColumnNames(cs *dbio.ColumnSeries) {
	for _, name := range cs.GetColumnNames() {
		col := cs.GetByName(name)
		l := columnFormatLength(name, col)

		if strings.EqualFold(name, "Epoch") {
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
			fmt.Print(sb.String())
		}
	}
	fmt.Printf("\n")
}

//func printResult(queryText string, cs *dbio.ColumnSeries, optional_writer ...*csv.Writer) (err error) {
func printResult(queryText string, cs *dbio.ColumnSeries, optionalFile ...string) (err error) {

	var oFile string
	if len(optionalFile) != 0 {
		// Might be a real filename
		oFile = optionalFile[0]
	}
	var writer *csv.Writer
	if len(oFile) != 0 {
		var file *os.File
		file, err = os.OpenFile(oFile, os.O_CREATE|os.O_RDWR, 0755)
		if err != nil {
			return err
		}
		defer file.Close()
		writer = csv.NewWriter(file)
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
		sqlparser.PrintExplain(queryText, explain)
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

	if writer == nil {
		printHeaderLine(cs)
		printColumnNames(cs)
		printHeaderLine(cs)
	}
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
					element = strconv.FormatFloat(float64(val), 'f', -1, 32)
				case reflect.Float64:
					val := col.([]float64)[i]
					element = strconv.FormatFloat(val, 'f', -1, 32)
				case reflect.Int8:
					val := col.([]int8)[i]
					element = strconv.FormatInt(int64(val), 10)
				case reflect.Int16:
					val := col.([]int16)[i]
					element = strconv.FormatInt(int64(val), 10)
				case reflect.Int32:
					val := col.([]int32)[i]
					element = strconv.FormatInt(int64(val), 10)
				case reflect.Int64:
					val := col.([]int64)[i]
					element = strconv.FormatInt(val, 10)
				case reflect.Uint8:
					val := col.([]uint8)[i]
					element = strconv.FormatUint(uint64(val), 10)
				case reflect.Uint16:
					val := col.([]uint16)[i]
					element = strconv.FormatUint(uint64(val), 10)
				case reflect.Uint32:
					val := col.([]uint32)[i]
					element = strconv.FormatUint(uint64(val), 10)
				case reflect.Uint64:
					val := col.([]uint64)[i]
					element = strconv.FormatUint(val, 10)
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
				fmt.Printf("%s  ", element)
			}
		}
		if writer == nil {
			fmt.Printf("\n")
		} else {
			writer.Write(row)
			row = []string{}
		}
	}
	if writer == nil {
		printHeaderLine(cs)
	} else {
		writer.Flush()
	}
	return err
}

// runesToString converts rune array (not slice) in reflect.Value to string
func runesToString(runes reflect.Value) string {
	length := runes.Len()

	runeSlice := make([]rune, length)
	for i := 0; i < length; i++ {
		runeSlice[i] = rune(runes.Index(i).Int())
	}

	return string(runeSlice)
}

func columnFormatLength(colName string, col interface{}) int {
	if strings.EqualFold(colName, "Epoch") {
		return 29
	}

	colType := reflect.TypeOf(col).Elem().Kind()
	switch colType {
	case reflect.Float32:
		fallthrough
	case reflect.Float64:
		fallthrough
	case reflect.Int8:
		fallthrough
	case reflect.Int16:
		fallthrough
	case reflect.Int32:
		fallthrough
	case reflect.Int64:
		fallthrough
	case reflect.Uint8:
		fallthrough
	case reflect.Uint16:
		fallthrough
	case reflect.Uint32:
		fallthrough
	case reflect.Uint64:
		fallthrough
	case reflect.String:
		fallthrough
	case reflect.Bool:
		return 10
	case reflect.Array:
		// e.g. STRING16 column has colType=[16]rune
		return reflect.TypeOf(col).Elem().Len()
	}
	// default
	return 10
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
