package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/alpacahq/marketstore/executor"
	"github.com/alpacahq/marketstore/utils/log"
)

var wf executor.WALFileType

func init() {
	flag.Lookup("logtostderr").Value.Set("true")
	log.SetLogLevel(log.INFO)

	walfile := flag.String("walFile", "", "Path to WAL File")
	flag.Parse()
	if *walfile == "" {
		fmt.Println("Must supply a value for walFile...")
		os.Exit(1)
	}
	wf.FilePath = filepath.Clean(*walfile)
	var err error
	wf.FilePtr, err = os.OpenFile(wf.FilePath, os.O_RDONLY, 0600)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
	wf.RootPath = filepath.Base(wf.FilePath)
}
func main() {
	wf.Replay(false)
}
