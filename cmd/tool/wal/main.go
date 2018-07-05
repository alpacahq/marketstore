package wal

import (
	"os"
	"path/filepath"

	"github.com/alpacahq/marketstore/executor"
	. "github.com/alpacahq/marketstore/utils/log"
	"github.com/alpacahq/slait/utils/log"
	"github.com/spf13/cobra"
)

const (
	walUsage        = "wal"
	walShortDesc    = "TODO: wal debugger short desc"
	walLongDesc     = "TODO: wal debugger long desc"
	walFilePathDesc = "Path to WAL File"
)

var (
	// Cmd is the wal command.
	Cmd = &cobra.Command{
		Use:     walUsage,
		Short:   walShortDesc,
		Long:    walLongDesc,
		Aliases: []string{"waldebugger"},
		Example: "TODO: wal example",
		Run:     executeWAL,
	}
	// walfilePath is the path to the walfile.
	walfilePath string
)

func init() {
	// Parse flags.
	Cmd.Flags().StringVarP(&walfilePath, "walFile", "w", "", walFilePathDesc)
	Cmd.MarkFlagRequired("walFile")
}

func executeWAL(cmd *cobra.Command, args []string) {
	log.SetLogLevel(log.INFO)

	// Read in WALFile.
	wf := &executor.WALFileType{
		FilePath: filepath.Clean(walfilePath),
	}
	filePtr, err := os.OpenFile(wf.FilePath, os.O_RDONLY, 0600)
	if err != nil {
		Log(FATAL, err.Error())
	}
	wf.FilePtr = filePtr
	wf.RootPath = filepath.Base(wf.FilePath)

	// Execute.
	wf.Replay(false)
}
