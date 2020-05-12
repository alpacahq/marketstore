package wal

import (
	"os"
	"path/filepath"

	"github.com/alpacahq/marketstore/executor"
	"github.com/alpacahq/marketstore/utils/log"
	"github.com/spf13/cobra"
)

const (
	usage           = "wal"
	short           = "Examine a WAL file's unwritten transactions"
	long            = "This command examines a WAL file's unwritten transactions"
	example         = "marketstore tool wal --file <path>"
	walFilePathDesc = "set the path to the WAL file"
)

var (
	// Cmd is the wal command.
	Cmd = &cobra.Command{
		Use:     usage,
		Short:   short,
		Long:    long,
		Aliases: []string{"waldebugger"},
		Example: example,
		RunE:    executeWAL,
	}
	// walfilePath is the path to the walfile.
	walfilePath string
)

func init() {
	// Parse flags.
	Cmd.Flags().StringVarP(&walfilePath, "file", "f", "", walFilePathDesc)
	Cmd.MarkFlagRequired("file")
}

func executeWAL(cmd *cobra.Command, args []string) error {
	log.SetLevel(log.INFO)

	// Read in WALFile.
	wf := &executor.WALFileType{
		FilePath: filepath.Clean(walfilePath),
	}
	filePtr, err := os.OpenFile(wf.FilePath, os.O_RDONLY, 0600)
	if err != nil {
		return err
	}
	wf.FilePtr = filePtr
	wf.RootPath = filepath.Base(wf.FilePath)

	// Execute.
	return wf.Replay(false)
}
