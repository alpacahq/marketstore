package wal

import (
	"os"
	"path/filepath"

	"github.com/spf13/cobra"

	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/utils/log"
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
	const ownerReadWrite = 0o600
	log.SetLevel(log.INFO)

	wfPath := filepath.Clean(walfilePath)
	// Read in WALFile.
	wf := &executor.WALFileType{}
	filePtr, err := os.OpenFile(wfPath, os.O_RDONLY, ownerReadWrite)
	if err != nil {
		return err
	}
	wf.FilePtr = filePtr

	// Execute.
	return wf.Replay(true)
}
