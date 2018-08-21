package tool

import (
	"github.com/alpacahq/marketstore/cmd/tool/integrity"
	"github.com/alpacahq/marketstore/cmd/tool/wal"
	"github.com/spf13/cobra"
)

const (
	usage   = "tool"
	short   = "Execute the specified tool"
	long    = "This command executes the specified tool"
	example = "marketstore tool wal --file <path>"
)

var (
	// Cmd is the tool command.
	Cmd = &cobra.Command{
		Use:        usage,
		Short:      short,
		Long:       long,
		SuggestFor: []string{"wal", "integrity"},
		Example:    example,
	}
)

func init() {
	Cmd.AddCommand(integrity.Cmd)
	Cmd.AddCommand(wal.Cmd)
}
