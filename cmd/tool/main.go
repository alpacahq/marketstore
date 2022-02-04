package tool

import (
	"github.com/spf13/cobra"

	"github.com/alpacahq/marketstore/v4/cmd/tool/integrity"
	"github.com/alpacahq/marketstore/v4/cmd/tool/wal"
)

const (
	usage   = "tool"
	short   = "Execute the specified tool"
	long    = "This command executes the specified tool"
	example = "marketstore tool wal --file <path>"
)

// Cmd is the tool command.
var Cmd = &cobra.Command{
	Use:        usage,
	Short:      short,
	Long:       long,
	SuggestFor: []string{"wal", "integrity"},
	Example:    example,
}

// nolint:gochecknoinits // cobra's standard way to initialize flags
func init() {
	Cmd.AddCommand(integrity.Cmd)
	Cmd.AddCommand(wal.Cmd)
}
