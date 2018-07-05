package tool

import (
	"github.com/alpacahq/marketstore/cmd/tool/integrity"
	"github.com/alpacahq/marketstore/cmd/tool/wal"
	"github.com/spf13/cobra"
)

const (
	toolUsage     = "tool"
	toolShortDesc = "Executes tools as subcommands"
	toolLongDesc  = "This command executes the specified tool. Lorem ipsum.."
	toolExample   = "marketstore tool wal [flags]"
)

var (
	// Cmd is the tool command.
	Cmd = &cobra.Command{
		Use:        toolUsage,
		Short:      toolShortDesc,
		Long:       toolLongDesc,
		Aliases:    []string{"s"},
		SuggestFor: []string{"wal", "integrity"},
		Example:    toolExample,
	}
)

func init() {
	Cmd.AddCommand(integrity.Cmd)
	Cmd.AddCommand(wal.Cmd)
}
