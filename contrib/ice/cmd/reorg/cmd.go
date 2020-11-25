package reorg

import (
	"github.com/spf13/cobra"
)

// Cmd is the parent for other Reorg related commands
var Cmd = &cobra.Command{
	Use:          "reorg",
	Short:        "Functions for handling ICE's reorg files",
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		return cmd.Help()
	},
}

func init() {
	Cmd.AddCommand(ShowRecordsCmd)
	Cmd.AddCommand(ImportCmd)
}
