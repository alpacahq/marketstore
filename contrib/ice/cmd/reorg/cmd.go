package reorg

import (
	"github.com/spf13/cobra"
)

//Cmd implements the main ice command
var Cmd = &cobra.Command{
	Use:          "reorg",
	Short:		  "functions for handling ICE's reorg files",
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		return cmd.Help()
	},
}


func init() {
	Cmd.AddCommand(ShowRecordsCmd)
	Cmd.AddCommand(ImportCmd)
}
