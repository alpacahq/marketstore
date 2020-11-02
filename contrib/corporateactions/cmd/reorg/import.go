package reorg

import (
	"github.com/spf13/cobra"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/contrib/corporateactions/reorg"
)

var reimport bool

var ImportCmd = &cobra.Command{
	Use: "import <datadir> <reorgdir>",
	SilenceUsage: false,
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) == 2 {
			dataDir := args[0]
			reorgDir := args[1]
			executor.NewInstanceSetup(dataDir, true, true, true, true)
			reorg.Import(reorgDir, reimport)
		} else {
			cmd.Help()
		}
		return nil
	},
}

func init() {
	ImportCmd.Flags().BoolVarP(&reimport, "reimport", "r", false, "reimport")
}
