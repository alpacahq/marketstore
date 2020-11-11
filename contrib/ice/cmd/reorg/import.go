package reorg

import (
	"github.com/alpacahq/marketstore/v4/contrib/ice/reorg"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/spf13/cobra"
)

var reimport bool

// ImportCmd provides a command line interface for importing corporate action entries from ICE's data files
// without --reimport option it only imports unprocessed data files (those without .processed suffix)
// with --reimport specified, it reprocess every file found in <icefilesdir>. Be aware that due to the nature of how
// marketstore stores records, it will duplicate corporate action records if run on an already existing data directory.
var ImportCmd = &cobra.Command{
	Use:          "import <datadir> <icefilesdir>",
	SilenceUsage: false,
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) != 2 {
			cmd.Help()
			return nil
		}
		dataDir := args[0]
		reorgDir := args[1]
		executor.NewInstanceSetup(dataDir, true, true, true, true)
		reorg.Import(reorgDir, reimport)
		return nil
	},
}

func init() {
	ImportCmd.Flags().BoolVarP(&reimport, "reimport", "r", false, "reimport")
}
