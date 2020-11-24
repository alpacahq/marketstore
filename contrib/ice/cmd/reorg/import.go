package reorg

import (
	"github.com/alpacahq/marketstore/v4/contrib/ice/reorg"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/spf13/cobra"
)

var reimport bool
var storeWithoutSymbols bool

// ImportCmd provides a command line interface for importing corporate action entries from ICE's data files
// without --reimport option it only imports unprocessed data files (those without .processed suffix)
// with --reimport specified, it reprocess every file found in <icefilesdir>. Be aware that due to the nature of how
// marketstore stores records, it will duplicate corporate action announcements if run on an already populated data directory.
var ImportCmd = &cobra.Command{
	Use:   "import <datadir> <icefilesdir>",
	Short: "Import corporate actions announcements ",
	Long: `This command is used for importing corporate action entries from ICE's rerog files
	<datadir> must point to Marketstore's data directory
	<icefilesdir> must contain ICE's reorg.* and sirs.*/sirs.refresh.* files
	Each successfully imported rerog file will be renamed to reorg.*.processed to avoid reimporting it later
	
	By default, without --reimport option it only imports unprocessed data files (those without .processed suffix)
	With --reimport specified, it reprocess every file reorg.* file found in <icefilesdir>.  Be aware that 
	due to the nature of how marketstore stores records, it will duplicate corporate action announcements 
	if run on already existing import

	--fallback-to-cusip allows Marketstore to store corporate action records by their TargetCusipID if a matching symbol is 
	not found. Default is false, so only records with matching symbols are stored 
	`,
	SilenceUsage: false,
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) != 2 {
			cmd.Help()
			return nil
		}
		dataDir := args[0]
		reorgDir := args[1]
		executor.NewInstanceSetup(dataDir, nil, true, true, true, true)
		reorg.Import(reorgDir, reimport, storeWithoutSymbols)
		return nil
	},
}

func init() {
	ImportCmd.Flags().BoolVarP(&reimport, "reimport", "r", false, "reimport")
	ImportCmd.Flags().BoolVarP(&storeWithoutSymbols, "fallback-to-cusip", "c", false, "fallback-to-cusip")
}
