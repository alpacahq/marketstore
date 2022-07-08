package reorg

import (
	"fmt"

	"github.com/alpacahq/marketstore/v4/internal/di"

	"github.com/spf13/cobra"

	"github.com/alpacahq/marketstore/v4/contrib/ice/reorg"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/utils"
)

var (
	reimport            bool
	storeWithoutSymbols bool
	disableVarComp      bool
)

// ImportCmd provides a command line interface for importing corporate action entries from ICE's data files
// without --reimport option it only imports unprocessed data files (those without .processed suffix)
// with --reimport specified, it reprocess every file found in <icefilesdir>. Be aware that due to the nature of how
// marketstore stores records,
// it will duplicate corporate action announcements if run on an already populated data directory.
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

	--fallback-to-cusip allows Marketstore to store corporate action records by their TargetCusipID 
    if a matching symbol is not found. Default is false, so only records with matching symbols are stored 
	`,
	SilenceUsage: false,
	RunE: func(cmd *cobra.Command, args []string) error {
		// usage: import <datadir> <icefilesdir>
		const argLen = 2
		if len(args) != argLen {
			_ = cmd.Help()
			return nil
		}
		dataDir := args[0]
		reorgDir := args[1]
		// walfile is rotated every walRotateInterval * primaryDiskRefreshInterval(= default:5min)
		cfg := utils.NewDefaultConfig(dataDir)
		cfg.WALBypass = true
		c := di.NewContainer(cfg)
		executor.NewInstanceSetup(c.GetCatalogDir(), c.GetInitWALFile())

		utils.InstanceConfig.DisableVariableCompression = disableVarComp
		err := reorg.Import(reorgDir, reimport, storeWithoutSymbols)
		if err != nil {
			return fmt.Errorf("failed to import: %w", err)
		}
		return nil
	},
}

// nolint:gochecknoinits // cobra's standard way to initialize flags
func init() {
	ImportCmd.Flags().BoolVarP(&reimport, "reimport", "r", false, "reimport")
	ImportCmd.Flags().BoolVarP(&storeWithoutSymbols, "fallback-to-cusip", "c", false, "fallback-to-cusip")
	// Please set the same value as disable_variable_compression in the marketstore's mkts.yml
	// where this plugin is running.
	// Different disable_variable_compression values between this plugin and mkts.yml
	// causes unexpected data inconsistency issue.
	// We need some refactor to prevent it from occurring due to this manual setting.
	ImportCmd.Flags().BoolVarP(&disableVarComp, "disable-variable-compression", "d", false,
		"disable variable compression feature",
	)
}
