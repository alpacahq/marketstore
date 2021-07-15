package sirs

import (
	"fmt"
	"path/filepath"
	"sort"

	"github.com/spf13/cobra"

	"github.com/alpacahq/marketstore/v4/contrib/ice/sirs"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

// ShowSecurityMasterCmd provides a command line option to view Symbol -> CUSIP mappings for a given SIRS file
// ICE provides a snapshot of securities information on each Friday and incremental updates relative to this snapshot
// from Monday till Thursday. Snapshots are named 'sirs.refresh.*' while updates are simply come in the form of 'sirs.*'
// This function shows the actual state of cusip -> symbol mapping at the given file - it displays information
// accumulated from the last snapshot and including the current file.
var ShowSecurityMasterCmd = &cobra.Command{
	Use:   "show <file-name>",
	Short: "show cusip -> symbol mapping of a given sirs file",
	Long: `This command shows the actual state of cusip -> symbol mapping at the given file - it displays information
	accumulated from the last snapshot and including the current file`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) != 1 {
			cmd.Help()
			return nil
		}
		fileName := args[0]
		basePath := filepath.Dir(fileName)
		dateStr := filepath.Ext(fileName)[1:]
		log.Info("loading security master file: %s", dateStr)
		sirsFiles, err := sirs.CollectSirsFiles(basePath, dateStr)
		if err != nil {
			return err
		}
		master, err := sirs.BuildSecurityMasterMap(sirsFiles)
		if err != nil {
			return err
		}
		cusips := map[string]string{}
		symbols := make([]string, 0, len(master))
		for cusip, symbol := range master {
			if symbol != "" {
				cusips[symbol] = cusip
				symbols = append(symbols, symbol)
			}
		}
		sort.Strings(symbols)
		for _, symbol := range symbols {
			fmt.Println(symbol, cusips[symbol])
		}
		return nil
	},
}
