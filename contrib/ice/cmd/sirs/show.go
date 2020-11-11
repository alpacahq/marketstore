package sirs

import (
	"sort"

	"github.com/alpacahq/marketstore/v4/contrib/ice/sirs"
	"github.com/alpacahq/marketstore/v4/utils/log"
	"github.com/spf13/cobra"
)

// ShowSecurityMasterCmd provides a command line option to view Symbol -> CUSIP mappings for a given SIRS file
// since SIRS files in most cases contains incremental updates since the last Friday,
// this function displays not only the contents of a signle file, but the cumulated changes from last snapshot
var ShowSecurityMasterCmd = &cobra.Command{
	Use:   "sirs <file-name>",
	Short: "load security master from a file",
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) != 1 {
			cmd.Help()
			return nil
		}
		//open file
		if len(args) == 1 {
			fileName := args[0]
			log.Info("loading security master", "file", fileName)
			sirsFiles, err := sirs.CollectSirsFilesFor(fileName)
			if err != nil {
				return err
			}
			master, err := sirs.BuildSecurityMasterMap(sirsFiles)
			if err != nil {
				return err
			}
			cusips := map[string]string{}
			symbols := make([]string, 0)
			for cusip, symbol := range master {
				if symbol != "" {
					cusips[symbol] = cusip
					symbols = append(symbols, symbol)
				}
			}
			sort.Strings(symbols)
			for _, symbol := range symbols {
				println(symbol, cusips[symbol])
			}
		}
		return nil
	},
}
