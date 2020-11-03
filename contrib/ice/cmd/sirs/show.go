package sirs

import (
	//"fmt"
	"sort"
	//"strings"
	"github.com/spf13/cobra"
	"github.com/alpacahq/marketstore/v4/utils/log"
	"github.com/alpacahq/marketstore/v4/contrib/ice/sirs"
)


var (
	verbose bool
)

//FileCmd load security master from a file
var ShowSecurityMasterCmd = &cobra.Command{
	Use:   "sirs <file-name>",
	Short: "load security master from a file",
	RunE: func(cmd *cobra.Command, args []string) error {
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
			rev := map[string]string{}
			symbols := make([]string, 0)
			for cusip, symbol := range master {
				if symbol != "" {
					rev[symbol] = cusip
					symbols = append(symbols, symbol)
				}
			}
			sort.Strings(symbols)
			for _, s := range symbols {
				println(s, rev[s])
			}
			return err
		} else {
			cmd.Help()
		}
		return nil
	},
}

func init() {
	ShowSecurityMasterCmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "verbosity")
}
