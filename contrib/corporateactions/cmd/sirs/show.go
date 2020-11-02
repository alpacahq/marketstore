package sirs

import (
	//"fmt"
	"sort"
	//"strings"
	"github.com/spf13/cobra"
	"github.com/alpacahq/marketstore/v4/utils/log"
	"github.com/alpacahq/marketstore/v4/contrib/corporateactions/sirs"
)


var (
	verbose bool
)

//FileCmd load security master from a file
var ShowSecurityMasterCmd = &cobra.Command{
	Use:   "securitymaster <file-name>",
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
			symbols := make([]string, 0)
			for k, s := range master {
				println(k, s)
				if s != "" {
					symbols = append(symbols, s)
				}
			}

			//sort.(symbols, func(i, j int) bool { return symbols[i] > symbols[j]})
			sort.Strings(symbols)
			for _, s := range symbols {
				println(s)
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
