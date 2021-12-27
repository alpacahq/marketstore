package cmd

import (
	"github.com/spf13/cobra"

	"github.com/alpacahq/marketstore/v4/contrib/ice/cmd/ftp"
	"github.com/alpacahq/marketstore/v4/contrib/ice/cmd/reorg"
	"github.com/alpacahq/marketstore/v4/contrib/ice/cmd/sirs"
)

//Cmd implements the main ice command.
var Cmd = &cobra.Command{
	Use:          "ice",
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		return cmd.Help()
	},
}

func init() {
	Cmd.AddCommand(sirs.ShowSecurityMasterCmd)
	Cmd.AddCommand(reorg.Cmd)
	Cmd.AddCommand(ftp.FTPSyncCmd)
}
