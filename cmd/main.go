package cmd

import (
	"github.com/alpacahq/marketstore/v4/utils/log"

	"github.com/spf13/cobra"

	"github.com/alpacahq/marketstore/v4/cmd/connect"
	"github.com/alpacahq/marketstore/v4/cmd/create"
	"github.com/alpacahq/marketstore/v4/cmd/estimate"
	"github.com/alpacahq/marketstore/v4/cmd/start"
	"github.com/alpacahq/marketstore/v4/cmd/tool"
	"github.com/alpacahq/marketstore/v4/utils"
)

// flagPrintVersion set flag to show current marketstore version.
var flagPrintVersion bool

// Execute builds the command tree and executes commands.
func Execute() error {
	// c is the root command.
	c := &cobra.Command{
		Use: "marketstore",
		RunE: func(cmd *cobra.Command, args []string) error {
			// Print version if specified.
			if flagPrintVersion {
				log.Info("version: %+v\n", utils.Tag)
				log.Info("commit hash: %+v\n", utils.GitHash)
				log.Info("utc build time: %+v\n", utils.BuildStamp)
				return nil
			}
			// Print information regarding usage.
			return cmd.Usage()
		},
	}

	// Adds subcommands and version flag.
	c.AddCommand(create.Cmd)
	c.AddCommand(estimate.Cmd)
	c.AddCommand(start.Cmd)
	c.AddCommand(tool.Cmd)
	c.AddCommand(connect.Cmd)
	c.Flags().BoolVarP(&flagPrintVersion, "version", "v", false, "show the version info and exit")

	return c.Execute()
}
