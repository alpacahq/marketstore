package cmd

import (
	"github.com/alpacahq/marketstore/cmd/connect"
	"github.com/alpacahq/marketstore/cmd/start"
	"github.com/alpacahq/marketstore/cmd/tool"
	"github.com/alpacahq/marketstore/utils"
	. "github.com/alpacahq/marketstore/utils/log"
	"github.com/spf13/cobra"
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
				Log(INFO, "version: %+v\n", utils.Tag)
				Log(INFO, "commit hash: %+v\n", utils.GitHash)
				Log(INFO, "utc build time: %+v\n", utils.BuildStamp)
				return nil
			}
			// Print information regarding usage.
			return cmd.Usage()
		},
	}

	// Adds subcommands and version flag.
	c.AddCommand(start.Cmd)
	c.AddCommand(tool.Cmd)
	c.AddCommand(connect.Cmd)
	c.Flags().BoolVarP(&flagPrintVersion, "version", "v", false, "show the version info and exit")

	return c.Execute()
}
