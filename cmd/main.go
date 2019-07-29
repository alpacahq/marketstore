package cmd

import (
	"fmt"

	"github.com/alpacahq/marketstore/cmd/connect"
	"github.com/alpacahq/marketstore/cmd/create"
	"github.com/alpacahq/marketstore/cmd/estimate"
	"github.com/alpacahq/marketstore/cmd/start"
	"github.com/alpacahq/marketstore/cmd/tool"
	"github.com/alpacahq/marketstore/utils"
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
				fmt.Printf("version: %+v\n", utils.Tag)
				fmt.Printf("commit hash: %+v\n", utils.GitHash)
				fmt.Printf("utc build time: %+v\n", utils.BuildStamp)
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
