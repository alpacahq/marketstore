package cmd

import (
	"github.com/alpacahq/marketstore/cmd/connect"
	"github.com/alpacahq/marketstore/cmd/start"
	"github.com/alpacahq/marketstore/cmd/tool"
	"github.com/alpacahq/marketstore/utils"
	. "github.com/alpacahq/marketstore/utils/log"
	"github.com/spf13/cobra"
)

var (
	// Root is the root cli command.
	Root = &cobra.Command{
		Use: "marketstore",
		Run: executeRoot,
	}
	// flagPrintVersion set flag to show current marketstore version.
	flagPrintVersion bool
)

func init() {
	Root.AddCommand(start.Cmd)
	Root.AddCommand(tool.Cmd)
	Root.AddCommand(connect.Cmd)
	Root.Flags().BoolVarP(&flagPrintVersion, "version", "v", false, "show the version info and exit")
}

// executeRoot implements the root command.
// All this does now is print version or usage.
func executeRoot(cmd *cobra.Command, args []string) {
	// Print version if specified.
	if flagPrintVersion {
		Log(INFO, "version: %+v\n", utils.Tag)
		Log(INFO, "commit hash: %+v\n", utils.GitHash)
		Log(INFO, "utc build time: %+v\n", utils.BuildStamp)
		return
	}
	// Print information regarding usage.
	cmd.Usage()
}
