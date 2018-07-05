package connect

import (
	"errors"

	"github.com/alpacahq/marketstore/cmd/connect/cli"
	. "github.com/alpacahq/marketstore/utils/log"
	"github.com/spf13/cobra"
)

const (
	// Command
	// -------------
	connectUsage     = "connect"
	connectShortDesc = "Opens a client for reading/writing values to a marketstore database"
	connectLongDesc  = "This command opens a client connection to a marketstore instance.. Lorem ipsum."
	connectExample   = "marketstore connect -serverURL example.com:5993"

	// Flags.
	// -------------
	// Remote URL.
	urlFlag    = "url"
	defaultURL = ""
	urlDesc    = "Network address to database instance at \"hostname:port\" when used in remote mode."
	// Local directory.
	dirFlag    = "dir"
	defaultDir = ""
	dirDesc    = "Filesystem path of database files when used in local mode"
)

var (
	// Cmd is the connect command.
	Cmd = &cobra.Command{
		Use:        connectUsage,
		Short:      connectShortDesc,
		Long:       connectLongDesc,
		SuggestFor: []string{"mkts", "open", "conn"},
		Example:    connectExample,
		Args:       validateArgs,
		Run:        executeConnect,
	}

	// url set via flag for remote db address.
	url string
	// dir set via flag for local directory location.
	dir string
)

func init() {
	Cmd.Flags().StringVar(&url, urlFlag, defaultURL, urlDesc)
	Cmd.Flags().StringVar(&dir, dirFlag, defaultDir, dirDesc)
}

// validateArgs returns an error that prevents cmd execution if
// the custom validation fails.
func validateArgs(cmd *cobra.Command, args []string) error {
	if len(dir) == 0 && len(url) == 0 {
		return errors.New("cannot connect to database, use a flag to set location")
	}
	return nil
}

// executeConnect implements the connect command.
func executeConnect(cmd *cobra.Command, args []string) {
	//
	var c *cli.Client
	var err error

	// Attempt local mode.
	if len(dir) != 0 {
		c, err = cli.NewLocalClient(dir)
		if err != nil {
			Log(INFO, err.Error())
			return
		}
	}

	// Attempt remote mode.
	if len(url) != 0 {
		c, err = cli.NewRemoteClient(url)
		if err != nil {
			Log(INFO, err.Error())
			return
		}
	}

	// Initialize connection.
	err = c.Connect()
	if err != nil {
		Log(INFO, err.Error())
		return
	}

	//	Start reader buffer.
	err = c.Read()
	if err != nil {
		Log(INFO, err.Error())
		return
	}

	// TODO: Gracefully close connections and clean up.
	Log(INFO, "closed connection")
}
