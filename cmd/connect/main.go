package connect

import (
	"errors"
	"github.com/alpacahq/marketstore/utils"

	"github.com/alpacahq/marketstore/cmd/connect/session"
	"github.com/alpacahq/marketstore/utils/log"
	"github.com/spf13/cobra"
)

const (
	// Command
	// -------------
	usage   = "connect"
	short   = "Open an interactive session with an existing marketstore database"
	long    = "This command opens an interactive session with an existing marketstore database"
	example = "marketstore connect --url <address>"

	// Flags.
	// -------------
	// Network Address.
	urlFlag    = "url"
	defaultURL = ""
	urlDesc    = "network address to database instance at \"hostname:port\" when used in remote mode"
	// Local directory.
	dirFlag    = "dir"
	defaultDir = ""
	dirDesc    = "filesystem path of the directory containing database files when used in local mode"
)

var (
	// Cmd is the connect command.
	Cmd = &cobra.Command{
		Use:        usage,
		Short:      short,
		Long:       long,
		SuggestFor: []string{"mkts", "open", "conn"},
		Example:    example,
		Args:       validateArgs,
		RunE:       executeConnect,
	}

	// url set via flag for remote db address.
	url string
	// dir set via flag for local directory location.
	dir string
	// turns compression of variable data off
	varCompOff bool
)

func init() {
	Cmd.Flags().StringVarP(&url, urlFlag, "u", defaultURL, urlDesc)
	Cmd.Flags().StringVarP(&dir, dirFlag, "d", defaultDir, dirDesc)
	Cmd.Flags().BoolVarP(&varCompOff, "disable variable compression", "c", false, "c")
	if varCompOff {
		utils.InstanceConfig.DisableVariableCompression = true
	}
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
func executeConnect(cmd *cobra.Command, args []string) error {

	var c *session.Client
	var err error

	// Attempt local mode.
	if len(dir) != 0 {
		c, err = session.NewLocalClient(dir)
		if err != nil {
			return err
		}
	}

	// Attempt remote mode.
	if len(url) != 0 {
		c, err = session.NewRemoteClient(url)
		if err != nil {
			return err
		}
	}

	// Initialize connection.
	err = c.Connect()
	if err != nil {
		return err
	}

	// Enter command loop
	err = c.Read()
	if err != nil {
		return err
	}

	log.Info("closed connection")
	return nil
}
