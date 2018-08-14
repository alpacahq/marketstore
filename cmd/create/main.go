// Package create - because packages cannot be named 'init' in go.
package create

import (
	"io/ioutil"
	"os"

	"github.com/spf13/cobra"
)

const (
	usage   = "init"
	short   = "Creates a new mkts.yml file"
	long    = "This command creates a new mkts.yml file in the current directory"
	example = "marketstore init"
)

var (
	// Cmd is the init command.
	Cmd = &cobra.Command{
		Use:        usage,
		Short:      short,
		Long:       long,
		SuggestFor: []string{"create", "new"},
		Example:    example,
		RunE:       executeInit,
	}
)

// executeInit implements the init command.
func executeInit(*cobra.Command, []string) error {
	// serialize default file.
	data, err := Asset("cmd/create/default.yml")
	if err != nil {
		return err
	}
	// write to current directory.
	err = ioutil.WriteFile("mkts.yml", data, 0644)
	if err != nil {
		return err
	}
	// create a new directory to store data.
	return os.Mkdir("data", 0700)
}
