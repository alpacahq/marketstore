// Package create - because packages cannot be named 'init' in go.
//go:generate go-bindata -pkg create default.yml
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
	// check for existing mkts.yml and return if it exists
	_, err := os.Stat("mkts.yml")
	if err == nil {
		return nil
	}

	// serialize default file.
	data, err := Asset("default.yml")
	if err != nil {
		return err
	}
	// write mkts.yml to current directory.
	err = ioutil.WriteFile("mkts.yml", data, 0644)
	if err != nil {
		return err
	}
	return nil
}
