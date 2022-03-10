package create

// Package create - because packages cannot be named 'init' in go.

import (
	_ "embed"
	"os"

	"github.com/spf13/cobra"
)

const (
	usage   = "init"
	short   = "Creates a new mkts.yml file"
	long    = "This command creates a new mkts.yml file in the current directory"
	example = "marketstore init"
)

// Cmd is the init command.
var Cmd = &cobra.Command{
	Use:        usage,
	Short:      short,
	Long:       long,
	SuggestFor: []string{"create", "new"},
	Example:    example,
	RunE:       executeInit,
}

//go:embed default.yml
var defaultYmlBinary []byte

// executeInit implements the init command.
func executeInit(*cobra.Command, []string) error {
	// check for existing mkts.yml and return if it exists
	_, err := os.Stat("mkts.yml")
	if err == nil {
		return nil
	}

	// write mkts.yml to current directory.
	err = os.WriteFile("mkts.yml", defaultYmlBinary, 0o600)
	if err != nil {
		return err
	}
	return nil
}
