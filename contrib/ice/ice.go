package main

import (
	"os"

	"github.com/alpacahq/marketstore/v4/contrib/ice/cmd"
)

func main() {
	if err := cmd.Cmd.Execute(); err != nil {
		os.Exit(1)
	}
}
