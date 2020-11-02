package main

import (
	"os"
	"github.com/alpacahq/marketstore/v4/contrib/corporateactions/cmd"
)

func main() {
	if err := cmd.Cmd.Execute(); err != nil {
		os.Exit(1)
	}
}
