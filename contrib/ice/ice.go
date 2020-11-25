package main

import (
	"os"

	"github.com/alpacahq/marketstore/v4/contrib/ice/cmd"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

func main() {
	if err := cmd.Cmd.Execute(); err != nil {
		log.Error("%+v", err)
		os.Exit(1)
	}
}
