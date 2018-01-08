package feedmanager

import (
	"fmt"
	"os"
	"path/filepath"
	"plugin"
	"strings"

	"github.com/alpacahq/marketstore/utils/log"
)

func LoadFromGOPATH(pluginName string) (pi *plugin.Plugin, err error) {
	envGOPATH := os.Getenv("GOPATH")
	gopaths := strings.Split(envGOPATH, ":")
	if len(gopaths) == 0 {
		return nil, fmt.Errorf("GOPATH is not set\n")
	}
	for _, path := range gopaths {
		pluginPath := filepath.Join(filepath.Join(path, "bin"), pluginName)
		log.Log(log.INFO, "Trying to load module from path: %s...\n", pluginPath)
		pi, err = plugin.Open(pluginPath)
		if err == nil {
			log.Log(log.INFO, "Success loading module %s.\n", pluginPath)
			return pi, nil
		}
	}
	/*
		Check the local directory - helpful for testing
	*/
	pluginPath := filepath.Join(".", pluginName)
	pi, err = plugin.Open(pluginPath)
	if err != nil {
		return nil,
			fmt.Errorf("module %s not found in bin under any paths in GOPATH=%s or local directory\n",
				pluginName, envGOPATH)
	}
	return pi, err
}
