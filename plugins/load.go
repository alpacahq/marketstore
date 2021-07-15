package plugins

import (
	"fmt"
	"os"
	"path/filepath"
	"plugin"
	"strings"

	"github.com/pkg/errors"

	"github.com/alpacahq/marketstore/v4/utils/log"
)

type SymbolLoader struct {
	module *plugin.Plugin
}

// NewSymbolLoader creates a SymbolLoader that loads symbol from a particular module.
// moduleName can be a file name under one of $GOPATH directories or current working
// directory, or an absolute path to the file.
func NewSymbolLoader(moduleName string) (*SymbolLoader, error) {
	pi, err := Load(moduleName)
	if err != nil {
		return nil, err
	}
	return &SymbolLoader{
		module: pi,
	}, nil
}

// LoadSymbol looks up a symbol from the module.  Plugin packages can accept this
// by defining an interface type without importing this package.  It is important
// to note that each plugin package cannot import this plugins package since
// plugin module cannot import any packages that import built-in plugin package.
func (l *SymbolLoader) LoadSymbol(symbolName string) (interface{}, error) {
	return l.module.Lookup(symbolName)
}

// Load loads plugin module.  If pluginName is relative path name, it is
// loaded from one of the current GOPATH directories or current working directory.
// If the path is an absolute path, it loads from the path. err is nil
// if it succeeds.
func Load(pluginName string) (pi *plugin.Plugin, err error) {
	if filepath.IsAbs(pluginName) {
		return plugin.Open(pluginName)
	}
	envGOPATH := os.Getenv("GOPATH")
	gopaths := strings.Split(envGOPATH, ":")
	if len(gopaths) == 0 {
		return nil, fmt.Errorf("GOPATH is not set\n")
	}
	for _, path := range gopaths {
		pluginPath := filepath.Join(filepath.Join(path, "bin"), pluginName)
		log.Info("Trying to load module from path: %s...\n", pluginPath)
		pi, err = plugin.Open(pluginPath)
		if err == nil {
			log.Info("Success loading module %s.\n", pluginPath)
			return pi, nil
		}
	}
	log.Debug("failed to load module from GOPATHs. err=" + err.Error())
	/*
		Check the local directory - helpful for testing
	*/
	pluginPath := filepath.Join(".", pluginName)
	pi, err = plugin.Open(pluginPath)
	if err != nil {
		return nil,
			errors.Wrap(err, fmt.Sprintf("module %s not found in bin under any paths in GOPATH=%s or local directory\n",
				pluginName, envGOPATH))
	}
	return pi, err
}
