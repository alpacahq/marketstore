package wal

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/alpacahq/marketstore/v4/utils/log"
)

type Finder struct {
	dirRead func(name string) ([]os.DirEntry, error)
}

func NewFinder(dirRead func(name string) ([]os.DirEntry, error)) *Finder {
	return &Finder{dirRead: dirRead}
}

// Find returns all absolute paths to "*.walfile" files directly under the directory.
func (f *Finder) Find(dir string) ([]string, error) {
	var ret []string
	files, err := f.dirRead(dir)
	if err != nil {
		return nil, fmt.Errorf("unable to read the directory %s: %w", dir, err)
	}
	for _, file := range files {
		// ignore directories
		if file.IsDir() {
			// ignore
			continue
		}

		// ignore files except wal
		filename := file.Name()
		if filepath.Ext(filename) != ".walfile" {
			continue
		}

		log.Debug("found a WALFILE: %s", filename)
		ret = append(ret, filepath.Join(dir, filename))
	}
	return ret, nil
}
