package ftp

import (
	"io"
	"os"

	"github.com/secsy/goftp"

	"github.com/alpacahq/marketstore/v4/utils/log"
)

type Client interface {
	Retrieve(path string, dest io.Writer) error
	ReadDir(path string) ([]os.FileInfo, error)
	Close() error
}

var _ Client = (*goftp.Client)(nil)

// NewClient is a thin wrapper around goftp.DialConfig. Connects instantly to the specified server.
func NewClient(username, password, ftpHost string) (Client, error) {
	config := goftp.Config{
		User:     username,
		Password: password,
	}
	log.Info("Connecting to ICE...")
	return goftp.DialConfig(config, ftpHost)
}
