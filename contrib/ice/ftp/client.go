package ftp

import (
	"io"
	"os"

	"github.com/alpacahq/marketstore/v4/utils/log"
	"github.com/secsy/goftp"
)

type FtpClient interface {
	Retrieve(path string, dest io.Writer) error
	ReadDir(path string) ([]os.FileInfo, error)
	Close() error
}

func NewClient(username string, password string, ftp_host string) (FtpClient, error) {
	config := goftp.Config{
		User:     username,
		Password: password,
	}
	log.Info("Connecting to ICE...")
	return goftp.DialConfig(config, ftp_host)
}
