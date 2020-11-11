package main

import (
	"flag"
	"net/url"
	"os"

	"github.com/alpacahq/marketstore/v4/contrib/ice/ftp"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

var (
	rawFtpURL string
	ftpURL    *url.URL
	dataDir   string
)

func init() {
	flag.StringVar(&rawFtpURL, "url", "", "full FTP url for ICE data, in the form of 'ftp://username:password@ice-ftp-host:port/path-to-files'")
	flag.StringVar(&dataDir, "datadir", "./data", "directory for storing ICE's reorg files")
	flag.Parse()
}

func main() {
	if len(rawFtpURL) == 0 {
		println("Usage: ")
		flag.PrintDefaults()
		os.Exit(-1)
	}
	ftpURL, error := url.Parse(rawFtpURL)
	if error != nil {
		log.Info("Please provide a valid url!", rawFtpURL)
		os.Exit(-1)
	}

	if err := os.MkdirAll(dataDir, 0777); err != nil {
		log.Info("Cannot create local storage directory!")
		os.Exit(-1)
	}

	password, ok := ftpURL.User.Password()
	if !ok {
		log.Info("Please provide a password in the FTP url!")
		os.Exit(-1)
	}
	client, err := ftp.NewClient(ftpURL.User.Username(), password, ftpURL.Host)
	if err != nil {
		log.Info("Unable to connect to ICE:", err)
		os.Exit(-1)
	}
	defer func() { client.Close(); println("connection closed") }()

	log.Info("Succesfully connected to ICE: %+v", ftpURL)

	loader := ftp.NewDownloader(client, ftpURL.Path, dataDir)

	newFiles := loader.Sync()
	log.Info("New files downloaded: %+v", newFiles)
}
