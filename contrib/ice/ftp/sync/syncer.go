package main

import (
	"log"
	"flag"
	"net/url"
	"os"
	"github.com/alpacahq/marketstore/v4/contrib/ice/ftp"
)

var (
	rawFtpUrl string
	ftpUrl *url.URL 
	dataDir string
)


func init() {
	flag.StringVar(&rawFtpUrl, "url", "", "full FTP url for ICE data, in the form of 'ftp://username:password@ice-ftp-host:port/path-to-files'")
	flag.StringVar(&dataDir, "datadir", "./data", "directory for storing ICE's reorg files")
	flag.Parse()
}


func main() {
	if len(rawFtpUrl) == 0 {
		println("Usage: ")
		flag.PrintDefaults()
		return 
	}
	ftpUrl, error := url.Parse(rawFtpUrl)
	if error != nil {
		log.Println("Please provide a valid url!", rawFtpUrl)
		return
	}
	log.Printf("%+v\n", ftpUrl)

	if err := os.MkdirAll(dataDir, 0777); err != nil {
		log.Println("Cannot create local storage directory!")
	}

	password, ok := ftpUrl.User.Password()
	if !ok {
		log.Println("Please provide a password in the FTP url!")
		return
	}
	client, err := ftp.NewClient(ftpUrl.User.Username(), password, ftpUrl.Host)
	if err != nil {
		log.Println("Unable to connect to ICE:", err)
		return 
	}
	defer func () { client.Close(); println("connection closed")}()

	log.Println("Succesfully connected to ICE")
	log.Printf("%T, %+v\n", client, client)

	loader := ftp.NewDownloader(client, ftpUrl.Path, dataDir)

	newFiles := loader.Sync()
	log.Printf("%+v\n", newFiles)
}