package ftp

import (
	"net/url"
	"os"

	"github.com/alpacahq/marketstore/v4/contrib/ice/enum"
	"github.com/alpacahq/marketstore/v4/contrib/ice/ftp"
	"github.com/alpacahq/marketstore/v4/utils/log"
	"github.com/spf13/cobra"
)

// FTPSyncCmd downloads reorg and sirs files from ICE's ftp directory to a local path
var FTPSyncCmd = &cobra.Command{
	Use:          "sync <localdir> <ftpurl>",
	Short:        "sync ICE's FTP directory to a local path",
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) != 2 || args[0] == "" || args[1] == "" {
			return cmd.Help()
		}

		dataDir := args[0]
		rawFtpURL := args[1]
		ftpURL, err := url.Parse(rawFtpURL)
		if err != nil {
			log.Info("Please provide a valid url!", rawFtpURL)
			return err
		}

		if err := os.MkdirAll(dataDir, 0777); err != nil {
			log.Info("Cannot create local storage directory!")
			return err
		}

		password, ok := ftpURL.User.Password()
		if !ok {
			log.Info("Please provide a password in the FTP url!")
			return err
		}
		client, err := ftp.NewClient(ftpURL.User.Username(), password, ftpURL.Host)
		if err != nil {
			log.Info("Unable to connect to ICE:", err)
			return err
		}
		defer func() { client.Close(); println("connection closed") }()

		log.Info("Succesfully connected to ICE: %+v", ftpURL)

		reorgLoader := ftp.NewDownloader(client, ftpURL.Path, dataDir, enum.ReorgFilePrefix)
		newFiles := reorgLoader.Sync()
		log.Info("New reorg files downloaded: %+v", newFiles)

		sirsLoader := ftp.NewDownloader(client, ftpURL.Path, dataDir, enum.SirsFilePrefix)
		newFiles = sirsLoader.Sync()
		log.Info("New sirs files downloaded: %+v", newFiles)

		return nil
	},
}
