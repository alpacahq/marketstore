package ftp

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/alpacahq/marketstore/v4/contrib/ice/enum"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

type Downloader struct {
	client  Client
	ftpPath string
	storagePath   string
	filePrefix    string
	processedFlag string
}

type FileInfoMap map[string]os.FileInfo

func NewDownloader(client Client, ftpPath, storagePath, filePrefix string) *Downloader {
	return &Downloader{
		client:        client,
		ftpPath:       ftpPath,
		storagePath:   storagePath,
		filePrefix:    filePrefix,
		processedFlag: enum.ProcessedFlag,
	}
}

func (f *Downloader) withFtpPath(filename string) string {
	return filepath.Join(f.ftpPath, filename)
}

func (f *Downloader) withStoragePath(filename string) string {
	return filepath.Join(f.storagePath, filename)
}

func (f *Downloader) getRemoteFiles() (FileInfoMap, error) {
	remotefiles, err := f.client.ReadDir(f.ftpPath)
	if err != nil {
		return nil, err
	}
	return f.filter(remotefiles), nil
}

func (f *Downloader) getLocalFiles() (FileInfoMap, error) {
	localDirEntries, err := os.ReadDir(f.storagePath)
	if err != nil {
		return nil, err
	}

	// convert []fs.DirEntry to []os.FileInfo
	localFiles := make([]os.FileInfo, len(localDirEntries))
	for i, localDirEntry := range localDirEntries {
		lf, err := localDirEntry.Info()
		if err != nil {
			return nil, fmt.Errorf("get file info for a dir entry: %w", err)
		}
		localFiles[i] = lf
	}

	return f.filter(localFiles), nil
}

func (f *Downloader) filter(files []os.FileInfo) FileInfoMap {
	fmap := FileInfoMap{}
	for _, file := range files {
		if strings.HasPrefix(file.Name(), f.filePrefix) {
			fmap[file.Name()] = file
		}
	}
	return fmap
}

func (f *Downloader) remoteLocalDiff() ([]string, error) {
	localfiles, err := f.getLocalFiles()
	if err != nil {
		return nil, err
	}
	remotefiles, err := f.getRemoteFiles()
	if err != nil {
		return nil, err
	}

	filenames := make([]string, 0)

	for remoteFileName, remoteFile := range remotefiles {
		if localFile, exists := localfiles[remoteFileName]; exists {
			if remoteFile.Size() != localFile.Size() {
				filenames = append(filenames, remoteFileName)
			}
		} else {
			if _, exists := localfiles[remoteFileName+f.processedFlag]; !exists {
				filenames = append(filenames, remoteFileName)
			}
		}
	}

	return filenames, nil
}

func (f *Downloader) Sync() ([]string, error) {
	filenames, err := f.remoteLocalDiff()
	if err != nil {
		return nil, err
	}
	log.Info("Downloading:")
	for _, filename := range filenames {
		log.Info(filename)
		file, err := os.Create(f.withStoragePath(filename))
		if err != nil {
			log.Info("Unable to create local file %s, error:\n %s\n", filename, err)
			return nil, err
		}
		err = f.client.Retrieve(f.withFtpPath(filename), file)
		if err != nil {
			return nil, err
		}
	}
	return filenames, nil
}
