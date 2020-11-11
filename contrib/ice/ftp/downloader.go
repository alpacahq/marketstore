package ftp

import (
	"bytes"
	"errors"
	"io/ioutil"

	"github.com/alpacahq/marketstore/v4/utils/log"

	"os"
	"path/filepath"
	"sort"
	"strings"
)

type Loader interface {
	Init(client FtpClient)
	Get(filename string) ([]byte, error)
}

const ProcessedFlag string = ".processed" //FIXME: move it to a common package

type Downloader struct {
	client        FtpClient
	ftpPath       string
	storagePath   string
	filePrefix    string
	processedFlag string
}

type FileInfoMap map[string]os.FileInfo

func NewDownloader(client FtpClient, ftpPath string, storagePath string) *Downloader {
	return &Downloader{
		client:        client,
		ftpPath:       ftpPath,
		storagePath:   storagePath,
		filePrefix:    "reorg",
		processedFlag: ProcessedFlag,
	}
}

func (f *Downloader) Init(client FtpClient) {
	f.client = client
	f.filePrefix = "reorg"
}

func (f *Downloader) Get(filename string) ([]byte, error) {
	var buffer = &bytes.Buffer{}
	err := f.client.Retrieve(f.withFtpPath(filename), buffer)
	log.Info("Get %s, len: %d\n", filename, buffer.Len())
	return buffer.Bytes(), err
}

func (f *Downloader) DownloadReorgFile() ([]byte, error) {
	filename, err := f.mostRecentReorgFile(f.ftpPath)
	switch err {
	case nil:
		return f.Get(filename)
	default:
		return nil, err
	}
}

func (f *Downloader) getDatePart(filename string) string {
	parts := strings.Split(filename, ".")
	if len(parts) == 2 && parts[0] == f.filePrefix {
		return parts[1]
	} else {
		return ""
	}
}

func (f *Downloader) filenameFor(dateStr string) string {
	return f.filePrefix + "." + dateStr
}

func (f *Downloader) withFtpPath(filename string) string {
	return filepath.Join(f.ftpPath, filename)
}

func (f *Downloader) withStoragePath(filename string) string {
	return filepath.Join(f.storagePath, filename)
}

func (f *Downloader) mostRecentReorgFile(path string) (string, error) {
	files, err := f.client.ReadDir(path)
	if err != nil {
		return "", err
	}
	dates := make([]string, 0)
	for _, file := range files {
		if datePart := f.getDatePart(file.Name()); datePart != "" {
			dates = append(dates, datePart)
		}
	}
	if len(dates) > 0 {
		sort.Strings(dates)
		mostRecent := f.filenameFor(dates[len(dates)-1])
		log.Info("most recent reorg file:", mostRecent)
		return mostRecent, nil
	}
	return "", errors.New("File not found!")
}

func (f *Downloader) getRemoteFiles() (FileInfoMap, error) {
	remotefiles, err := f.client.ReadDir(f.ftpPath)
	if err != nil {
		return nil, err
	}
	return f.filterPrefix(remotefiles), nil
}

func (f *Downloader) getLocalFiles() (FileInfoMap, error) {
	localfiles, err := ioutil.ReadDir(f.storagePath)
	if err != nil {
		return nil, err
	}
	return f.filterPrefix(localfiles), nil
}

func (f *Downloader) filterPrefix(files []os.FileInfo) FileInfoMap {
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

func (f *Downloader) Sync() []string {
	filenames, err := f.remoteLocalDiff()
	if err != nil {
		return []string{}
	}
	log.Info("Downloading:")
	for _, filename := range filenames {
		log.Info(filename)
		file, err := os.Create(f.withStoragePath(filename))
		if err != nil {
			log.Info("Unable to create local file %s, error:\n %s\n", filename, err)
			continue
		}
		err = f.client.Retrieve(f.withFtpPath(filename), file)
	}
	return filenames
}
