package ftp

import (
	"errors"
	"fmt"
	"io"
	"os"
	"time"
)

type MockFtpClient struct {
	files map[string][]byte
	dirs  map[string][]string
}

func NewMockClient() MockFtpClient {
	return MockFtpClient{files: make(map[string][]byte), dirs: make(map[string][]string)}
}

func (m MockFtpClient) Retrieve(path string, dest io.Writer) error {
	buff, ok := m.files[path]
	if !ok {
		return errors.New("Retrieve: file not found")
	}
	_, _ = dest.Write(buff)
	return nil
}

func (m MockFtpClient) ReadDir(path string) ([]os.FileInfo, error) {
	dirEntries, err := os.ReadDir("." + path)
	if err != nil {
		return nil, err
	}

	// convert []fs.DirEntry to []os.FileInfo
	fileInfos := make([]os.FileInfo, len(dirEntries))
	for i, dirEntry := range dirEntries {
		lf, err := dirEntry.Info()
		if err != nil {
			return nil, fmt.Errorf("get file info for a dir entry: %w", err)
		}
		fileInfos[i] = lf
	}

	return fileInfos, nil
}

func (m MockFtpClient) Close() error {
	return nil
}

func (m *MockFtpClient) SetFileContent(path string, content []byte) {
	m.files[path] = content
}

func (m *MockFtpClient) SetDirContent(path string, filenames []string) {
	m.dirs[path] = filenames
}

type MockFile struct {
	name    string
	size    int64
	mode    os.FileMode
	modTime time.Time
	isDir   bool
	sys     interface{}
}

func (m *MockFile) Name() string {
	return m.name
}

func (m *MockFile) Size() int64 {
	return m.size
}

func (m *MockFile) Mode() os.FileMode {
	return m.mode
}

func (m *MockFile) ModTime() time.Time {
	return m.modTime
}

func (m *MockFile) IsDir() bool {
	return m.isDir
}

func (m *MockFile) Sys() interface{} {
	return m.sys
}
