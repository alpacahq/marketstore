package ftp

import (
	"os"
	"io"
	"io/ioutil"
	"time"
	"errors"
)


type MockFtpClient struct {
	files map[string][]byte
	dirs map[string][]string
}

func NewMockClient() (MockFtpClient) {
	return MockFtpClient{files: make(map[string][]byte), dirs: make(map[string][]string)}
}


func (m MockFtpClient) Retrieve(path string, dest io.Writer) error {
	buff, ok := m.files[path]
	if ok {
		dest.Write(buff)
		return nil
	} else {
		return errors.New("Retrieve: file not found")
	}
}

func (m MockFtpClient) ReadDir(path string) ([]os.FileInfo, error) {
	fileinfos := make([]os.FileInfo, 0)
	for _, filename := range(m.dirs[path]) {
		fileinfos = append(fileinfos, MockFile{Name_: filename}) 
	}
	return ioutil.ReadDir("."+path)
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
	Name_ string
	Size_ int64
	Mode_ os.FileMode
	ModTime_ time.Time 
	IsDir_ bool
	Sys_ interface{} 
}

func (m MockFile) Name() string {
	return m.Name_
}     
func (m MockFile) Size() int64 {
	return m.Size_
}
func (m MockFile) Mode() os.FileMode {
	return m.Mode_
}   
func (m MockFile) ModTime() time.Time {
	return m.ModTime_
}
func (m MockFile) IsDir() bool {
	return m.IsDir_
}   

func (m MockFile) Sys() interface{} {
	return m.Sys_
} 

