// Code generated for package create by go-bindata DO NOT EDIT. (@generated)
// sources:
// default.yml
package create

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func bindataRead(data []byte, name string) ([]byte, error) {
	gz, err := gzip.NewReader(bytes.NewBuffer(data))
	if err != nil {
		return nil, fmt.Errorf("Read %q: %v", name, err)
	}

	var buf bytes.Buffer
	_, err = io.Copy(&buf, gz)
	clErr := gz.Close()

	if err != nil {
		return nil, fmt.Errorf("Read %q: %v", name, err)
	}
	if clErr != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

type asset struct {
	bytes []byte
	info  os.FileInfo
}

type bindataFileInfo struct {
	name    string
	size    int64
	mode    os.FileMode
	modTime time.Time
}

// Name return file name
func (fi bindataFileInfo) Name() string {
	return fi.name
}

// Size return file size
func (fi bindataFileInfo) Size() int64 {
	return fi.size
}

// Mode return file mode
func (fi bindataFileInfo) Mode() os.FileMode {
	return fi.mode
}

// Mode return file modify time
func (fi bindataFileInfo) ModTime() time.Time {
	return fi.modTime
}

// IsDir return file whether a directory
func (fi bindataFileInfo) IsDir() bool {
	return fi.mode&os.ModeDir != 0
}

// Sys return file is sys mode
func (fi bindataFileInfo) Sys() interface{} {
	return nil
}

var _defaultYml = []byte("\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\xff\xac\x94\xdf\x8f\xdb\x36\x0c\xc7\xdf\xfd\x57\x10\xce\xcb\xdd\x01\xb9\x24\xdb\x82\x21\x7a\x4b\x6f\x6b\xbb\xa1\x3f\x82\xb5\x1d\xd6\x27\x43\xb6\x68\x47\x88\x2c\xba\x14\x7d\x49\x86\xfe\xf1\x83\xec\xe4\x72\xe7\x34\xc0\xae\xa8\x9e\x2c\x92\xfe\xf0\x4b\x4a\xe2\x08\xc6\xff\x77\x25\x23\x58\xb6\x42\xe3\x0a\x3d\xb2\x16\x34\x50\x6b\xde\xa0\x04\x21\x46\x28\xc8\x97\xb6\x6a\x59\x8b\x25\x7f\x9b\x3c\x87\xcb\x44\x92\x19\xcb\x58\x08\xf1\x5e\x81\xd1\xa2\x61\xb0\x46\x10\xa3\xe0\x21\x0a\xa8\x04\x59\x63\x17\x9b\xeb\x80\xc9\x08\x9c\x0d\x82\x3e\x5b\x53\x10\x05\xa9\xa3\x42\xbb\xf8\x9d\x3e\x86\xf4\x31\x10\xed\x50\x12\x3f\xfc\x0e\x01\xf9\x1e\x19\xae\xa8\x89\xfa\xb5\xbb\x4e\x0e\xb8\x86\x58\x14\xcc\x17\x8b\x9f\x87\x92\x3a\x60\x74\x03\xee\x1a\x0a\x68\x20\xdf\x3f\xd1\x74\x84\xc6\x44\x7f\x7e\x78\xff\x6e\xfc\xd7\xea\x0e\x96\xab\x3f\x92\x8a\x9b\x22\x1b\xf2\xe7\xdf\x8b\x7e\x75\xc4\x3a\xaa\x32\x87\xf7\xe8\x14\x58\x5f\xd2\x37\xf4\x76\x2d\xa0\x0a\xba\x28\xb8\x8a\x51\x5f\xb7\x9a\xfd\x57\x64\x26\xbe\x4e\xbe\xb4\xc8\x7b\x9d\x3b\x54\x20\xdc\xe2\x05\x82\x76\x8e\xb6\x27\x29\x42\x90\x23\xc4\x5f\x2d\x1a\x90\x35\x53\x5b\xad\x41\x43\xe1\x2c\x7a\x89\x17\xc3\x63\x11\xbb\x9a\x04\xa1\x26\xab\x58\x17\x98\x35\xc8\x96\x8c\x82\x69\xb2\xd5\x2e\x63\x12\x2d\x98\x59\x2f\xc8\xf7\xda\x29\x98\x27\xe8\xa3\x8e\x4c\x1b\x73\x59\xcb\x51\x4a\xc1\xa8\xc5\xfa\x0a\x3c\x6e\x41\x6c\x8d\x90\xb7\xc5\x06\x05\x36\xb8\x0f\x47\x12\x63\x4d\xf7\xa8\xa0\xd4\x2e\x9c\xd1\x8e\xa4\x2e\x28\x92\x70\x67\x43\x87\x3c\xc3\x8d\x3a\xd3\xbf\xe4\x51\x41\xba\xac\x91\x6d\xa1\x27\xef\x70\x9b\x7d\x26\xde\xa4\x47\xdc\x31\x26\x76\xa7\x0d\xd8\x1d\x54\xb4\x05\xd1\x75\x13\xe0\xca\x60\xa9\x5b\x27\xf0\xe9\xe3\xdd\x75\x32\x82\x56\xac\xb3\x62\x31\x64\x2d\xbb\xc7\xb7\x57\xcd\x17\x8b\x5f\xd2\x0e\xd9\xd7\x01\x06\xf3\xb6\xaa\xa2\xb4\xa6\x61\x2a\x41\x7b\x03\x6b\xd4\x2c\x39\x6a\x01\xf4\xa6\x21\xeb\x25\x24\xcf\x78\x82\xc9\x08\x7e\xdf\xe9\xba\x71\x08\xc2\xb6\xaa\x90\xa1\x26\xd3\x3a\x8c\xd5\x7e\xf2\xe3\x82\xea\x3a\x1e\xa5\xd0\x41\xc4\x73\xde\x77\x32\x8a\x2d\xeb\xb1\x41\x25\x23\x00\x18\x1f\xf0\x0a\xc8\x1b\x1b\x36\xba\xaa\x6e\x03\x75\x2e\x00\xf2\x0a\xd2\x9b\xc9\xec\xad\xf5\x93\xf7\xaf\xdf\xdc\xfd\x9d\x1e\x1c\xfd\x84\x51\x87\x1d\x80\xc1\x78\x42\xdd\xbc\x09\x27\x6b\xa4\xcf\xdf\x5a\xff\xc4\x30\x3b\xb7\xbc\x7e\xba\xfd\x6d\x20\x2c\x08\xa3\xae\xcf\x54\xdd\x4c\x6e\x2e\xc9\x29\xad\x13\x64\x05\x5e\x07\xa3\xbf\x74\x55\x7f\x47\xff\x73\x5d\x6c\x2a\xa6\xd6\x1b\xd8\x12\x6f\x7e\xfc\x49\xe4\x55\xcf\x3d\x3b\x8a\xca\xe8\x5d\x89\x68\x90\x4f\x55\x7b\x5d\xa3\x82\x57\x46\xef\x5e\xa2\x14\x6b\xe4\x0b\xb5\x77\x73\x23\x0b\xa2\xe3\x2c\x4b\x7f\x9a\xce\x7e\x1d\x4f\x17\xe3\xe9\x0c\xa6\x53\x35\x9d\xa6\x83\x4c\x0d\xb9\x7d\x45\xfe\x94\xe6\x94\x6a\xd5\xbb\x1e\xd9\x87\xa9\xfa\xa5\x1b\x9b\x6d\x70\xaf\x60\x4f\x2d\x67\x87\xdd\x20\x26\x0e\xa7\xfe\x39\xad\x45\x9a\xa0\x26\x13\xdd\xd8\xdb\x63\x72\x4b\x83\xf0\xb0\xaf\x73\x72\x61\x98\xa9\xbf\x1f\xcb\xe5\xea\xcd\x37\x1d\x1f\x56\x9f\x07\xd5\xe5\x56\x6a\xbc\xd0\xc9\x17\x9d\xef\x65\xe7\x7b\x46\x2b\x67\x83\x56\x5e\x90\x3b\x86\xdb\x7f\x5e\x7c\x7c\x30\x74\xf5\xc7\x99\x53\x72\x97\x3c\x8d\x8f\x20\x4d\xfe\x0b\x00\x00\xff\xff\xdd\x9a\x9e\x58\xf2\x07\x00\x00")

func defaultYmlBytes() ([]byte, error) {
	return bindataRead(
		_defaultYml,
		"default.yml",
	)
}

func defaultYml() (*asset, error) {
	bytes, err := defaultYmlBytes()
	if err != nil {
		return nil, err
	}

	info := bindataFileInfo{name: "default.yml", size: 2034, mode: os.FileMode(420), modTime: time.Unix(1605195683, 0)}
	a := &asset{bytes: bytes, info: info}
	return a, nil
}

// Asset loads and returns the asset for the given name.
// It returns an error if the asset could not be found or
// could not be loaded.
func Asset(name string) ([]byte, error) {
	cannonicalName := strings.Replace(name, "\\", "/", -1)
	if f, ok := _bindata[cannonicalName]; ok {
		a, err := f()
		if err != nil {
			return nil, fmt.Errorf("Asset %s can't read by error: %v", name, err)
		}
		return a.bytes, nil
	}
	return nil, fmt.Errorf("Asset %s not found", name)
}

// MustAsset is like Asset but panics when Asset would return an error.
// It simplifies safe initialization of global variables.
func MustAsset(name string) []byte {
	a, err := Asset(name)
	if err != nil {
		panic("asset: Asset(" + name + "): " + err.Error())
	}

	return a
}

// AssetInfo loads and returns the asset info for the given name.
// It returns an error if the asset could not be found or
// could not be loaded.
func AssetInfo(name string) (os.FileInfo, error) {
	cannonicalName := strings.Replace(name, "\\", "/", -1)
	if f, ok := _bindata[cannonicalName]; ok {
		a, err := f()
		if err != nil {
			return nil, fmt.Errorf("AssetInfo %s can't read by error: %v", name, err)
		}
		return a.info, nil
	}
	return nil, fmt.Errorf("AssetInfo %s not found", name)
}

// AssetNames returns the names of the assets.
func AssetNames() []string {
	names := make([]string, 0, len(_bindata))
	for name := range _bindata {
		names = append(names, name)
	}
	return names
}

// _bindata is a table, holding each asset generator, mapped to its name.
var _bindata = map[string]func() (*asset, error){
	"default.yml": defaultYml,
}

// AssetDir returns the file names below a certain
// directory embedded in the file by go-bindata.
// For example if you run go-bindata on data/... and data contains the
// following hierarchy:
//     data/
//       foo.txt
//       img/
//         a.png
//         b.png
// then AssetDir("data") would return []string{"foo.txt", "img"}
// AssetDir("data/img") would return []string{"a.png", "b.png"}
// AssetDir("foo.txt") and AssetDir("notexist") would return an error
// AssetDir("") will return []string{"data"}.
func AssetDir(name string) ([]string, error) {
	node := _bintree
	if len(name) != 0 {
		cannonicalName := strings.Replace(name, "\\", "/", -1)
		pathList := strings.Split(cannonicalName, "/")
		for _, p := range pathList {
			node = node.Children[p]
			if node == nil {
				return nil, fmt.Errorf("Asset %s not found", name)
			}
		}
	}
	if node.Func != nil {
		return nil, fmt.Errorf("Asset %s not found", name)
	}
	rv := make([]string, 0, len(node.Children))
	for childName := range node.Children {
		rv = append(rv, childName)
	}
	return rv, nil
}

type bintree struct {
	Func     func() (*asset, error)
	Children map[string]*bintree
}

var _bintree = &bintree{nil, map[string]*bintree{
	"default.yml": &bintree{defaultYml, map[string]*bintree{}},
}}

// RestoreAsset restores an asset under the given directory
func RestoreAsset(dir, name string) error {
	data, err := Asset(name)
	if err != nil {
		return err
	}
	info, err := AssetInfo(name)
	if err != nil {
		return err
	}
	err = os.MkdirAll(_filePath(dir, filepath.Dir(name)), os.FileMode(0755))
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(_filePath(dir, name), data, info.Mode())
	if err != nil {
		return err
	}
	err = os.Chtimes(_filePath(dir, name), info.ModTime(), info.ModTime())
	if err != nil {
		return err
	}
	return nil
}

// RestoreAssets restores an asset under the given directory recursively
func RestoreAssets(dir, name string) error {
	children, err := AssetDir(name)
	// File
	if err != nil {
		return RestoreAsset(dir, name)
	}
	// Dir
	for _, child := range children {
		err = RestoreAssets(dir, filepath.Join(name, child))
		if err != nil {
			return err
		}
	}
	return nil
}

func _filePath(dir, name string) string {
	cannonicalName := strings.Replace(name, "\\", "/", -1)
	return filepath.Join(append([]string{dir}, strings.Split(cannonicalName, "/")...)...)
}
