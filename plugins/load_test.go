package plugins_test

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"plugin"
	"runtime"
	"testing"

	"github.com/alpacahq/marketstore/v4/plugins"
	"github.com/alpacahq/marketstore/v4/utils/test"
	"github.com/stretchr/testify/assert"
)

func setup(t *testing.T, testName string) (tearDown func(), testPluginLib, oldGoPath, absTestPluginLib string) {
	dirName, _ := ioutil.TempDir("", fmt.Sprintf("plugins_test-%s", testName))

	osType := runtime.GOOS
	if osType != "linux" {
		t.Skip("Only linux runs plugins")
	}

	binDirName := filepath.Join(dirName, "bin")
	os.MkdirAll(binDirName, 0777)
	testFileName := "plugin.go"
	testFilePath := filepath.Join(dirName, testFileName)
	testPluginLib = "plugin.so"
	soFilePath := filepath.Join(binDirName, testPluginLib)
	file, err := os.Create(testFilePath)
	if err != nil {
		t.Fatal("Could not create test plugin source file")
	}
	code := `package main
func main() {}
`
	file.WriteString(code)
	file.Close()
	cmd := exec.Command("go",
		"build",
		"-buildmode=plugin",
		"-o",
		soFilePath,
		testFilePath)

	if err := cmd.Run(); err != nil {
		fmt.Println(err)
		t.Skip("Unable to build test plugin ** is go version > 1.9 in your path?")
	}

	goPath := os.Getenv("GOPATH")
	newGoPath := dirName + ":" + goPath
	oldGoPath = goPath
	absTestPluginLib = soFilePath
	os.Setenv("GOPATH", newGoPath)

	return func() {
		test.CleanupDummyDataDir(dirName)

		if oldGoPath != "" {
			os.Setenv("GOPATH", oldGoPath)
		}
	}, testPluginLib, oldGoPath, absTestPluginLib
}

func TestLoadFromGOPATH(t *testing.T) {
	tearDown, testPluginLib, _, absTestPluginLib := setup(t, "TestLoadFromGOPATH")
	defer tearDown()

	var pi *plugin.Plugin
	var err error
	pi, err = plugins.Load(testPluginLib)
	assert.NotNil(t, pi)
	assert.Nil(t, err)

	pi, err = plugins.Load("nonexistent")
	assert.Nil(t, pi)
	assert.NotNil(t, err)

	// abs path case
	pi, err = plugins.Load(absTestPluginLib)
	assert.NotNil(t, pi)
	assert.Nil(t, err)
}
