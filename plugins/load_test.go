package plugins

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"plugin"
	"runtime"
	"testing"

	. "gopkg.in/check.v1"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }

type TestSuite struct {
	TestPluginLib string
	OldGoPath     string
}

var _ = Suite(&TestSuite{})

func (s *TestSuite) SetUpSuite(c *C) {
	osType := runtime.GOOS
	if osType != "linux" {
		c.Skip("Only linux runs plugins")
	}
	dirName := c.MkDir()
	binDirName := filepath.Join(dirName, "bin")
	os.MkdirAll(binDirName, 0777)
	testFileName := "plugin.go"
	testFilePath := filepath.Join(dirName, testFileName)
	s.TestPluginLib = "plugin.so"
	soFilePath := filepath.Join(binDirName, s.TestPluginLib)
	file, err := os.Create(testFilePath)
	if err != nil {
		c.Fatal("Could not create test plugin source file")
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
		c.Skip("Unable to build test plugin ** is go version > 1.9 in your path?")
	}

	goPath := os.Getenv("GOPATH")
	newGoPath := dirName + ":" + goPath
	s.OldGoPath = goPath
	os.Setenv("GOPATH", newGoPath)
}

func (s *TestSuite) TearDownSuite(c *C) {
	if s.OldGoPath != "" {
		os.Setenv("GOPATH", s.OldGoPath)
	}
}

func (s *TestSuite) TestLoadFromGOPATH(c *C) {
	var pi *plugin.Plugin
	var err error
	pi, err = Load(s.TestPluginLib)
	c.Check(pi, NotNil)
	c.Check(err, IsNil)
}
