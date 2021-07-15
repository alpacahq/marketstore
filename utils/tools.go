// +build tools

package utils

// https://github.com/golang/go/wiki/Modules#how-can-i-track-tool-dependencies-for-a-module
import (
	_ "github.com/golang/mock/mockgen"
	_ "golang.org/x/tools/cmd/stringer"
)
