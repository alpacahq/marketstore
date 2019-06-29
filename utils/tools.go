// +build citools

package utils

// This file forces go.mod to download the following tool libraries that we use for CI
import (
	_ "github.com/golangci/golangci-lint/cmd/golangci-lint"
	_ "github.com/reviewdog/reviewdog/cmd/reviewdog"
)
