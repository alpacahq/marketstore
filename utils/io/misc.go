package io

import (
	"fmt"
	"runtime"
	"syscall"

	"github.com/alpacahq/marketstore/v4/utils/log"
	"go.uber.org/zap"
)

func Syncfs() {
	if err := syscall.Sync(); err != nil {
		log.Error("failed to call Sync", zap.Error(err))
	}
}

func GetCallerFileContext(level int) (fileContext string) {
	_, file, line, _ := runtime.Caller(1 + level)
	return fmt.Sprintf("%s:%d", file, line)
}
