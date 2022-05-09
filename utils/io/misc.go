package io

import (
	"fmt"
	"runtime"
	"syscall"
)

func Syncfs() {
	syscall.Sync()
}

func GetCallerFileContext(level int) (fileContext string) {
	_, file, line, _ := runtime.Caller(1 + level)
	return fmt.Sprintf("%s:%d", file, line)
}
