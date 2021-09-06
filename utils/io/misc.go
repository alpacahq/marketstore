package io

import (
	"fmt"
	"runtime"
	"syscall"
)

func Syncfs() {
	syscall.Sync()
}

func GetCallerFileContext(level int) (FileContext string) {
	_, file, line, _ := runtime.Caller(1 + level)
	return fmt.Sprintf("%s:%d", file, line)
}
