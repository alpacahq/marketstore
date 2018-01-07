package io

import (
	"fmt"
	"io"
	"os"
	"path"
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

func CopyDir(source, dest string) (err error) {
	err = os.MkdirAll(dest, 0777)
	if err != nil {
		return err
	}
	dir, _ := os.Open(source)
	defer dir.Close()
	objects, err := dir.Readdir(-1)
	for _, obj := range objects {
		srcStr := source + "/" + obj.Name()
		destStr := dest + "/" + obj.Name()
		if obj.IsDir() {
			err = CopyDir(srcStr, destStr)
			if err != nil {
				return err
			}
		} else if path.Ext(obj.Name()) != ".bin" {
			err = CopyFile(srcStr, destStr)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func CopyFile(source, dest string) (err error) {
	srcFile, err := os.Open(source)
	defer srcFile.Close()
	if err != nil {
		return err
	}
	destFile, err := os.Create(dest)
	if err != nil {
		return err
	}
	defer destFile.Close()
	_, err = io.Copy(destFile, srcFile)
	if err == nil {
		err = os.Chmod(dest, 0777)
	}
	return err
}
