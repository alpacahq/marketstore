package cmd

import (
	"fmt"
	"github.com/pkg/errors"
)

func ABAB() error {
	return errors.New("hogehoge")
}

func Hoge() {
	err := ABAB()
	fmt.Println(err)
	err = ABAB()
}
