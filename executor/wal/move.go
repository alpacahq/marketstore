package wal

import (
	"fmt"
	"os"

	"github.com/alpacahq/marketstore/v4/utils/log"
)

func Move(oldFP, newFP string) error {
	err := os.Rename(oldFP, newFP)
	if err != nil {
		return fmt.Errorf("failed to move %s to %s:%w", oldFP, newFP, err)
	}
	log.Info(fmt.Sprintf("moved %s to %s", oldFP, newFP))
	return nil
}
