package wal

import (
	"fmt"
	"os"

	"github.com/alpacahq/marketstore/v4/utils/log"
)

func Move(oldFP, newFP string) error {
	if err := os.Rename(oldFP, newFP); err != nil {
		return fmt.Errorf("failed to move %s to %s:%w", oldFP, newFP, err)
	}
	log.Debug(fmt.Sprintf("moved %s to %s", oldFP, newFP))
	return nil
}
