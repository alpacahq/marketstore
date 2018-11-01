package log

import (
	"fmt"

	"go.uber.org/zap"
)

func init() {
	logger, _ := zap.NewProduction()
	zap.ReplaceGlobals(logger)
}

func Log(level Level, format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	switch level {
	default:
	case INFO:
		if logLevel >= INFO {
			zap.S().Info(msg)
		}
	case WARNING:
		if logLevel >= WARNING {
			zap.S().Warn(msg)
		}
	case ERROR:
		if logLevel >= ERROR {
			zap.S().Error(msg)
		}
	case FATAL:
		zap.S().Fatal(msg)
	}
}

func SetLogLevel(level Level) {
	logLevel = level
}

type Level int

const (
	FATAL Level = iota
	ERROR
	WARNING
	INFO
)

var logLevel Level
