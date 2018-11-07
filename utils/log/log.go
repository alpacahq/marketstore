package log

import (
	"fmt"

	"go.uber.org/zap"
)

func init() {
	logger, _ := zap.NewProduction()
	zap.ReplaceGlobals(logger)
}

func Info(format string, args ...interface{}) {
	if logLevel <= INFO {
		zap.S().Info(fmt.Sprintf(format, args...))
	}
}

func Warn(format string, args ...interface{}) {
	if logLevel <= WARNING {
		zap.S().Warn(fmt.Sprintf(format, args...))
	}
}

func Error(format string, args ...interface{}) {
	if logLevel <= ERROR {
		zap.S().Error(fmt.Sprintf(format, args...))
	}
}

func Fatal(format string, args ...interface{}) {
	zap.S().Fatal(fmt.Sprintf(format, args...))
}

func SetLevel(level Level) {
	logLevel = level
}

type Level int

const (
	INFO Level = iota
	WARNING
	ERROR
	FATAL
)

var logLevel Level
