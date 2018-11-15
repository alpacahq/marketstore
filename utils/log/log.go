package log

import (
	"go.uber.org/zap"
)

func init() {
	logger, err := zap.NewProduction()
	if err != nil {
		panic(err)
	}

	zap.ReplaceGlobals(logger)
}

func Debug(format string, args ...interface{}) {
	if logLevel <= DEBUG {
		zap.S().Debugf(format, args...)
	}
}

func Info(format string, args ...interface{}) {
	if logLevel <= INFO {
		zap.S().Infof(format, args)
	}
}

func Warn(format string, args ...interface{}) {
	if logLevel <= WARNING {
		zap.S().Warnf(format, args...)
	}
}

func Error(format string, args ...interface{}) {
	if logLevel <= ERROR {
		zap.S().Errorf(format, args...)
	}
}

func Fatal(format string, args ...interface{}) {
	zap.S().Fatalf(format, args...)
}

func SetLevel(level Level) {
	logLevel = level
}

type Level int

const (
	DEBUG Level = iota
	INFO
	WARNING
	ERROR
	FATAL
)

var logLevel Level
