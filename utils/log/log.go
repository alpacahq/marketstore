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

func Debug(msg string, args ...interface{}) {
	if logLevel <= DEBUG {
		if len(args) > 0 {
			zap.S().Debugf(msg, args...)
		} else {
			zap.S().Debug(msg)
		}
	}
}

func Info(msg string, args ...interface{}) {
	if logLevel <= INFO {
		if len(args) > 0 {
			zap.S().Infof(msg, args...)
		} else {
			zap.S().Info(msg)
		}
	}
}

func Warn(msg string, args ...interface{}) {
	if logLevel <= WARNING {
		if len(args) > 0 {
			zap.S().Warnf(msg, args...)
		} else {
			zap.S().Warn(msg)
		}
	}
}

func Error(msg string, args ...interface{}) {
	if logLevel <= ERROR {
		if len(args) > 0 {
			zap.S().Errorf(msg, args...)
		} else {
			zap.S().Error(msg)
		}
	}
}

func Fatal(msg string, args ...interface{}) {
	if len(args) > 0 {
		zap.S().Fatalf(msg, args...)
	} else {
		zap.S().Fatal(msg)
	}
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
