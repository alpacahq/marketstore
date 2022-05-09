package log

import (
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// nolint:gochecknoinits // TODO: refactor
func init() {
	atom := zap.NewAtomicLevelAt(zapcore.DebugLevel)

	encoderCfg := zap.NewProductionEncoderConfig()
	encoderCfg.TimeKey = "timestamp"
	encoderCfg.EncodeTime = zapcore.ISO8601TimeEncoder

	logger := zap.New(zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderCfg),
		zapcore.Lock(os.Stdout),
		atom,
	))

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
