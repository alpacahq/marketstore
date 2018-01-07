package log

import (
	"runtime/debug"

	"github.com/golang/glog"
)

func Log(level Level, format string, args ...interface{}) {
	switch level {
	default:
	case INFO:
		if logLevel >= INFO {
			glog.Infof(format, args...)
		}
	case WARNING:
		if logLevel >= WARNING {
			glog.Warningf(format, args...)
		}
	case ERROR:
		if logLevel >= ERROR {
			glog.Errorf(format, args...)
			debug.PrintStack()
		}
	case FATAL:
		glog.Fatalf(format, args...)
		debug.PrintStack()
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
