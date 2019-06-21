package utils

import (
	"errors"

	. "github.com/alpacahq/slait/utils/log"

	yaml "gopkg.in/yaml.v2"
)

var Sha1hash string
var Version string = "dev"

type TrimPlan struct {
	TopicMatch string `yaml:"topic"`
	Duration   string `yaml:"duration"`
}

type SlaitConfig struct {
	ListenPort string     `yaml:"listen_port"`
	LogLevel   string     `yaml:"log_level"`
	DataDir    string     `yaml:"data_dir"`
	TrimConfig []TrimPlan `yaml:"trim_config"`
}

func ParseConfig(data []byte) (err error) {
	if err = yaml.Unmarshal(data, &GlobalConfig); err != nil {
		return err
	}
	if GlobalConfig.ListenPort == "" {
		errMsg := "Invalid listen port."
		Log(FATAL, errMsg)
		return errors.New(errMsg)
	}
	switch GlobalConfig.LogLevel {
	case "info":
		SetLogLevel(INFO)
	case "warning":
		SetLogLevel(WARNING)
	case "error":
		fallthrough
	default:
		SetLogLevel(ERROR)
	}

	return err
}

var GlobalConfig SlaitConfig
