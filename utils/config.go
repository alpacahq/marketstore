package utils

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"gopkg.in/yaml.v2"

	. "github.com/alpacahq/marketstore/utils/log"
)

var InstanceConfig MktsConfig

type MktsConfig struct {
	RootDirectory     string
	ListenPort        string
	Queryable         bool
	StopGracePeriod   time.Duration
	WALRotateInterval int
	EnableAdd         bool
	EnableRemove      bool
	EnableLastKnown   bool
	StartTime         time.Time
	Triggers          []TriggerSetting
}

func (m *MktsConfig) Parse(data []byte) error {
	var err error
	var aux struct {
		RootDirectory     string `yaml:"root_directory"`
		ListenPort        string `yaml:"listen_port"`
		LogLevel          string `yaml:"log_level"`
		Queryable         string `yaml:"queryable"`
		StopGracePeriod   int    `yaml:"stop_grace_period"`
		WALRotateInterval int    `yaml:"wal_rotate_interval"`
		EnableAdd         string `yaml:"enable_add"`
		EnableRemove      string `yaml:"enable_remove"`
		EnableLastKnown   string `yaml:"enable_last_known"`
		Triggers          []struct {
			Module string                 `yaml:"module"`
			On     string                 `yaml:"on"`
			Config map[string]interface{} `yaml:"config"`
		} `yaml:"triggers"`
	}

	if err := yaml.Unmarshal(data, &aux); err != nil {
		return err
	}
	if aux.RootDirectory == "" {
		Log(FATAL, "Invalid root directory.")
		return errors.New("Invalid root directory.")
	}
	if aux.ListenPort == "" {
		Log(FATAL, "Invalid listen port.")
		return errors.New("Invalid listen port.")
	}

	if aux.WALRotateInterval == 0 {
		m.WALRotateInterval = 5 // Default of rotate interval of five periods
	} else {
		m.WALRotateInterval = aux.WALRotateInterval
	}
	if aux.Queryable != "" {
		queryable, err := strconv.ParseBool(aux.Queryable)
		if err != nil {
			Log(ERROR, "Invalid value: %v for Queryable. Running as queryable...")
		} else {
			m.Queryable = queryable
		}
	}
	if aux.LogLevel != "" {
		switch aux.LogLevel {
		case "error":
			SetLogLevel(ERROR)
		case "warning":
			SetLogLevel(WARNING)
		case "info":
			SetLogLevel(INFO)
		}
	} else {
		SetLogLevel(INFO)
	}
	if aux.StopGracePeriod > 0 {
		m.StopGracePeriod = time.Duration(aux.StopGracePeriod) * time.Second
	}
	if aux.EnableAdd != "" {
		enableAdd, err := strconv.ParseBool(aux.EnableAdd)
		if err != nil {
			Log(ERROR, "Invalid value: %v for enable_add. Disabling add...", aux.EnableAdd)
		} else {
			m.EnableAdd = enableAdd
		}
	}
	if aux.EnableRemove != "" {
		enableRemove, err := strconv.ParseBool(aux.EnableRemove)
		if err != nil {
			Log(ERROR, "Invalid value: %v for enable_add. Disabling remove...", aux.EnableRemove)
		} else {
			m.EnableRemove = enableRemove
		}
	}
	m.EnableLastKnown = false
	Log(INFO, "Disabling \"enable_last_known\" feature until it is fixed...")
	/*
		// Broken - disable for now
		if aux.EnableLastKnown != "" {
			enableLastKnown, err := strconv.ParseBool(aux.EnableLastKnown)
			if err != nil {
				Log(ERROR, "Invalid value: %v for enable_last_known.  Disabling lastKnown...", aux.EnableLastKnown)
			} else {
				m.EnableLastKnown = enableLastKnown
			}
		}
	*/
	m.RootDirectory = aux.RootDirectory
	m.ListenPort = fmt.Sprintf(":%v", aux.ListenPort)

	for _, trig := range aux.Triggers {
		triggerSetting := TriggerSetting{
			Module: trig.Module,
			On:     trig.On,
			Config: trig.Config,
		}
		m.Triggers = append(m.Triggers, triggerSetting)
	}
	return err
}
