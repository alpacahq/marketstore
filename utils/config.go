package utils

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/alpacahq/marketstore/utils/log"
	"gopkg.in/yaml.v2"
)

var InstanceConfig MktsConfig

func init() {
	InstanceConfig.Timezone = time.UTC
}

type TriggerSetting struct {
	Module string
	On     string
	Config map[string]interface{}
}

type BgWorkerSetting struct {
	Module string
	Name   string
	Config map[string]interface{}
}

type MktsConfig struct {
	RootDirectory     string
	ListenPort        string
	Timezone          *time.Location
	Queryable         bool
	StopGracePeriod   time.Duration
	WALRotateInterval int
	EnableAdd         bool
	EnableRemove      bool
	EnableLastKnown   bool
	StartTime         time.Time
	Triggers          []*TriggerSetting
	BgWorkers         []*BgWorkerSetting
}

func (m *MktsConfig) Parse(data []byte) error {
	var (
		err error
		aux struct {
			RootDirectory     string `yaml:"root_directory"`
			ListenPort        string `yaml:"listen_port"`
			Timezone          string `yaml:"timezone"`
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
			BgWorkers []struct {
				Module string                 `yaml:"module"`
				Name   string                 `yaml:"name"`
				Config map[string]interface{} `yaml:"config"`
			} `yaml:"bgworkers"`
		}
	)

	if err := yaml.Unmarshal(data, &aux); err != nil {
		return err
	}

	if aux.RootDirectory == "" {
		log.Fatal("Invalid root directory.")
		return errors.New("Invalid root directory.")
	}

	if aux.ListenPort == "" {
		log.Fatal("Invalid listen port.")
		return errors.New("Invalid listen port.")
	}

	// Giving "" to LoadLocation will be UTC anyway, which is our default too.
	m.Timezone, err = time.LoadLocation(aux.Timezone)
	if err != nil {
		log.Fatal("Invalid timezone.")
		return errors.New("Invalid timezone")
	}

	if aux.WALRotateInterval == 0 {
		m.WALRotateInterval = 5 // Default of rotate interval of five periods
	} else {
		m.WALRotateInterval = aux.WALRotateInterval
	}

	if aux.Queryable != "" {
		queryable, err := strconv.ParseBool(aux.Queryable)
		if err != nil {
			log.Error("Invalid value: %v for Queryable. Running as queryable...", aux.Queryable)
		} else {
			m.Queryable = queryable
		}
	}

	if aux.LogLevel != "" {
		switch strings.ToLower(aux.LogLevel) {
		case "fatal":
			log.SetLevel(log.FATAL)
		case "error":
			log.SetLevel(log.ERROR)
		case "warning":
			log.SetLevel(log.WARNING)
		case "debug":
			log.SetLevel(log.DEBUG)
		case "info":
			fallthrough
		default:
			log.SetLevel(log.INFO)
		}
	}

	if aux.StopGracePeriod > 0 {
		m.StopGracePeriod = time.Duration(aux.StopGracePeriod) * time.Second
	}

	if aux.EnableAdd != "" {
		enableAdd, err := strconv.ParseBool(aux.EnableAdd)
		if err != nil {
			log.Error("Invalid value: %v for enable_add. Disabling add...", aux.EnableAdd)
		} else {
			m.EnableAdd = enableAdd
		}
	}

	if aux.EnableRemove != "" {
		enableRemove, err := strconv.ParseBool(aux.EnableRemove)
		if err != nil {
			log.Error("Invalid value: %v for enable_add. Disabling remove...", aux.EnableRemove)
		} else {
			m.EnableRemove = enableRemove
		}
	}

	m.EnableLastKnown = false
	log.Info("Disabling \"enable_last_known\" feature until it is fixed...")
	/*
		// Broken - disable for now
		if aux.EnableLastKnown != "" {
			enableLastKnown, err := strconv.ParseBool(aux.EnableLastKnown)
			if err != nil {
				log.Error("Invalid value: %v for enable_last_known.  Disabling lastKnown...", aux.EnableLastKnown)
			} else {
				m.EnableLastKnown = enableLastKnown
			}
		}
	*/
	m.RootDirectory = aux.RootDirectory
	m.ListenPort = fmt.Sprintf(":%v", aux.ListenPort)

	for _, trig := range aux.Triggers {
		triggerSetting := &TriggerSetting{
			Module: trig.Module,
			On:     trig.On,
			Config: trig.Config,
		}
		m.Triggers = append(m.Triggers, triggerSetting)
	}

	for _, bg := range aux.BgWorkers {
		bgWorkerSetting := &BgWorkerSetting{
			Module: bg.Module,
			Name:   bg.Name,
			Config: bg.Config,
		}
		m.BgWorkers = append(m.BgWorkers, bgWorkerSetting)
	}

	return err
}
