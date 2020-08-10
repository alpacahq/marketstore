package utils

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/alpacahq/marketstore/v4/utils/log"
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
	RootDirectory              string
	ListenURL                  string
	GRPCListenURL              string
	GRPCMaxSendMsgSize         int // in bytes
	GRPCMaxRecvMsgSize         int // in bytes
	UtilitiesURL               string
	Timezone                   *time.Location
	Queryable                  bool
	StopGracePeriod            time.Duration
	WALRotateInterval          int
	EnableAdd                  bool
	EnableRemove               bool
	EnableLastKnown            bool
	DisableVariableCompression bool
	InitCatalog                bool
	InitWALCache               bool
	BackgroundSync             bool
	WALBypass                  bool
	ClusterMode                bool
	StartTime                  time.Time
	Triggers                   []*TriggerSetting
	BgWorkers                  []*BgWorkerSetting
}

func (m *MktsConfig) Parse(data []byte) error {
	var (
		err error
		aux struct {
			RootDirectory              string `yaml:"root_directory"`
			ListenHost                 string `yaml:"listen_host"`
			ListenPort                 string `yaml:"listen_port"`
			GRPCListenPort             string `yaml:"grpc_listen_port"`
			GRPCMaxSendMsgSize         int    `yaml:"grpc_max_send_msg_size"` // in MB
			GRPCMaxRecvMsgSize         int    `yaml:"grpc_max_recv_msg_size"` // in MB
			UtilitiesURL               string `yaml:"utilities_url"`
			Timezone                   string `yaml:"timezone"`
			LogLevel                   string `yaml:"log_level"`
			Queryable                  string `yaml:"queryable"`
			StopGracePeriod            int    `yaml:"stop_grace_period"`
			WALRotateInterval          int    `yaml:"wal_rotate_interval"`
			EnableAdd                  string `yaml:"enable_add"`
			EnableRemove               string `yaml:"enable_remove"`
			EnableLastKnown            string `yaml:"enable_last_known"`
			DisableVariableCompression string `yaml:"disable_variable_compression"`
			InitCatalog                string `yaml:"init_catalog"`
			InitWALCache               string `yaml:"init_wal_cache"`
			BackgroundSync             string `yaml:"background_sync"`
			WALBypass                  string `yaml:"wal_bypass"`
			ClusterMode                string `yaml:"cluster_mode"`
			Triggers                   []struct {
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

	// GRPC is optional for now
	// if aux.GRPCListenPort == "" {
	// 	log.Fatal("Invalid GRPC listen port.")
	// 	return errors.New("Invalid GRPC listen port.")
	// }
	if aux.GRPCMaxSendMsgSize == 0 {
		aux.GRPCMaxSendMsgSize = 1024
	} else if aux.GRPCMaxSendMsgSize < 64 {
		log.Warn("WARNING: Low grpc_max_send_msg_size: %dMB (recommend at least 64MB)", aux.GRPCMaxSendMsgSize)
	}
	m.GRPCMaxSendMsgSize = aux.GRPCMaxSendMsgSize * (1 << 20)

	if aux.GRPCMaxRecvMsgSize == 0 {
		aux.GRPCMaxRecvMsgSize = 1024
	} else if aux.GRPCMaxRecvMsgSize < 64 {
		log.Warn("WARNING: Low grpc_max_recv_msg_size: %dMB (recommend at least 64MB)", aux.GRPCMaxRecvMsgSize)
	}
	m.GRPCMaxRecvMsgSize = aux.GRPCMaxRecvMsgSize * (1 << 20)

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

	if aux.DisableVariableCompression != "" {
		m.DisableVariableCompression, err = strconv.ParseBool(aux.DisableVariableCompression)
		if err != nil {
			log.Error("Invalid value for DisableVariableCompression")
		}
	}
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
	m.InitCatalog = true
	if aux.InitCatalog != "" {
		m.InitCatalog, err = strconv.ParseBool(aux.InitCatalog)
		if err != nil {
			log.Error("Invalid value for InitCatalog")
		}
	}

	m.InitWALCache = true
	if aux.InitWALCache != "" {
		m.InitWALCache, err = strconv.ParseBool(aux.InitWALCache)
		if err != nil {
			log.Error("Invalid value for InitWALCache")
		}
	}

	m.BackgroundSync = true
	if aux.BackgroundSync != "" {
		m.BackgroundSync, err = strconv.ParseBool(aux.BackgroundSync)
		if err != nil {
			log.Error("Invalid value for BackgroundSync")
		}
	}

	m.WALBypass = false
	if aux.WALBypass != "" {
		m.WALBypass, err = strconv.ParseBool(aux.WALBypass)
		if err != nil {
			log.Error("Invalid value for WALBypass")
		}
	}

	m.ClusterMode = true
	if aux.ClusterMode != "" {
		m.ClusterMode, err = strconv.ParseBool(aux.ClusterMode)
		if err != nil {
			log.Error("Invalid value for ClusterMode")
		}
	}

	m.RootDirectory = aux.RootDirectory
	m.ListenURL = fmt.Sprintf("%v:%v", aux.ListenHost, aux.ListenPort)
	if aux.GRPCListenPort != "" {
		m.GRPCListenURL = fmt.Sprintf("%v:%v", aux.ListenHost, aux.GRPCListenPort)
	}
	m.UtilitiesURL = fmt.Sprintf("%v", aux.UtilitiesURL)

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
