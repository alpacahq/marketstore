package utils

import (
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v2"

	"github.com/alpacahq/marketstore/v4/utils/log"
)

var InstanceConfig MktsConfig

func init() {
	InstanceConfig.Timezone = time.UTC
}

type ReplicationSetting struct {
	Enabled           bool
	TLSEnabled        bool
	CertFile          string
	KeyFile           string
	ListenPort        int
	MasterHost        string
	RetryInterval     time.Duration
	RetryBackoffCoeff int
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
	// RootDirectory is the absolute path to the data directory
	RootDirectory              string
	ListenURL                  string
	GRPCListenURL              string
	GRPCMaxSendMsgSize         int // in bytes
	GRPCMaxRecvMsgSize         int // in bytes
	UtilitiesURL               string
	Timezone                   *time.Location
	StopGracePeriod            time.Duration
	WALRotateInterval          int
	DisableVariableCompression bool
	InitCatalog                bool
	InitWALCache               bool
	BackgroundSync             bool
	WALBypass                  bool
	StartTime                  time.Time
	Replication                ReplicationSetting
	Triggers                   []*TriggerSetting
	BgWorkers                  []*BgWorkerSetting
}

// 2^20 = 1048576.
const megabyteToByte = 1 << 20

func NewDefaultConfig(rootDir string) *MktsConfig {
	return &MktsConfig{
		RootDirectory:              rootDir,
		ListenURL:                  "",
		GRPCListenURL:              "",
		GRPCMaxSendMsgSize:         1024 * megabyteToByte, // 1024MB
		GRPCMaxRecvMsgSize:         1024 * megabyteToByte, // 1024MB
		UtilitiesURL:               "",
		Timezone:                   time.UTC,
		StopGracePeriod:            0,
		WALRotateInterval:          5, // Default of rotate interval of five periods
		DisableVariableCompression: false,
		InitCatalog:                true,
		InitWALCache:               true,
		BackgroundSync:             true,
		WALBypass:                  false,
		StartTime:                  time.Now(),
		Replication: ReplicationSetting{
			Enabled:    false,
			TLSEnabled: false,
			CertFile:   "",
			KeyFile:    "",
			ListenPort: 5996, // default listen port for Replication master
			MasterHost: "",
			// default retry intervals are 10s -> 20s -> 40s -> ...
			RetryInterval:     10 * time.Second,
			RetryBackoffCoeff: 2,
		},
		Triggers:  nil,
		BgWorkers: nil,
	}
}

type aux struct {
	// RootDirectory can be either a relative or absolute path
	RootDirectory              string `yaml:"root_directory"`
	ListenHost                 string `yaml:"listen_host"`
	ListenPort                 string `yaml:"listen_port"`
	GRPCListenPort             string `yaml:"grpc_listen_port"`
	GRPCMaxSendMsgSize         int    `yaml:"grpc_max_send_msg_size"` // in MB
	GRPCMaxRecvMsgSize         int    `yaml:"grpc_max_recv_msg_size"` // in MB
	UtilitiesURL               string `yaml:"utilities_url"`
	Timezone                   string `yaml:"timezone"`
	LogLevel                   string `yaml:"log_level"`
	StopGracePeriod            int    `yaml:"stop_grace_period"`
	WALRotateInterval          int    `yaml:"wal_rotate_interval"`
	DisableVariableCompression string `yaml:"disable_variable_compression"`
	InitCatalog                string `yaml:"init_catalog"`
	InitWALCache               string `yaml:"init_wal_cache"`
	BackgroundSync             string `yaml:"background_sync"`
	WALBypass                  string `yaml:"wal_bypass"`
	Replication                struct {
		Enabled    bool   `yaml:"enabled"`
		TLSEnabled bool   `yaml:"tls_enabled"`
		CertFile   string `yaml:"cert_file"`
		KeyFile    string `yaml:"key_file"`
		// ListenPort is used for the replication protocol by the master instance
		ListenPort        int           `yaml:"listen_port"`
		MasterHost        string        `yaml:"master_host"`
		RetryInterval     time.Duration `yaml:"retry_interval"`
		RetryBackoffCoeff int           `yaml:"retry_backoff_coeff"`
	} `yaml:"replication"`
	Triggers []struct {
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

func ParseConfig(data []byte) (*MktsConfig, error) {
	var aux aux
	if err := yaml.Unmarshal(data, &aux); err != nil {
		return nil, err
	}

	absoluteRootDir, err := filepath.Abs(filepath.Clean(aux.RootDirectory))
	if aux.RootDirectory == "" || err != nil {
		return nil, fmt.Errorf("invalid root directory. rootDir=%s: %w", aux.RootDirectory, err)
	}
	m := NewDefaultConfig(absoluteRootDir)

	if aux.ListenPort == "" {
		return nil, errors.New("invalid listen port. Listen port can't be empty")
	}

	// GRPC is optional for now
	// if aux.GRPCListenPort == "" {
	// 	log.Error("Invalid GRPC listen port.")
	// 	return errors.New("Invalid GRPC listen port.")
	// }
	const (
		recommendedMinGRPCSendMsgSize = 64
		recommendedMinGRPCRecvMsgSize = 64
	)
	if aux.GRPCMaxSendMsgSize != 0 {
		m.GRPCMaxSendMsgSize = aux.GRPCMaxSendMsgSize * megabyteToByte
		if aux.GRPCMaxSendMsgSize < recommendedMinGRPCSendMsgSize {
			log.Warn("WARNING: Low grpc_max_send_msg_size: %dMB (recommend at least 64MB)", aux.GRPCMaxSendMsgSize)
		}
	}

	if aux.GRPCMaxRecvMsgSize != 0 {
		m.GRPCMaxRecvMsgSize = aux.GRPCMaxRecvMsgSize * megabyteToByte
		if aux.GRPCMaxRecvMsgSize < recommendedMinGRPCRecvMsgSize {
			log.Warn("WARNING: Low grpc_max_recv_msg_size: %dMB (recommend at least 64MB)", aux.GRPCMaxRecvMsgSize)
		}
	}

	// Giving "" to LoadLocation will be UTC anyway, which is our default too.
	m.Timezone, err = time.LoadLocation(aux.Timezone)
	if err != nil {
		return nil, fmt.Errorf("invalid timezone:%s", aux.Timezone)
	}

	if aux.WALRotateInterval != 0 {
		m.WALRotateInterval = aux.WALRotateInterval
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
		default: // case "info":
			log.SetLevel(log.INFO)
		}
	}

	if aux.StopGracePeriod > 0 {
		m.StopGracePeriod = time.Duration(aux.StopGracePeriod) * time.Second
	}

	if aux.DisableVariableCompression != "" {
		m.DisableVariableCompression, err = strconv.ParseBool(aux.DisableVariableCompression)
		if err != nil {
			return nil, fmt.Errorf("invalid value for DisableVariableCompression: %w", err)
		}
	}

	if aux.InitCatalog != "" {
		m.InitCatalog, err = strconv.ParseBool(aux.InitCatalog)
		if err != nil {
			return nil, fmt.Errorf("invalid value for InitCatalog: %w", err)
		}
	}

	if aux.InitWALCache != "" {
		m.InitWALCache, err = strconv.ParseBool(aux.InitWALCache)
		if err != nil {
			return nil, fmt.Errorf("invalid value for InitWALCache: %w", err)
		}
	}

	if aux.BackgroundSync != "" {
		m.BackgroundSync, err = strconv.ParseBool(aux.BackgroundSync)
		if err != nil {
			return nil, fmt.Errorf("invalid value for BackgroundSync: %w", err)
		}
	}

	if aux.WALBypass != "" {
		m.WALBypass, err = strconv.ParseBool(aux.WALBypass)
		if err != nil {
			return nil, fmt.Errorf("invalid value for WALBypass: %w", err)
		}
	}

	if aux.Replication.ListenPort != 0 {
		m.Replication.ListenPort = aux.Replication.ListenPort
	}

	m.Replication.Enabled = aux.Replication.Enabled
	m.Replication.TLSEnabled = aux.Replication.TLSEnabled
	m.Replication.CertFile = aux.Replication.CertFile
	m.Replication.KeyFile = aux.Replication.KeyFile
	m.Replication.MasterHost = aux.Replication.MasterHost
	if aux.Replication.RetryInterval != 0 {
		m.Replication.RetryInterval = aux.Replication.RetryInterval
	}

	if aux.Replication.RetryBackoffCoeff != 0 {
		m.Replication.RetryBackoffCoeff = aux.Replication.RetryBackoffCoeff
	}

	m.ListenURL = fmt.Sprintf("%v:%v", aux.ListenHost, aux.ListenPort)
	if aux.GRPCListenPort != "" {
		m.GRPCListenURL = fmt.Sprintf("%v:%v", aux.ListenHost, aux.GRPCListenPort)
	}
	m.UtilitiesURL = aux.UtilitiesURL

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

	return m, nil
}
