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

const (
	// 2^20 = 1048576.
	megabyteToByte                     = 1 << 20
	defaultReplicationMasterListenPort = 5996
	defaultWALRotateInterval           = 5 // * DiskRefreshInterval
)

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
		WALRotateInterval:          defaultWALRotateInterval,
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
			ListenPort: defaultReplicationMasterListenPort,
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
	var a aux
	if err := yaml.Unmarshal(data, &a); err != nil {
		return nil, err
	}

	absoluteRootDir, err := filepath.Abs(filepath.Clean(a.RootDirectory))
	if a.RootDirectory == "" || err != nil {
		return nil, fmt.Errorf("invalid root directory. rootDir=%s: %w", a.RootDirectory, err)
	}
	m := NewDefaultConfig(absoluteRootDir)

	if a.ListenPort == "" {
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
	if a.GRPCMaxSendMsgSize != 0 {
		m.GRPCMaxSendMsgSize = a.GRPCMaxSendMsgSize * megabyteToByte
		if a.GRPCMaxSendMsgSize < recommendedMinGRPCSendMsgSize {
			log.Warn("WARNING: Low grpc_max_send_msg_size: %dMB (recommend at least 64MB)", a.GRPCMaxSendMsgSize)
		}
	}

	if a.GRPCMaxRecvMsgSize != 0 {
		m.GRPCMaxRecvMsgSize = a.GRPCMaxRecvMsgSize * megabyteToByte
		if a.GRPCMaxRecvMsgSize < recommendedMinGRPCRecvMsgSize {
			log.Warn("WARNING: Low grpc_max_recv_msg_size: %dMB (recommend at least 64MB)", a.GRPCMaxRecvMsgSize)
		}
	}

	// Giving "" to LoadLocation will be UTC anyway, which is our default too.
	m.Timezone, err = time.LoadLocation(a.Timezone)
	if err != nil {
		return nil, fmt.Errorf("invalid timezone:%s", a.Timezone)
	}

	if a.WALRotateInterval != 0 {
		m.WALRotateInterval = a.WALRotateInterval
	}

	if a.LogLevel != "" {
		switch strings.ToLower(a.LogLevel) {
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

	if a.StopGracePeriod > 0 {
		m.StopGracePeriod = time.Duration(a.StopGracePeriod) * time.Second
	}

	if a.DisableVariableCompression != "" {
		m.DisableVariableCompression, err = strconv.ParseBool(a.DisableVariableCompression)
		if err != nil {
			return nil, fmt.Errorf("invalid value for DisableVariableCompression: %w", err)
		}
	}

	if a.InitCatalog != "" {
		m.InitCatalog, err = strconv.ParseBool(a.InitCatalog)
		if err != nil {
			return nil, fmt.Errorf("invalid value for InitCatalog: %w", err)
		}
	}

	if a.InitWALCache != "" {
		m.InitWALCache, err = strconv.ParseBool(a.InitWALCache)
		if err != nil {
			return nil, fmt.Errorf("invalid value for InitWALCache: %w", err)
		}
	}

	if a.BackgroundSync != "" {
		m.BackgroundSync, err = strconv.ParseBool(a.BackgroundSync)
		if err != nil {
			return nil, fmt.Errorf("invalid value for BackgroundSync: %w", err)
		}
	}

	if a.WALBypass != "" {
		m.WALBypass, err = strconv.ParseBool(a.WALBypass)
		if err != nil {
			return nil, fmt.Errorf("invalid value for WALBypass: %w", err)
		}
	}

	if a.Replication.ListenPort != 0 {
		m.Replication.ListenPort = a.Replication.ListenPort
	}

	m.Replication.Enabled = a.Replication.Enabled
	m.Replication.TLSEnabled = a.Replication.TLSEnabled
	m.Replication.CertFile = a.Replication.CertFile
	m.Replication.KeyFile = a.Replication.KeyFile
	m.Replication.MasterHost = a.Replication.MasterHost
	if a.Replication.RetryInterval != 0 {
		m.Replication.RetryInterval = a.Replication.RetryInterval
	}

	if a.Replication.RetryBackoffCoeff != 0 {
		m.Replication.RetryBackoffCoeff = a.Replication.RetryBackoffCoeff
	}

	m.ListenURL = fmt.Sprintf("%v:%v", a.ListenHost, a.ListenPort)
	if a.GRPCListenPort != "" {
		m.GRPCListenURL = fmt.Sprintf("%v:%v", a.ListenHost, a.GRPCListenPort)
	}
	m.UtilitiesURL = a.UtilitiesURL

	for _, trig := range a.Triggers {
		triggerSetting := &TriggerSetting{
			Module: trig.Module,
			On:     trig.On,
			Config: trig.Config,
		}
		m.Triggers = append(m.Triggers, triggerSetting)
	}

	for _, bg := range a.BgWorkers {
		bgWorkerSetting := &BgWorkerSetting{
			Module: bg.Module,
			Name:   bg.Name,
			Config: bg.Config,
		}
		m.BgWorkers = append(m.BgWorkers, bgWorkerSetting)
	}

	return m, nil
}
