package configs

import (
	"fmt"
	"time"

	"github.com/kelseyhightower/envconfig"
)

// EnvConfig is a struct that allows only certain settings to be overridden by environment variables
// in order to flexibly re-run processes that are performed only at marketstore start-up/certain times of the day,
// or strings that we do not want to directly write in the configuration file for security reasons.
type EnvConfig struct {
	APIToken   string `envconfig:"API_TOKEN"`
	UpdateTime string `envconfig:"UPDATE_TIME"`
}

// envOverride updates some configs by environment variables.
func envOverride(config *DefaultConfig) (*DefaultConfig, error) {
	var env EnvConfig
	err := envconfig.Process("XIGNITE_FEEDER", &env)
	if err != nil {
		return nil, fmt.Errorf("failed to read env variables for Xignite Feeder plugin: %w", err)
	}

	// override UpdateTime
	if env.UpdateTime != "" {
		t, err := time.Parse(ctLayout, env.UpdateTime)
		if err != nil {
			return nil, err
		}
		config.UpdateTime = t
	}

	// override APIToken
	if env.APIToken != "" {
		config.APIToken = env.APIToken
	}

	return config, nil
}
