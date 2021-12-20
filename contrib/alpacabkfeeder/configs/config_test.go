package configs_test

import (
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/alpacahq/marketstore/v4/contrib/alpacabkfeeder/configs"
)

var testConfig = map[string]interface{}{
	"api_key_id":     "hello",
	"api_secret_key": "world",
	"update_time":    "12:34:56",
	"exchanges":      []string{"foo"},
	"index_groups":   []string{"bar"},
}

func TestNewConfig(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		config  map[string]interface{}
		envVars map[string]string
		want    *configs.DefaultConfig
		wantErr bool
	}{
		"ok/ API key ID, API secret key and UpdateTime can be overridden by env vars": {
			config: testConfig,
			envVars: map[string]string{
				"ALPACA_BROKER_FEEDER_API_KEY_ID":     "yo",
				"ALPACA_BROKER_FEEDER_API_SECRET_KEY": "yoyo",
				"ALPACA_BROKER_FEEDER_UPDATE_TIME":    "20:00:00",
			},
			want: &configs.DefaultConfig{
				Exchanges:           []string{"foo"},
				ClosedDaysOfTheWeek: []time.Weekday{time.Sunday},
				ClosedDays:          []time.Time{time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)},
				UpdateTime:          time.Date(0, 1, 1, 20, 0, 0, 0, time.UTC),
				APIKeyID:            "yo",
				APISecretKey:        "yoyo",
			},
			wantErr: false,
		},
		"ok/ nothing is overidden when env vars are empty": {
			config:  testConfig,
			envVars: map[string]string{},
			want: &configs.DefaultConfig{
				Exchanges:           []string{"foo"},
				ClosedDaysOfTheWeek: []time.Weekday{time.Sunday},
				ClosedDays:          []time.Time{time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)},
				UpdateTime:          time.Date(0, 1, 1, 12, 34, 56, 0, time.UTC),
				APIKeyID:            "hello",
				APISecretKey:        "world",
			},
			wantErr: false,
		},
	}
	for name, tt := range tests {
		tt := tt

		t.Run(name, func(t *testing.T) {
			// avoid env vars being used by multiple tests in parallel
			//t.Parallel()

			// --- given ---
			for key, value := range tt.envVars {
				_ = os.Setenv(key, value)
			}

			// --- when ---
			got, err := configs.NewConfig(tt.config)

			// --- then ---
			if (err != nil) != tt.wantErr {
				t.Errorf("NewConfig() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewConfig() \ngot = %v,\nwant %v", got, tt.want)
			}

			// --- shutDown ---
			for key := range tt.envVars {
				_ = os.Unsetenv(key)
			}
		})
	}
}
