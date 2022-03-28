package configs_test

import (
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/alpacahq/marketstore/v4/contrib/alpacabkfeeder/configs"
)

var testConfig = map[string]interface{}{
	"api_key_id":             "hello",
	"api_secret_key":         "world",
	"symbols_update_time":    "01:23:45",
	"update_time":            "12:34:56",
	"stocks_json_basic_auth": "foo:bar",
	"exchanges":              []string{"AMEX", "ARCA", "BATS", "NYSE", "NASDAQ", "NYSEARCA", "OTC"},
	"index_groups":           []string{"bar"},
}

var configWithInvalidExchange = map[string]interface{}{
	"exchanges": []string{"invalid_exchange"},
}

func TestNewConfig(t *testing.T) {
	// avoid t.Parallel() as env vars are used.

	tests := map[string]struct {
		config  map[string]interface{}
		envVars map[string]string
		want    *configs.DefaultConfig
		wantErr bool
	}{
		"ok/ API key ID, API secret key, UpdateTime, basicAuth can be overridden by env vars": {
			config: testConfig,
			envVars: map[string]string{
				"ALPACA_BROKER_FEEDER_API_KEY_ID":             "yo",
				"ALPACA_BROKER_FEEDER_API_SECRET_KEY":         "yoyo",
				"ALPACA_BROKER_FEEDER_SYMBOLS_UPDATE_TIME":    "10:00:00",
				"ALPACA_BROKER_FEEDER_UPDATE_TIME":            "20:00:00",
				"ALPACA_BROKER_FEEDER_STOCKS_JSON_BASIC_AUTH": "akkie:mypassword",
			},
			want: &configs.DefaultConfig{
				Exchanges: []configs.Exchange{
					configs.AMEX, configs.ARCA, configs.BATS, configs.NYSE,
					configs.NASDAQ, configs.NYSEARCA, configs.OTC,
				},
				ClosedDaysOfTheWeek: []time.Weekday{},
				ClosedDays:          []time.Time{},
				SymbolsUpdateTime:   time.Date(0, 1, 1, 10, 0, 0, 0, time.UTC),
				UpdateTime:          time.Date(0, 1, 1, 20, 0, 0, 0, time.UTC),
				APIKeyID:            "yo",
				APISecretKey:        "yoyo",
				StocksJSONBasicAuth: "akkie:mypassword",
			},
			wantErr: false,
		},
		"ok/ nothing is overidden when env vars are empty": {
			config:  testConfig,
			envVars: map[string]string{},
			want: &configs.DefaultConfig{
				Exchanges: []configs.Exchange{
					configs.AMEX, configs.ARCA, configs.BATS, configs.NYSE,
					configs.NASDAQ, configs.NYSEARCA, configs.OTC,
				},
				ClosedDaysOfTheWeek: []time.Weekday{},
				ClosedDays:          []time.Time{},
				SymbolsUpdateTime:   time.Date(0, 1, 1, 1, 23, 45, 0, time.UTC),
				UpdateTime:          time.Date(0, 1, 1, 12, 34, 56, 0, time.UTC),
				APIKeyID:            "hello",
				APISecretKey:        "world",
				StocksJSONBasicAuth: "foo:bar",
			},
			wantErr: false,
		},
		"ng/ error when unknown exchange name is provided": {
			config:  configWithInvalidExchange,
			envVars: map[string]string{},
			want:    nil,
			wantErr: true,
		},
	}
	for name, tt := range tests {
		tt := tt

		t.Run(name, func(t *testing.T) {
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
