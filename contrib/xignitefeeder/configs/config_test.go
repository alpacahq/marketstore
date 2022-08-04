package configs_test

import (
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/alpacahq/marketstore/v4/contrib/xignitefeeder/configs"
)

var testConfig = map[string]interface{}{
	"token":                "hellohellohellohellohellohello12",
	"update_time":          "12:34:56",
	"exchanges":            []string{"foo"},
	"index_groups":         []string{"bar"},
	"not_quote_stock_list": []string{"1111"},
}

func TestNewConfig(t *testing.T) {
	// avoid t.Parallel() as env vars are used.

	tests := map[string]struct {
		config  map[string]interface{}
		envVars map[string]string
		want    *configs.DefaultConfig
		wantErr bool
	}{
		"ok/ API token, UpdateTime, and NotQuoteSymbolList can be overridden by env vars": {
			config: testConfig,
			envVars: map[string]string{
				"XIGNITE_FEEDER_API_TOKEN":            "ABCDEFGHIJKLMNOPQRSTUVWXYZ789012",
				"XIGNITE_FEEDER_UPDATE_TIME":          "20:00:00",
				"XIGNITE_FEEDER_NOT_QUOTE_STOCK_LIST": "1234,5678,9012",
			},
			want: &configs.DefaultConfig{
				Exchanges:           []string{"foo"},
				IndexGroups:         []string{"bar"},
				NotQuoteStockList:   []string{"1234", "5678", "9012"},
				ClosedDaysOfTheWeek: []time.Weekday{},
				ClosedDays:          []time.Time{},
				UpdateTime:          time.Date(0, 1, 1, 20, 0, 0, 0, time.UTC),
				APIToken:            "ABCDEFGHIJKLMNOPQRSTUVWXYZ789012",
			},
			wantErr: false,
		},
		"ng/ Token length must be 32 bytes": {
			config: testConfig,
			envVars: map[string]string{
				"XIGNITE_FEEDER_API_TOKEN": "ABCDE", // 5 bytes
			},
			want:    nil,
			wantErr: true,
		},
		"ok/ nothing is overidden when env vars are empty": {
			config:  testConfig,
			envVars: map[string]string{},
			want: &configs.DefaultConfig{
				Exchanges:           []string{"foo"},
				IndexGroups:         []string{"bar"},
				NotQuoteStockList:   []string{"1111"},
				ClosedDaysOfTheWeek: []time.Weekday{},
				ClosedDays:          []time.Time{},
				UpdateTime:          time.Date(0, 1, 1, 12, 34, 56, 0, time.UTC),
				APIToken:            "hellohellohellohellohellohello12",
			},
			wantErr: false,
		},
	}
	for name, tt := range tests {
		tt := tt

		t.Run(name, func(t *testing.T) {
			// t.Parallel()

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
