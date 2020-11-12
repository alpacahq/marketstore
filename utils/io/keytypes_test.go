package io_test

import (
	"fmt"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"reflect"
	"testing"
)

func TestNewTimeBucketKeyFromWalKeyPath(t *testing.T) {
	tests := []struct {
		name       string
		walKeyPath string
		wantTbk    *io.TimeBucketKey
		wantYear   int
		wantErr    bool
	}{
		{
			name:       "Success",
			walKeyPath: "/project/marketstore/data/AMZN/1Min/TICK/2017.bin",
			wantTbk: &io.TimeBucketKey{
				Key: fmt.Sprintf("AMZN/1Min/TICK:%s", io.DefaultTimeBucketSchema),
			},
			wantYear: 2017,
			wantErr:  false,
		},
		{
			name:       "Invalid format of WalKeyPath",
			walKeyPath: "/foor/bar/2017.bin",
			wantErr:  true,
		},
		{
			name:       "Invalid format of year",
			walKeyPath: "/project/marketstore/data/AMZN/1Min/TICK/InvalidYear.bin",
			wantErr:  true,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			gotTbk, gotYear, err := io.NewTimeBucketKeyFromWalKeyPath(tt.walKeyPath)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewTimeBucketKeyFromWalKeyPath() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotTbk, tt.wantTbk) {
				t.Errorf("NewTimeBucketKeyFromWalKeyPath() gotTbk = %v, want %v", gotTbk, tt.wantTbk)
			}
			if gotYear != tt.wantYear {
				t.Errorf("NewTimeBucketKeyFromWalKeyPath() gotYear = %v, want %v", gotYear, tt.wantYear)
			}
		})
	}
}
