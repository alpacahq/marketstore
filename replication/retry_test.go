package replication_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alpacahq/marketstore/v4/replication"
)

// Retryer succeeds at a certain trial.
type retryer struct {
	Count     int
	SucceedAt int
}

func (r *retryer) try(_ context.Context) error {
	r.Count++
	if r.Count == r.SucceedAt {
		return nil
	}
	return replication.ErrRetryable
}

func TestRetryer_Run(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		retryFunc func(ctx context.Context) error
		interval  time.Duration
		context   context.Context
		wantErr   bool
	}{
		{
			name:      "success",
			retryFunc: func(ctx context.Context) error { return nil },
			context:   context.Background(),
			wantErr:   false,
		},
		{
			name:      "not retryable error",
			retryFunc: func(ctx context.Context) error { return errors.New("some error") },
			context:   context.Background(),
			wantErr:   true,
		},
		{
			name:      "retryable error",
			retryFunc: func(ctx context.Context) error { return replication.ErrRetryable },
			context:   context.Background(),
			wantErr:   true,
		},
		{
			name: "succeed at the 3rd try",
			retryFunc: func() func(ctx context.Context) error {
				r := retryer{SucceedAt: 3}
				return r.try
			}(),
			context: context.Background(),
			wantErr: false,
		},
		{
			name: "don't retry if context is canceled",
			retryFunc: func(ctx context.Context) error {
				return replication.ErrRetryable
			},
			context: func() context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx // already canceled context is passed
			}(),
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// --- given ---
			r := replication.NewRetryer(tt.retryFunc, 10*time.Millisecond, 2)

			// --- when ---
			err := r.Run(tt.context)

			// --- then ---
			if (err != nil) != tt.wantErr {
				t.Errorf("Run() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
