package replication_test

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/pkg/errors"

	"github.com/alpacahq/marketstore/v4/replication"
)

type MockGRPCClient struct {
	ConnectFunc func(ctx context.Context) error
	RecvFunc    func() ([]byte, error)
}

func (mg *MockGRPCClient) Connect(ctx context.Context) error {
	return mg.ConnectFunc(ctx)
}

func (mg *MockGRPCClient) Recv() ([]byte, error) {
	time.Sleep(500 * time.Millisecond) // to simulate that actual Recv() func blocks until it receives a new message
	return mg.RecvFunc()
}

func TestNewReceiver(t *testing.T) {
	t.Parallel()
	// --- given ---
	mockService := MockGRPCClient{}
	// --- when ---
	got := replication.NewReceiver(&mockService, &MockReplayer{})
	// --- then ---
	if got == nil {
		t.Error("Receiver is not initialized")
	}
}

type MockReplayer struct {
	ReplayFunc   func(transactionGroup []byte) error
	ReplayCalled bool
}

func (mr *MockReplayer) Replay(transactionGroup []byte) error {
	mr.ReplayCalled = true
	return mr.ReplayFunc(transactionGroup)
}

func TestReceiver_Run(t *testing.T) {
	tests := []struct {
		name             string
		mockConnectFunc  func(ctx context.Context) error
		mockRecvFunc     func() ([]byte, error)
		mockReplayFunc   func(transactionGroup []byte) error
		wantErr          bool
		wantReplayCalled bool
	}{
		{
			name:             "gRPC connect error/ an error occurs and Run() fails",
			mockConnectFunc:  func(ctx context.Context) error { return errors.New("some error") },
			mockRecvFunc:     func() ([]byte, error) { return nil, nil },
			mockReplayFunc:   func(transactionGroup []byte) error { return nil },
			wantErr:          true,
			wantReplayCalled: false,
		},
		{
			name:             "gRPC receive EOF error/ goroutine is stopped and nothing is replayed",
			mockConnectFunc:  func(ctx context.Context) error { return nil },
			mockRecvFunc:     func() ([]byte, error) { return nil, io.EOF },
			mockReplayFunc:   func(transactionGroup []byte) error { return nil },
			wantErr:          true,
			wantReplayCalled: false,
		},
		{
			name:             "gRPC receive an error/ goroutine is stopped and nothing is replayed",
			mockConnectFunc:  func(ctx context.Context) error { return nil },
			mockRecvFunc:     func() ([]byte, error) { return nil, errors.New("some error") },
			mockReplayFunc:   func(transactionGroup []byte) error { return nil },
			wantErr:          true,
			wantReplayCalled: false,
		},
		{
			name:             "replay process error/ goroutine is stopped",
			mockConnectFunc:  func(ctx context.Context) error { return nil },
			mockRecvFunc:     func() ([]byte, error) { return nil, nil },
			mockReplayFunc:   func(transactionGroup []byte) error { return errors.New("some error") },
			wantErr:          true,
			wantReplayCalled: true,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			// --- given ---
			mockGRPCClient := &MockGRPCClient{
				ConnectFunc: tt.mockConnectFunc,
				RecvFunc:    tt.mockRecvFunc,
			}
			mockReplayer := &MockReplayer{
				ReplayFunc: tt.mockReplayFunc,
			}
			r := replication.NewReceiver(mockGRPCClient, mockReplayer)

			// --- when ---
			err := r.Run(context.Background())

			// --- then ---
			if (err != nil) != tt.wantErr {
				t.Errorf("Run() error = %v, wantErr %v", err, tt.wantErr)
			}

			if tt.wantReplayCalled != mockReplayer.ReplayCalled {
				t.Errorf("want: Replay() called=%v, got: Replay() called=%v",
					tt.wantReplayCalled, mockReplayer.ReplayCalled)
			}
		})
	}
}
