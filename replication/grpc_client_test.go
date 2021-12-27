package replication_test

import (
	"context"
	"errors"
	"io"
	"reflect"
	"testing"

	pb "github.com/alpacahq/marketstore/v4/proto"
	"github.com/alpacahq/marketstore/v4/replication"
	"github.com/alpacahq/marketstore/v4/replication/mock"
)

func TestGRPCReplicationClient_Connect(t *testing.T) {
	// --- given ---
	t.Parallel()
	client := replication.NewGRPCReplicationClient(&mock.ReplicationClient{})

	// --- when ---
	err := client.Connect(context.Background())
	// --- then ---
	if err != nil {
		t.Error("Connect should succeed")
	}
}

func TestGRPCReplicationClient_Connect_Error(t *testing.T) {
	// --- given ---
	t.Parallel()
	client := replication.NewGRPCReplicationClient(&mock.ReplicationClient{Error: errors.New("an error")})

	// --- when ---
	err := client.Connect(context.Background())

	// --- then ---
	if err == nil {
		t.Error("Connect should fail")
	}
}

func TestGRPCReplicationClient_Recv_streamNotInitialized(t *testing.T) {
	// --- given ---
	t.Parallel()
	client := replication.NewGRPCReplicationClient(&mock.ReplicationClient{})
	// _ = client.Connect(context.Background()) // Not Connected yet

	// --- when ---
	_, err := client.Recv()

	// --- then ---
	if err == nil {
		t.Errorf("Recv() call should fail before connect")
	}
}

func TestGRPCReplicationClient_Recv(t *testing.T) {
	tests := []struct {
		name             string
		mockStreamClient pb.Replication_GetWALStreamClient
		want             []byte
		wantErr          bool
	}{
		{
			name: "success",
			mockStreamClient: &mock.WALStreamClient{
				Response: &pb.GetWALStreamResponse{TransactionGroup: []byte{1, 2, 3}},
				Error:    nil,
			},
			want:    []byte{1, 2, 3},
			wantErr: false,
		},
		{
			name: "error/received io.EOF",
			mockStreamClient: &mock.WALStreamClient{
				Response: &pb.GetWALStreamResponse{TransactionGroup: nil},
				Error:    io.EOF,
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "error/received an error",
			mockStreamClient: &mock.WALStreamClient{
				Response: &pb.GetWALStreamResponse{TransactionGroup: nil},
				Error:    errors.New("some error"),
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "error/received nil message",
			mockStreamClient: &mock.WALStreamClient{
				Response: nil, // nil message
				Error:    nil,
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			// --- given ---
			client := replication.NewGRPCReplicationClient(&mock.ReplicationClient{StreamClient: tt.mockStreamClient})
			_ = client.Connect(context.Background())

			// --- when ---
			got, err := client.Recv()

			// --- then ---
			if (err != nil) != tt.wantErr {
				t.Errorf("NewGRPCReplicationClient() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewGRPCReplicationClient() got = %v, want %v", got, tt.want)
			}
		})
	}
}
