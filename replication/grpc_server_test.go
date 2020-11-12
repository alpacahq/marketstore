package replication_test

import (
	"github.com/alpacahq/marketstore/v4/proto"
	"github.com/alpacahq/marketstore/v4/replication"
	"github.com/alpacahq/marketstore/v4/replication/mock"
	"github.com/google/go-cmp/cmp"
	"testing"
	"time"
)

// listen messages -> wait 500ms -> put a test message to a channel -> wait 100ms -> the message should be sent
func TestGRPCReplicationServer_GetWALStream_success(t *testing.T) {
	// --- given ---
	t.Parallel()
	replServer := replication.NewGRPCReplicationService()
	testTGMessage := []byte{1, 2, 3}

	stream := &mock.WALStreamServer{
		SendFunc: func(resp *proto.GetWALStreamResponse) error {
			// test message should be sent
			if !cmp.Equal(resp.TransactionGroup, testTGMessage) {
				t.Errorf("got: %v, want: %v", resp.TransactionGroup, testTGMessage)
			}
			return nil
		},
	}

	// --- when ---
	// start to listen
	go func() {
		_ = replServer.GetWALStream(nil, stream)
	}()
	time.Sleep(500 * time.Millisecond)

	replServer.SendReplicationMessage(testTGMessage)
	time.Sleep(100 * time.Millisecond)

	// --- then ---
	// assertion is done in SendFunc
}

func TestGRPCReplicationServer_GetWALStream_error(t *testing.T) {
	// --- given ---
	t.Parallel()
	replServer := replication.NewGRPCReplicationService()
	testTGMessage := []byte{1, 2, 3}

	stream := &mock.ErrorWALStreamServer{}

	// --- when ---

	// start to listen, but Send function should return error and GetWALStream should return nil
	go func() {
		err := replServer.GetWALStream(nil, stream)
		if err != nil {
			t.Errorf("GetWALStream should return nil when Send failed")
		}
	}()
	time.Sleep(500 * time.Millisecond)

	// send a message to the channel
	replServer.SendReplicationMessage(testTGMessage)
	time.Sleep(100 * time.Millisecond)

	// --- then ---
	// assertion is done in the go func
}

func TestGRPCReplicationServer_GetWALStream_getClientAddr_error(t *testing.T) {
	// --- given ---
	t.Parallel()
	replServer := replication.NewGRPCReplicationService()
	stream := &mock.GetClientAddrErrorWALStreamServer{}

	// --- when ---
	// getClientAddr fails and an error should be returned
	err := replServer.GetWALStream(nil, stream)

	// --- then ---
	if err == nil {
		t.Errorf("getClientAddr should fail")
	}
}
