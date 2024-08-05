package peer_transport

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"go.etcd.io/raft/v3/raftpb"
)

// TestSend tests the Send method with a valid peer
func TestSend(t *testing.T) {
	transport := NewHttpPeerTransport(8080, nil)

	// Create a mock server to simulate peer response
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Println("receives a response")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	// Add a mock peer
	transport.AddPeer(1, server.URL)

	// Create a raft message
	msg := raftpb.Message{
		To:   1,
		From: 2,
		Term: 1,
	}

	transport.Send([]raftpb.Message{msg})

	// Wait for a short period to allow the async operation to complete
	time.Sleep(1 * time.Second)
}

// TestSendPeerNotFound tests the Send method when the peer is not found
