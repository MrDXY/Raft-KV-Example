package peer_transport

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"go.etcd.io/raft/v3/raftpb"
)

type PeerTransport interface {
	AddPeer(uint64, string)
	RemovePeer(uint64)
	// Send handles the traffic out
	Send([]raftpb.Message)
	// Handle handles the traffic in
	handle(message raftpb.Message) error
	Start()
	Stop()
}

const (
	RaftPrefix = "/raft"
)

type HttpPeerTransport struct {
	mu           sync.Mutex
	wg           sync.WaitGroup
	peers        map[uint64]string
	server       *http.Server
	transport    *http.Transport
	rPeerProcess func(ctx context.Context, m raftpb.Message) error
}

func NewHttpPeerTransport(port int, rPeerProcess func(ctx context.Context, m raftpb.Message) error) PeerTransport {
	t := &HttpPeerTransport{
		peers:        make(map[uint64]string),
		rPeerProcess: rPeerProcess,
	}
	t.transport = &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
	}
	t.server = &http.Server{
		Addr:    ":" + strconv.Itoa(port),
		Handler: t.createHandler(),
	}
	return t
}

// Start starts the HTTP server and waits for stop signal.
func (t *HttpPeerTransport) Start() {
	t.wg.Add(1)
	defer t.wg.Done()

	if err := t.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("HttpPeerTransport: ListenAndServe failed: %v", err)
	}
	log.Println("HttpPeerTransport started")
}

// Stop gracefully shuts down the server without interrupting any active connections.
func (t *HttpPeerTransport) Stop() {
	// Gracefully shutdown the server with a timeout context.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := t.server.Shutdown(shutdownCtx); err != nil {
		log.Printf("HttpPeerTransport: Shutdown failed: %v", err)
	} else {
		log.Println("HttpPeerTransport: Server gracefully stopped")
	}
	// Wait for the ListenAndServe goroutine to finish.
	t.wg.Wait()
	log.Println("HttpPeerTransport stopped")
}

func (t *HttpPeerTransport) AddPeer(id uint64, addresses string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.peers[id] = addresses
}

func (t *HttpPeerTransport) RemovePeer(id uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.peers, id)
}

func (t *HttpPeerTransport) Send(msgs []raftpb.Message) {
	t.mu.Lock()
	defer t.mu.Unlock()

	for _, msg := range msgs {
		peer, ok := t.peers[msg.To]
		if !ok {
			log.Printf("Peer %d not found", msg.To)
			continue
		}

		data, err := msg.Marshal()
		if err != nil {
			log.Printf("Failed to marshal message: %v", err)
			continue
		}

		go func(peer string, data []byte) {
			req, err := http.NewRequest(http.MethodPost, peer+RaftPrefix, bytes.NewBuffer(data))
			if err != nil {
				log.Printf("Failed to create request for Peer %s: %v", peer, err)
				return
			}
			req.Header.Set("Content-Type", "application/json")

			resp, err := t.transport.RoundTrip(req)
			if err != nil {
				log.Printf("Failed to send message to Peer %s: %v", peer, err)
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				log.Printf("Failed to send message to Peer %s, status code: %d", peer, resp.StatusCode)
			}
		}(peer, data)
	}
}

func (t *HttpPeerTransport) handle(msg raftpb.Message) error {
	return t.rPeerProcess(context.TODO(), msg)
}

func (t *HttpPeerTransport) createHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != RaftPrefix {
			http.Error(w, "Not Found", http.StatusNotFound)
			return
		}

		var msg raftpb.Message
		b, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "error reading raft message", http.StatusBadRequest)
			return
		}
		if err := msg.Unmarshal(b); err != nil {
			http.Error(w, "Bad Request", http.StatusBadRequest)
			return
		}

		if err := t.handle(msg); err != nil {
			http.Error(w, "error processing raft message: "+err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
}
