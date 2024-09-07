package client_transport

import (
	"context"
	"errors"
	"github.com/mrdxy/raft-kv-example/storage/backend"
	"io"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.etcd.io/raft/v3/raftpb"
)

type ClientTransport interface {
	Start()
	Stop()
	handleGet(key string) (string, bool)
	handleSet(key string, val string) error
	handleConfChange(cc raftpb.ConfChange) error
}

const (
	KVPrefix   = "/kv"
	ConfPrefix = "/conf"
)

type HttpClientTransport struct {
	mu          sync.Mutex
	wg          sync.WaitGroup
	server      *http.Server
	transport   *http.Transport
	store       backend.Backend
	rPropose    func(ctx context.Context, data []byte) error          // raft propose handler
	rConfChange func(ctx context.Context, cc raftpb.ConfChange) error // raft config change handler
	rReadIndex  func(ctx context.Context) error                       // request for read index
}

func NewHttpClientTransport(port int,
	rPropose func(ctx context.Context, data []byte) error,
	rConfChange func(ctx context.Context, cc raftpb.ConfChange) error,
	rReadIndex func(ctx context.Context) error,
	store backend.Backend) ClientTransport {
	t := &HttpClientTransport{
		store:       store,
		rPropose:    rPropose,
		rConfChange: rConfChange,
		rReadIndex:  rReadIndex,
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

func (h *HttpClientTransport) handleGet(key string) (string, bool) {
	err := h.rReadIndex(context.TODO())
	if err != nil {
		return err.Error(), false
	}
	return h.store.Get(key)
}

func (h *HttpClientTransport) handleSet(key string, val string) error {
	b, err := backend.EncodeKV(backend.KV{Key: key, Val: val})
	if err != nil {
		log.Printf("encode kv error, msg: %s", err)
	}
	return h.rPropose(context.TODO(), b)
}

func (h *HttpClientTransport) handleConfChange(cc raftpb.ConfChange) error {
	return h.rConfChange(context.TODO(), cc)
}

func (h *HttpClientTransport) createHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		path := r.URL.Path

		switch {
		case strings.HasPrefix(path, "/kv"):
			h.handleKVRequests(w, r)
		case strings.HasPrefix(path, "/conf"):
			h.handleConfChangeRequests(w, r)
		default:
			http.Error(w, "Invalid path", http.StatusNotFound)
		}
	})
}

func (h *HttpClientTransport) handleKVRequests(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPut:
		data, err := io.ReadAll(r.Body)
		if err != nil {
			log.Printf("Failed to read on PUT (%v)\n", err)
			http.Error(w, "Failed on PUT", http.StatusBadRequest)
			return
		}

		// "key=value"
		parts := strings.SplitN(string(data), "=", 2)
		if len(parts) != 2 {
			log.Println("Invalid PUT request format")
			http.Error(w, "Invalid PUT request format", http.StatusBadRequest)
			return
		}

		key, value := parts[0], parts[1]

		err = h.handleSet(key, value)
		if err != nil {
			log.Printf("Failed to handleSet on PUT (%v)\n", err)
			http.Error(w, "Failed on PUT", http.StatusBadRequest)
			return
		}

		w.WriteHeader(http.StatusOK)
	case http.MethodGet:
		data, err := io.ReadAll(r.Body)
		if err != nil {
			log.Printf("Failed to read on GET (%v)\n", err)
			http.Error(w, "Failed on GET", http.StatusBadRequest)
			return
		}

		key := string(data)

		if v, ok := h.handleGet(key); ok {
			w.Write([]byte(v))
		} else {
			w.Write([]byte(v))
			http.Error(w, "Failed to GET", http.StatusNotFound)
		}
	default:
		w.Header().Set("Allow", http.MethodPut)
		w.Header().Add("Allow", http.MethodGet)
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (h *HttpClientTransport) handleConfChangeRequests(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		data, err := io.ReadAll(r.Body)
		if err != nil {
			log.Printf("Failed to read on POST (%v)\n", err)
			http.Error(w, "Failed on POST", http.StatusBadRequest)
			return
		}

		// "nodeID=url"
		parts := strings.SplitN(string(data), "=", 2)
		if len(parts) != 2 {
			log.Println("Invalid POST request format")
			http.Error(w, "Invalid POST request format", http.StatusBadRequest)
			return
		}

		nodeID, err := strconv.ParseUint(parts[0], 10, 64)
		if err != nil {
			log.Printf("Failed to convert nodeID for conf change (%v)\n", err)
			http.Error(w, "Failed on POST", http.StatusBadRequest)
			return
		}
		url := parts[1]

		cc := raftpb.ConfChange{
			Type:    raftpb.ConfChangeAddNode,
			NodeID:  nodeID,
			Context: []byte(url),
		}
		err = h.handleConfChange(cc)
		if err != nil {
			log.Printf("Failed to handle conf change (%v)\n", err)
			http.Error(w, "Failed on POST", http.StatusBadRequest)
			return
		}

		w.WriteHeader(http.StatusNoContent)
	case http.MethodDelete:
		data, err := io.ReadAll(r.Body)
		if err != nil {
			log.Printf("Failed to read on DELETE (%v)\n", err)
			http.Error(w, "Failed on DELETE", http.StatusBadRequest)
			return
		}

		nodeID, err := strconv.ParseUint(string(data), 10, 64)
		if err != nil {
			log.Printf("Failed to convert nodeID for conf change (%v)\n", err)
			http.Error(w, "Failed on DELETE", http.StatusBadRequest)
			return
		}

		cc := raftpb.ConfChange{
			Type:   raftpb.ConfChangeRemoveNode,
			NodeID: nodeID,
		}
		err = h.handleConfChange(cc)
		if err != nil {
			log.Printf("Failed to handle conf change (%v)\n", err)
			http.Error(w, "Failed on DELETE", http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		w.Header().Set("Allow", http.MethodPost)
		w.Header().Add("Allow", http.MethodDelete)
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// Start starts the HTTP server and waits for stop signal.
func (t *HttpClientTransport) Start() {
	t.wg.Add(1)
	defer t.wg.Done()

	if err := t.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("HttpPeerTransport: ListenAndServe failed: %v", err)
	}
	log.Println("HttpPeerTransport started")
}

// Stop gracefully shuts down the server without interrupting any active connections.
func (t *HttpClientTransport) Stop() {
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
