package server

import (
	"github.com/mrdxy/raft-kv-example/raft"
	"github.com/mrdxy/raft-kv-example/storage/backend"
	"github.com/mrdxy/raft-kv-example/transport/client_transport"
)

type Config struct {
	Id         int
	Peers      []string
	PeerPort   int
	ClientPort int
}

type Server struct {
	ct      client_transport.ClientTransport
	r       raft.Raft
	backend backend.Backend
	// TODO add a stop/err channel to control server errors or stops
}

func NewServer(config *Config) *Server {
	b := backend.NewInMemoryBackend()
	r := raft.NewRaftNode(config.Id, config.PeerPort, config.Peers, false, b.CreateSnap)
	c := client_transport.NewHttpClientTransport(config.ClientPort, r.ClientPropose, r.ClientCCPropose, r.ClientReadIndex, b)
	return &Server{
		ct:      c,
		r:       r,
		backend: b,
	}
}

func (s *Server) Start() {
	go s.r.StartRaft()
	go s.backend.Start(s.r.CommitC(), s.r.SnapShotC())
	s.ct.Start()
}
