package raft

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/mrdxy/raft-kv-example/storage/snapshot"
	"github.com/mrdxy/raft-kv-example/storage/wal"
	"github.com/mrdxy/raft-kv-example/transport/peer_transport"
	"github.com/mrdxy/raft-kv-example/util"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

type Raft interface {
	// TODO wrap with response to notify whether operation success or failed
	ClientPropose(ctx context.Context, data []byte) error
	ClientCCPropose(ctx context.Context, cc raftpb.ConfChange) error
	PeerProcess(ctx context.Context, m raftpb.Message) error

	CommitC() <-chan *Commit
	SnapShotC() <-chan snapshot.Snapshotter

	StartRaft()

	//IsIDRemoved(id uint64) bool
	//ReportUnreachable(id uint64)
	//ReportSnapshot(id uint64, status raft.SnapshotStatus)
}

type Commit struct {
	Data       []string
	ApplyDoneC chan<- struct{}
}

type raftNode struct {
	commitC chan *Commit // entries committed to log (k,v)
	errorC  chan error   // errors from raft session

	id          int      // client ID for raft session
	port        int      // port for peer traffic
	peers       []string // raft peer URLs
	join        bool     // node is joining an existing cluster
	waldir      string   // path to WAL directory
	snapdir     string   // path to snapshot directory
	getSnapshot func() ([]byte, error)
	snapCount   uint64

	confState     raftpb.ConfState
	snapshotIndex uint64
	appliedIndex  uint64

	// raft backing for the Commit/error channel
	node    raft.Node
	restart bool

	raftStorage      *raft.MemoryStorage
	wal              wal.WAL
	snapshotter      snapshot.Snapshotter
	snapshotterReady chan snapshot.Snapshotter // signals when snapshotter is ready

	transport peer_transport.PeerTransport
	stopc     chan struct{} // signals proposal channel closed
}

var defaultSnapshotCount uint64 = 1000

func NewRaftNode(id, port int, peers []string, join bool, getSnapshot func() ([]byte, error)) Raft {
	return &raftNode{
		commitC:          make(chan *Commit),
		errorC:           make(chan error),
		id:               id,
		port:             port,
		peers:            peers,
		join:             join,
		waldir:           fmt.Sprintf("data-%d-wal", id),
		snapdir:          fmt.Sprintf("data-%d-snap", id),
		getSnapshot:      getSnapshot,
		snapCount:        defaultSnapshotCount,
		snapshotterReady: make(chan snapshot.Snapshotter, 1),
		stopc:            make(chan struct{}),
		// rest of structure populated after WAL replay
	}
}

func (rc *raftNode) CommitC() <-chan *Commit {
	return rc.commitC
}

func (rc *raftNode) SnapShotC() <-chan snapshot.Snapshotter {
	return rc.snapshotterReady
}

func (rc *raftNode) PeerProcess(ctx context.Context, m raftpb.Message) error {
	return rc.node.Step(ctx, m)
}

func (rc *raftNode) ClientPropose(ctx context.Context, data []byte) error {
	return rc.node.Propose(ctx, data)
}

func (rc *raftNode) ClientCCPropose(ctx context.Context, cc raftpb.ConfChange) error {
	return rc.node.ProposeConfChange(ctx, cc)
}

func (rc *raftNode) StartRaft() {
	rc.initDirectories()
	s := rc.loadSnapshot()
	h, e := rc.loadWAL(s)
	rc.replay(s, h, e)
	rc.startWAL()
	rc.signalSnapshotterReady()

	rc.initializeRaftNode()
	rc.initializePeerTransport()

	go rc.transport.Start()
	go rc.serveChannels()
}

func (rc *raftNode) initDirectories() {
	if util.Exist(rc.waldir) {
		rc.restart = true
	}
	rc.createDirIfNotExist(rc.snapdir)
	rc.createDirIfNotExist(rc.waldir)
}

func (rc *raftNode) createDirIfNotExist(dir string) {
	if !util.Exist(dir) {
		if err := os.Mkdir(dir, 0750); err != nil {
			log.Fatalf("Cannot create directory %s: %v", dir, err)
		}
	}
}

func (rc *raftNode) loadSnapshot() *raftpb.Snapshot {
	rc.snapshotter = snapshot.NewFileSnapshot(rc.snapdir)
	s, err := rc.snapshotter.Load()
	if err != nil && !errors.Is(err, snapshot.ErrNoSnapshot) {
		log.Fatalf("error loading snap (%v)", err)
	}
	return s
}

func (rc *raftNode) loadWAL(s *raftpb.Snapshot) (h raftpb.HardState, e []raftpb.Entry) {
	w, err := wal.NewFileWAL(rc.waldir)
	if err != nil {
		log.Fatalf("failed to open WAL (%v)", err)
	}
	rc.wal = w
	snap := &raftpb.Snapshot{}
	if s != nil {
		snap = s
	}
	h, e, err = rc.wal.Read(snap)
	if err != nil {
		log.Fatalf("failed to read WAL (%v)", err)
	}
	return h, e
}

func (rc *raftNode) replay(s *raftpb.Snapshot, h raftpb.HardState, e []raftpb.Entry) {
	rc.raftStorage = raft.NewMemoryStorage()
	if s != nil {
		err := rc.raftStorage.ApplySnapshot(*s)
		if err != nil {
			log.Fatalf("failed to replay snapshot (%v)", err)
		}
		rc.confState = s.Metadata.ConfState
		rc.snapshotIndex = s.Metadata.Index
		rc.appliedIndex = s.Metadata.Index
	}
	err := rc.raftStorage.SetHardState(h)
	if err != nil {
		log.Fatalf("failed to replay HardState (%v)", err)
	}

	// append to storage so raft starts at the right place in log
	err = rc.raftStorage.Append(e)
	if err != nil {
		log.Fatalf("failed to replay Entries (%v)", err)
	}
}

func (rc *raftNode) startWAL() {
	if err := rc.wal.Start(); err != nil {
		log.Fatalf("Failed to start WAL: %v", err)
	}
}

func (rc *raftNode) signalSnapshotterReady() {
	rc.snapshotterReady <- rc.snapshotter
}

func (rc *raftNode) initializeRaftNode() {
	c := &raft.Config{
		ID:                        uint64(rc.id),
		ElectionTick:              10,
		HeartbeatTick:             1,
		Storage:                   rc.raftStorage,
		MaxSizePerMsg:             1024 * 1024,
		MaxInflightMsgs:           256,
		MaxUncommittedEntriesSize: 1 << 30,
	}
	peers := rc.createPeers()

	if rc.restart || rc.join {
		rc.node = raft.RestartNode(c)
	} else {
		rc.node = raft.StartNode(c, peers)
	}
}

func (rc *raftNode) createPeers() []raft.Peer {
	peers := make([]raft.Peer, len(rc.peers))
	for i := range peers {
		peers[i] = raft.Peer{ID: uint64(i + 1)}
	}
	return peers
}

func (rc *raftNode) initializePeerTransport() {
	rc.transport = peer_transport.NewHttpPeerTransport(rc.port, rc.PeerProcess)
	for i := range rc.peers {
		if i+1 != rc.id {
			rc.transport.AddPeer(uint64(i+1), rc.peers[i])
		}
	}
}

func (rc *raftNode) serveChannels() {
	defer rc.cleanup()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			rc.node.Tick()
		case rd := <-rc.node.Ready():
			rc.processRaftReady(rd)
		case <-rc.stopc:
			return
		}
	}
}

func (rc *raftNode) cleanup() {
	rc.wal.Close()
	close(rc.commitC)
	close(rc.errorC)
	rc.node.Stop()
}

func (rc *raftNode) processRaftReady(rd raft.Ready) {
	// update snapshot file and wal file
	if !raft.IsEmptySnap(rd.Snapshot) {
		rc.saveSnap(&rd.Snapshot)
	}

	if err := rc.wal.SaveLog(rd.HardState, rd.Entries); err != nil {
		log.Fatalf("Failed to save log: %v", err)
	}

	// apply snapshot to raftStorage and backend
	if !raft.IsEmptySnap(rd.Snapshot) {
		err := rc.raftStorage.ApplySnapshot(rd.Snapshot)
		if err != nil {
			log.Fatalf("Failed to apply snapshot: %v", err)
		}
		rc.publishSnapshot(rd.Snapshot)
	}
	// apply entries to raftStorage and backend
	if err := rc.raftStorage.Append(rd.Entries); err != nil {
		log.Fatalf("Failed to append log: %v", err)
	}
	rc.transport.Send(rc.processMessages(rd.Messages))
	applyDoneC, ok := rc.publishEntries(rc.entriesToApply(rd.CommittedEntries))
	if !ok {
		rc.stop()
		return
	}
	rc.maybeTriggerSnapshot(applyDoneC)
	rc.node.Advance()
}

func (rc *raftNode) saveSnap(snap *raftpb.Snapshot) {
	// save the snapshot file before writing the snapshot to the wal.
	// This makes it possible for the snapshot file to become orphaned, but prevents
	// a WAL snapshot entry from having no corresponding snapshot file.
	if err := rc.snapshotter.Save(snap); err != nil {
		log.Fatalf("snapshotter failed to save snapshot, err: %v", err)
	}
	if err := rc.wal.SaveSnapshot(snap); err != nil {
		log.Fatalf("wal failed to save snapshot, err: %v", err)
	}
}

func (rc *raftNode) entriesToApply(ents []raftpb.Entry) (nents []raftpb.Entry) {
	if len(ents) == 0 {
		return ents
	}
	firstIdx := ents[0].Index
	// ensures the following case won't happen
	// new ent:            ----
	//               |gap|
	// applied: ----
	if firstIdx > rc.appliedIndex+1 {
		log.Fatalf("first index of committed entry[%d] should <= progress.appliedIndex[%d]+1", firstIdx, rc.appliedIndex)
	}
	// ensures the following case, no new entries
	// new ent:    ----
	//             |     |
	// applied: ----------
	if rc.appliedIndex-firstIdx+1 < uint64(len(ents)) {
		nents = ents[rc.appliedIndex-firstIdx+1:]
	}
	return nents
}

// publishEntries writes committed log entries to Commit channel and returns
// whether all entries could be published.
func (rc *raftNode) publishEntries(ents []raftpb.Entry) (<-chan struct{}, bool) {
	if len(ents) == 0 {
		return nil, true
	}

	data := make([]string, 0, len(ents))
	for i := range ents {
		switch ents[i].Type {
		case raftpb.EntryNormal:
			if len(ents[i].Data) == 0 {
				// ignore empty messages
				break
			}
			s := string(ents[i].Data)
			data = append(data, s)
		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			cc.Unmarshal(ents[i].Data)
			rc.confState = *rc.node.ApplyConfChange(cc)
			switch cc.Type {
			case raftpb.ConfChangeAddNode:
				if len(cc.Context) > 0 {
					rc.transport.AddPeer(cc.NodeID, string(cc.Context))
				}
			case raftpb.ConfChangeRemoveNode:
				if cc.NodeID == uint64(rc.id) {
					log.Println("I've been removed from the cluster! Shutting down.")
					return nil, false
				}
				rc.transport.RemovePeer(cc.NodeID)
			}
		}
	}

	var applyDoneC chan struct{}

	if len(data) > 0 {
		applyDoneC = make(chan struct{}, 1)
		select {
		case rc.commitC <- &Commit{data, applyDoneC}:
		case <-rc.stopc:
			return nil, false
		}
	}

	// after commit, update appliedIndex
	rc.appliedIndex = ents[len(ents)-1].Index

	return applyDoneC, true
}

func (rc *raftNode) publishSnapshot(snapshotToSave raftpb.Snapshot) {
	if raft.IsEmptySnap(snapshotToSave) {
		return
	}

	log.Printf("publishing snapshot at index %d", rc.snapshotIndex)
	defer log.Printf("finished publishing snapshot at index %d", rc.snapshotIndex)

	if snapshotToSave.Metadata.Index <= rc.appliedIndex {
		log.Fatalf("snapshot index [%d] should > progress.appliedIndex [%d]", snapshotToSave.Metadata.Index, rc.appliedIndex)
	}
	rc.commitC <- nil // trigger kvstore to load snapshot

	rc.confState = snapshotToSave.Metadata.ConfState
	rc.snapshotIndex = snapshotToSave.Metadata.Index
	rc.appliedIndex = snapshotToSave.Metadata.Index
}

// When there is a `raftpb.EntryConfChange` after creating the snapshot,
// then the confState included in the snapshot is out of date. so We need
// to update the confState before sending a snapshot to a follower.
func (rc *raftNode) processMessages(ms []raftpb.Message) []raftpb.Message {
	for i := 0; i < len(ms); i++ {
		if ms[i].Type == raftpb.MsgSnap {
			ms[i].Snapshot.Metadata.ConfState = rc.confState
		}
	}
	return ms
}

// stop closes http, closes all channels, and stops raft.
func (rc *raftNode) stop() {
	rc.transport.Stop()
	close(rc.commitC)
	close(rc.errorC)
	rc.node.Stop()
}

var snapshotCatchUpEntriesN uint64 = 10000

func (rc *raftNode) maybeTriggerSnapshot(applyDoneC <-chan struct{}) {
	if rc.appliedIndex-rc.snapshotIndex <= rc.snapCount {
		return
	}

	// wait until all committed entries are applied (or server is closed)
	if applyDoneC != nil {
		select {
		case <-applyDoneC:
		case <-rc.stopc:
			return
		}
	}

	log.Printf("start snapshot [applied index: %d | last snapshot index: %d]", rc.appliedIndex, rc.snapshotIndex)
	data, err := rc.getSnapshot()
	if err != nil {
		log.Panic(err)
	}
	snap, err := rc.raftStorage.CreateSnapshot(rc.appliedIndex, &rc.confState, data)
	if err != nil {
		panic(err)
	}
	rc.saveSnap(&snap)

	compactIndex := uint64(1)
	if rc.appliedIndex > snapshotCatchUpEntriesN {
		compactIndex = rc.appliedIndex - snapshotCatchUpEntriesN
	}
	if err := rc.raftStorage.Compact(compactIndex); err != nil {
		if !errors.Is(err, raft.ErrCompacted) {
			panic(err)
		}
	} else {
		log.Printf("compacted log at index %d", compactIndex)
	}

	rc.snapshotIndex = rc.appliedIndex
}
