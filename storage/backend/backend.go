package backend

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"errors"
	"github.com/mrdxy/raft-kv-example/raft"
	"github.com/mrdxy/raft-kv-example/storage/snapshot"
	"log"
	"sync"
)

type KV struct {
	Key string
	Val string
}

type Backend interface {
	put(key string, val string)
	Get(key string) (val string, ok bool)
	CreateSnap() (snapshot []byte, err error)
	loadSnap() error
	Start(commitC <-chan *raft.Commit, snapshotterC <-chan snapshot.Snapshotter)
}

type InMemoryBackend struct {
	mu          sync.RWMutex
	dataMap     map[string]string
	snapshotter snapshot.Snapshotter
}

func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		mu:      sync.RWMutex{},
		dataMap: make(map[string]string),
	}
}

func (b *InMemoryBackend) Start(commitC <-chan *raft.Commit, snapshotterC <-chan snapshot.Snapshotter) {
	snapshotter, ok := <-snapshotterC
	if !ok {
		log.Fatal("failed to read from snapshotterC")
	}
	b.snapshotter = snapshotter
	err := b.loadSnap()
	if err != nil {
		log.Fatalf("failed to load snapshot, err: %v", err)
	}
	b.consumeCommit(commitC)
}

func (b *InMemoryBackend) consumeCommit(commitC <-chan *raft.Commit) {
	for commit := range commitC {
		// TODO enable this comment, you can delay here to make the (applied index) < (read index), to testify the correctness of linearizability
		//time.Sleep(200 * time.Millisecond)
		switch commit.Type {
		case raft.CommitSnapshot:
			if commit.SnapData.Data != nil && len(commit.SnapData.Data) != 0 {
				var store = make(map[string]string)
				if err := json.Unmarshal(commit.SnapData.Data, &store); err != nil {
					log.Panicf("apply sanpshot failed, err: %v", err)
				}
				b.mu.Lock()
				b.dataMap = store
				b.mu.Unlock()
				commit.ApplyProgress.AppliedIndex = commit.SnapData.Index
				commit.ApplyProgress.ApplyWait.Trigger(commit.SnapData.Index)
				return
			}
		case raft.CommitEntry:
			for _, data := range commit.EntryData {
				if len(data.Data) != 0 {
					kv, err := DecodeKV(data.Data)
					if err != nil {
						log.Fatalf("failed to decode KV, err: %v", err)
					}
					b.put(kv.Key, kv.Val)
				}
				commit.ApplyProgress.AppliedIndex = data.Index
				commit.ApplyProgress.ApplyWait.Trigger(data.Index)
			}
		}
		close(commit.ApplyDoneC)
	}
}

func (b *InMemoryBackend) put(key string, val string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.dataMap[key] = val
}

func (b *InMemoryBackend) Get(key string) (val string, ok bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	v, ok := b.dataMap[key]
	return v, ok
}

func (b *InMemoryBackend) CreateSnap() (snapshot []byte, err error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return json.Marshal(b.dataMap)
}

func (b *InMemoryBackend) loadSnap() error {
	snappb, err := b.snapshotter.Load()
	if err != nil && !errors.Is(err, snapshot.ErrNoSnapshot) {
		return err
	}
	var store = make(map[string]string)
	if snappb != nil {
		if err := json.Unmarshal(snappb.Data, &store); err != nil {
			return err
		}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.dataMap = store
	return nil
}

func EncodeKV(kv KV) ([]byte, error) {
	var buffer bytes.Buffer

	if err := gob.NewEncoder(&buffer).Encode(kv); err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

func DecodeKV(data []byte) (KV, error) {
	var kv KV

	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&kv); err != nil {
		return kv, err
	}

	return kv, nil
}
