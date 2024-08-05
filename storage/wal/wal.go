package wal

import (
	"errors"
	"fmt"
	"github.com/mrdxy/raft-kv-example/pb/walpb"
	"go.etcd.io/raft/v3"
	"io"
	"os"
	"sync"

	"go.etcd.io/raft/v3/raftpb"
)

// WAL has two purposes:
// - first is to write data to the log file.
// - second is to read out all the log records to help the raft node upon restart/start
type WAL interface {
	SaveLog(h raftpb.HardState, entries []raftpb.Entry) error
	SaveSnapshot(s *raftpb.Snapshot) error
	// Read will read all the data that walpb contains, it will overwrite itself if conflict
	Read(s *raftpb.Snapshot) (h raftpb.HardState, e []raftpb.Entry, err error)
	Start() error
	Close() error
}

const (
	MetadataType int64 = iota + 1
	EntryType
	StateType
	SnapshotType
	CrcType
)

var (
	SegmentSizeBytes int64 = 64 * 1000 * 1000 // 64MB
)

type FileWAL struct {
	dir   string
	mu    sync.Mutex
	lr    *LogFileReader
	lw    *LogFileWriter
	llcm  *LogFileLCM
	state raftpb.HardState
	enti  uint64
}

func NewFileWAL(dir string) (*FileWAL, error) {
	llcm, err := NewLogFileLCM(dir, SegmentSizeBytes)
	if err != nil {
		return nil, err
	}
	fw := &FileWAL{
		mu:   sync.Mutex{},
		dir:  dir,
		llcm: llcm,
	}
	return fw, nil
}

func NewReadOnlyFileWal(dir string) *FileWAL {
	return &FileWAL{
		mu:  sync.Mutex{},
		dir: dir,
	}
}

func (f *FileWAL) Start() error {
	if f.lr == nil {
		return errors.New("file wal reader is not initialized")
	}

	// init log writer
	tail, err := openLatestLogFile(f.dir)
	if err != nil {
		return err
	}
	lw, err := NewLogFileWriter(tail, f.lr.TailOffset())
	if err != nil {
		return err
	}
	f.lw = lw

	// init log lcm tail
	f.llcm.SetTail(tail)
	f.lr = nil

	return nil
}

func (f *FileWAL) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.lw != nil {
		if err := f.lw.Sync(); err != nil {
			return err
		}
	}

	if f.llcm != nil {
		if err := f.llcm.Stop(); err != nil {
			return err
		}
		f.llcm = nil
	}
	return nil
}

func (f *FileWAL) SaveLog(h raftpb.HardState, ents []raftpb.Entry) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	// shortcut, do not call sync
	if raft.IsEmptyHardState(h) && len(ents) == 0 {
		return nil
	}

	mustSync := raft.MustSync(h, f.state, len(ents))

	for i := range ents {
		if err := f.saveEntry(ents[i]); err != nil {
			return err
		}
	}
	if err := f.saveState(h); err != nil {
		return err
	}

	curOff, err := f.lw.CurOffset()
	if err != nil {
		return err
	}
	if curOff < SegmentSizeBytes {
		if mustSync {
			return f.lw.Sync()
		}
		return nil
	}
	postCut := func(newTail *os.File) error {
		f.lw, err = NewLogFileWriter(newTail, 0)
		return err
	}
	return f.llcm.Cut(nil, nil, postCut, f.enti)
}

func (f *FileWAL) saveState(h raftpb.HardState) error {
	if raft.IsEmptyHardState(h) {
		return nil
	}
	f.state = h
	data, err := h.Marshal()
	if err != nil {
		return err
	}
	if err := f.lw.Write(&walpb.Record{Type: StateType, Data: data}); err != nil {
		return err
	}
	return nil
}

func (f *FileWAL) saveEntry(entry raftpb.Entry) error {
	data, err := entry.Marshal()
	if err != nil {
		return err
	}
	if err := f.lw.Write(&walpb.Record{Type: EntryType, Data: data}); err != nil {
		return err
	}
	f.enti = entry.Index
	return nil
}

func (f *FileWAL) SaveSnapshot(s *raftpb.Snapshot) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	data, err := s.Marshal()
	if err != nil {
		return err
	}
	if err := f.lw.Write(&walpb.Record{Type: SnapshotType, Data: data}); err != nil {
		return err
	}
	return f.lw.Sync()
}

func (f *FileWAL) Read(s *raftpb.Snapshot) (h raftpb.HardState, ents []raftpb.Entry, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	lr, err := NewLogFileReader(f.dir, s.Metadata)

	rec := &walpb.Record{}
	var match bool
	for err = lr.Read(rec); err == nil; err = lr.Read(rec) {
		switch rec.Type {
		case EntryType:
			var entry raftpb.Entry
			e := entry.Unmarshal(rec.Data)
			if e != nil {
				panic(fmt.Sprintf("unmarshal should never fail (%v)", err))
			}
			// 0 <= e.Index-w.start.Index - 1 < len(ents)
			if entry.Index > s.Metadata.Index {
				// prevent "panic: runtime error: slice bounds out of range [:13038096702221461992] with capacity 0"
				up := entry.Index - s.Metadata.Index - 1
				if up > uint64(len(ents)) {
					// return error before append call causes runtime panic
					return h, nil, errors.New("entries out of bound")
				}
				// The line below is potentially overriding some 'uncommitted' entries.
				ents = append(ents[:up], entry)
			}
			f.enti = entry.Index

		case StateType:
			e := h.Unmarshal(rec.Data)
			if e != nil {
				panic(fmt.Sprintf("unmarshal should never fail (%v)", err))
			}
		case MetadataType:
		case CrcType:
		case SnapshotType:
			var snap raftpb.Snapshot
			e := snap.Unmarshal(rec.Data)
			if e != nil {
				panic(fmt.Sprintf("unmarshal should never fail (%v)", err))
			}
			if snap.Metadata.Index == s.Metadata.Index {
				if snap.Metadata.Term != s.Metadata.Term {
					h.Reset()
					return h, nil, errors.New("snap shot mismatch")
				}
				match = true
			}
		default:
			h.Reset()
			return h, nil, fmt.Errorf("unexpected block type %d", rec.Type)
		}
	}
	if !errors.Is(err, io.EOF) {
		h.Reset()
		return h, nil, err
	}

	// close decoder, disable reading
	if err = lr.Close(); err != nil {
		h.Reset()
		return h, nil, err
	}

	err = nil
	if !match {
		err = errors.New("snap shot mismatch")
	}
	f.lr = lr
	return h, ents, err
}
