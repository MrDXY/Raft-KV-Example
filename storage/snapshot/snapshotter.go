package snapshot

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/golang/protobuf/proto"
	"go.etcd.io/raft/v3/raftpb"
)

const snapSuffix = ".snap"

var (
	ErrNoSnapshot = errors.New("snap: no available snapshot")
)

type Snapshotter interface {
	// Save snapshot to persist disk
	Save(snapshot *raftpb.Snapshot) error

	// Load snapshot from persist disk
	Load() (*raftpb.Snapshot, error)
}

type FileSnapshotter struct {
	dir string
}

func NewFileSnapshot(dir string) *FileSnapshotter {
	return &FileSnapshotter{dir: dir}
}

// Save saves the snapshot to the specified directory in both JSON and Protobuf formats.
func (fs *FileSnapshotter) Save(snapshot *raftpb.Snapshot) error {
	if err := os.MkdirAll(fs.dir, os.ModePerm); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}
	fname := fmt.Sprintf("%016x-%016x%s", snapshot.Metadata.Term, snapshot.Metadata.Index, snapSuffix)

	// Save as Protobuf
	protoPath := filepath.Join(fs.dir, fname)
	protoData, err := proto.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("failed to marshal snapshot to Protobuf: %w", err)
	}
	if err := os.WriteFile(protoPath, protoData, 0644); err != nil {
		return fmt.Errorf("failed to write Protobuf snapshot: %w", err)
	}

	return nil
}

// Load loads the snapshot from the specified directory (prefers Protobuf format).
func (fs *FileSnapshotter) Load() (*raftpb.Snapshot, error) {
	// get all the snap names
	dir, err := os.Open(fs.dir)
	if err != nil {
		return nil, err
	}
	defer dir.Close()
	names, err := dir.Readdirnames(-1)
	if err != nil {
		return nil, err
	}
	snaps := checkSuffix(names)
	if len(snaps) == 0 {
		return nil, ErrNoSnapshot
	}
	sort.Sort(sort.Reverse(sort.StringSlice(snaps)))

	// read the latest snapshot file
	var snapshot raftpb.Snapshot
	protoData, err := os.ReadFile(filepath.Join(fs.dir, snaps[0]))
	if err != nil {
		return nil, fmt.Errorf("failed to read Protobuf snapshot file: %w", err)
	}
	if err := proto.Unmarshal(protoData, &snapshot); err != nil {
		return nil, fmt.Errorf("failed to unmarshal Protobuf snapshot: %w", err)
	}
	log.Printf("loading snapshot at term %d and index %d", snapshot.Metadata.Term, snapshot.Metadata.Index)
	return &snapshot, nil
}

func checkSuffix(names []string) []string {
	var snaps []string
	for i := range names {
		if strings.HasSuffix(names[i], snapSuffix) {
			snaps = append(snaps, names[i])
		} else {
			fmt.Errorf("found unexpected non-snap file; skipping %s", names[i])
		}
	}
	return snaps
}

// TODO purge useless files
func (fs *FileSnapshotter) purge() {

}
