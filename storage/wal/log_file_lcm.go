package wal

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/mrdxy/raft-kv-example/util"
	"io"
	"os"
	"path/filepath"
	"strings"

	"go.etcd.io/raft/v3/raftpb"
)

// LogFileLCM is responsible for log file life cycle manage
type LogFileLCM struct {
	dir      string
	dirFile  *os.File
	fileSize int64
	count    int
	tail     *os.File
	tmpFilec chan *os.File
	errc     chan error
	donec    chan struct{}
}

func NewLogFileLCM(dir string, fileSize int64) (*LogFileLCM, error) {
	tailFile, err := openLatestLogFile(dir)
	if err != nil {
		return nil, err
	}
	// 	1.1.(n) create 0-0.wal file
	//  1.2.(y) read the last wal file
	if tailFile == nil {
		tailFile, err = initialLogFile(dir, fileSize)
		if err != nil {
			return nil, err
		}
	}
	err = tailFile.Close()
	if err != nil {
		return nil, err
	}
	dirFile, err := os.Open(dir)
	if err != nil {
		return nil, err
	}
	llcm := &LogFileLCM{
		dir:      dir,
		dirFile:  dirFile,
		fileSize: fileSize,
		tmpFilec: make(chan *os.File),
		errc:     make(chan error, 1),
		donec:    make(chan struct{}),
	}
	go llcm.runCreate()
	return llcm, nil
}

func (llcm *LogFileLCM) SetTail(file *os.File) {
	llcm.tail = file
}

// Cut will Cut the file as it reach to the pre-defined size limit
func (llcm *LogFileLCM) Cut(preCut func() error, initNew func(file *os.File) error, postCut func(file *os.File) error, index uint64) error {
	// 1. execute preCut function
	if preCut != nil {
		if err := preCut(); err != nil {
			return err
		}
	}

	// 2. close old log file; truncate to avoid wasting space if an early Cut
	tail := llcm.tail
	if tail != nil {
		//
		off, err := tail.Seek(0, io.SeekCurrent)
		if err != nil {
			return err
		}
		if err = tail.Truncate(off); err != nil {
			return err
		}
		if err = util.Fdatasync(tail); err != nil {
			return err
		}
		tail.Close()
	}

	// 3. create new log file
	tmpFile, err := llcm.createTmp()
	if err != nil {
		return err
	}

	// 4. init new log file
	if initNew != nil {
		if err := initNew(tmpFile); err != nil {
			return err
		}
	}

	// 5. rename tmp file
	off, err := tmpFile.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	nextName := filepath.Join(llcm.dir, FileName(llcm.seq()+1, index+1))
	if err = os.Rename(tmpFile.Name(), nextName); err != nil {
		return err
	}
	if err = util.Fsync(llcm.dirFile); err != nil {
		return err
	}

	// 6. reopen newTail with its new path so calls to Name() match the wal filename format
	if err := tmpFile.Close(); err != nil {
		return err
	}
	newTail, err := util.OpenFile(nextName, os.O_WRONLY, util.PrivateFileMode)
	if err != nil {
		return err
	}
	if _, err = newTail.Seek(off, io.SeekStart); err != nil {
		return err
	}
	llcm.tail = newTail
	if postCut != nil {
		if err := postCut(newTail); err != nil {
			return err
		}
	}
	return nil
}

func (llcm *LogFileLCM) Stop() error {
	if llcm.tail != nil {
		if err := llcm.tail.Close(); err != nil {
			return err
		}
	}
	if llcm.dirFile != nil {
		if err := llcm.dirFile.Close(); err != nil {
			return err
		}
	}
	close(llcm.donec)
	return <-llcm.errc
}

func initialLogFile(dir string, fileSize int64) (*os.File, error) {
	tmpdirpath := filepath.Clean(dir) + ".tmp"
	if util.Exist(tmpdirpath) {
		if err := os.RemoveAll(tmpdirpath); err != nil {
			return nil, err
		}
	}
	defer os.RemoveAll(tmpdirpath)
	if err := util.CreateDirAll(tmpdirpath); err != nil {
		return nil, err
	}
	tailName := filepath.Join(tmpdirpath, FileName(0, 0))
	f, err := util.OpenFile(tailName, os.O_WRONLY|os.O_CREATE, util.PrivateFileMode)
	if err != nil {
		return nil, err
	}
	err = util.Preallocate(f, fileSize, true)
	if err != nil {
		return nil, err
	}
	if err = os.RemoveAll(dir); err != nil {
		return nil, err
	}
	if err = os.Rename(tmpdirpath, dir); err != nil {
		return nil, err
	}
	return f, nil
}

func closeAll(rcs ...*os.File) error {
	stringArr := make([]string, 0)
	for _, f := range rcs {
		if err := f.Close(); err != nil {
			stringArr = append(stringArr, err.Error())
		}
	}
	if len(stringArr) == 0 {
		return nil
	}
	return errors.New(strings.Join(stringArr, ", "))
}

// createTmp returns a fresh file for writing. Rename the file before calling
// createTmp again or there will be file collisions.
// it will 'block' if the tmp file lock is already taken.
func (llcm *LogFileLCM) createTmp() (f *os.File, err error) {
	select {
	case f = <-llcm.tmpFilec:
	case err = <-llcm.errc:
	}
	return f, err
}

func (llcm *LogFileLCM) alloc() (f *os.File, err error) {
	// count % 2 so this file isn't the same as the one last published
	tmpFilePath := filepath.Join(llcm.dir, fmt.Sprintf("%d.tmp", llcm.count%2))
	if f, err = util.OpenFile(tmpFilePath, os.O_CREATE|os.O_WRONLY, util.PrivateFileMode); err != nil {
		return nil, err
	}
	if err = util.Preallocate(f, llcm.fileSize, true); err != nil {
		_ = f.Close()
		return nil, err
	}
	llcm.count++
	return f, nil
}

func (llcm *LogFileLCM) runCreate() {
	defer close(llcm.errc)
	for {
		f, err := llcm.alloc()
		if err != nil {
			llcm.errc <- err
			return
		}
		select {
		case llcm.tmpFilec <- f:
		case <-llcm.donec:
			_ = os.Remove(f.Name())
			_ = f.Close()
			return
		}
	}
}

func (llcm *LogFileLCM) seq() uint64 {
	t := llcm.tail
	if t == nil {
		return 0
	}
	seq, _, _ := parseName(filepath.Base(t.Name()))
	return seq
}

func checkNames(names []string) []string {
	wnames := make([]string, 0)
	for _, name := range names {
		if _, _, err := parseName(name); err != nil {
			_ = fmt.Errorf("ignore file: %s", name)
			continue
		}
		wnames = append(wnames, name)
	}
	return wnames
}

func openLatestLogFile(dir string) (*os.File, error) {
	names, err := listNames(dir)
	if err != nil {
		return nil, fmt.Errorf("listNames failed: %w", err)
	}
	if len(names) == 0 {
		return nil, nil
	}
	if !isValidSeq(names) {
		return nil, fmt.Errorf("wal: file sequence numbers do not increase continuously")
	}
	return util.OpenFile(filepath.Join(dir, names[len(names)-1]), os.O_RDWR, util.PrivateFileMode)
}

func openReadOnlyLogFiles(dir string, names []string, nameIndex int) ([]*bufio.Reader, []*os.File, func() error, error) {
	files := make([]*os.File, 0)
	rds := make([]*bufio.Reader, 0)
	for _, name := range names[nameIndex:] {
		p := filepath.Join(dir, name)
		rf, err := os.OpenFile(p, os.O_RDONLY, util.PrivateFileMode)
		if err != nil {
			err := closeAll(files...)
			if err != nil {
				return nil, nil, nil, err
			}
			return nil, nil, nil, fmt.Errorf("[openReadOnlyLogFiles] os.OpenFile failed (%q): %w", p, err)
		}
		files = append(files, rf)
		rd := bufio.NewReader(rf)
		rds = append(rds, rd)
	}

	closer := func() error { return closeAll(files...) }

	return rds, files, closer, nil
}

func selectFileBeforeSnap(dir string, snap raftpb.SnapshotMetadata) ([]string, int, error) {
	names, err := listNames(dir)
	if err != nil {
		return nil, -1, fmt.Errorf("listNames failed: %w", err)
	}

	nameIndex, ok := searchIndex(names, snap.Index)
	if !ok {
		return nil, -1, fmt.Errorf("wal: file not found which matches the snapshot index '%d'", snap.Index)
	}

	if !isValidSeq(names[nameIndex:]) {
		return nil, -1, fmt.Errorf("wal: file sequence numbers (starting from %d) do not increase continuously", nameIndex)
	}

	return names, nameIndex, nil
}

// searchIndex returns the last array index of names whose raft index section is
// equal to or smaller than the given index.
// The given names MUST be sorted.
func searchIndex(names []string, index uint64) (int, bool) {
	for i := len(names) - 1; i >= 0; i-- {
		name := names[i]
		_, curIndex, err := parseName(name)
		if err != nil {
			_ = fmt.Errorf("failed to parse log file name, %v", err)
		}
		if index >= curIndex {
			return i, true
		}
	}
	return -1, false
}

// names should have been sorted based on sequence number.
// isValidSeq checks whether seq increases continuously.
func isValidSeq(names []string) bool {
	var lastSeq uint64
	for _, name := range names {
		curSeq, _, err := parseName(name)
		if err != nil {
			_ = fmt.Errorf("failed to parse log file name, %v", err)
		}
		if lastSeq != 0 && lastSeq != curSeq-1 {
			return false
		}
		lastSeq = curSeq
	}
	return true
}

func listNames(dirpath string) ([]string, error) {
	if !util.Exist(dirpath) {
		return nil, nil
	}

	names, err := util.ReadDirInc(dirpath)
	if err != nil {
		return nil, err
	}
	wnames := checkNames(names)
	return wnames, nil
}

func parseName(str string) (seq, index uint64, err error) {
	if !strings.HasSuffix(str, ".walpb") {
		return 0, 0, errors.New("not a valid log file")
	}
	_, err = fmt.Sscanf(str, "%016x-%016x.walpb", &seq, &index)
	return seq, index, err
}

func FileName(seq, index uint64) string {
	return fmt.Sprintf("%016x-%016x.walpb", seq, index)
}
