package wal

import (
	"bufio"
	"encoding/binary"
	"github.com/mrdxy/raft-kv-example/pb/walpb"
	"io"
	"os"

	"go.etcd.io/raft/v3/raftpb"
)

const (
	// frameSizeBytes is frame size in bytes, including record size and padding size.
	frameSizeBytes = 8
)

type LogFileReader struct {
	rds        []*bufio.Reader
	files      []*os.File
	readOffset int64
	closer     func() error
}

func NewLogFileReader(dir string, snapshot raftpb.SnapshotMetadata) (*LogFileReader, error) {
	names, index, err := selectFileBeforeSnap(dir, snapshot)
	if err != nil {
		return nil, err
	}
	readers, files, closer, err := openReadOnlyLogFiles(dir, names, index)
	if err != nil {
		return nil, err
	}
	return &LogFileReader{
		rds:    readers,
		closer: closer,
		files:  files,
	}, nil
}

func (lr *LogFileReader) Read(rec *walpb.Record) error {
	rec.Reset()
	return lr.readRecord(rec)
}

func (lr *LogFileReader) Tail() *os.File {
	return lr.files[len(lr.files)-1]
}

func (lr *LogFileReader) TailOffset() int64 {
	return lr.readOffset
}

func (lr *LogFileReader) Close() error {
	if err := lr.closer(); err != nil {
		return err
	}
	return nil
}

func (lr *LogFileReader) readRecord(rec *walpb.Record) error {
	if len(lr.rds) == 0 {
		return io.EOF
	}

	fileBufReader := lr.rds[0]
	l, err := readInt64(fileBufReader)
	if err == io.EOF || (err == nil && l == 0) {
		// hit end of file or preallocated space
		lr.rds = lr.rds[1:]
		if len(lr.rds) == 0 {
			return io.EOF
		}
		lr.readOffset = 0
		return lr.readRecord(rec)
	}
	if err != nil {
		return err
	}

	recBytes, padBytes := decodeFrameSize(l)

	data := make([]byte, recBytes+padBytes)
	if _, err = io.ReadFull(fileBufReader, data); err != nil {
		// ReadFull returns io.EOF only if no bytes were read
		// the decoder should treat this as an ErrUnexpectedEOF instead.
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return err
	}
	if err := rec.Unmarshal(data[:recBytes]); err != nil {
		return err
	}
	lr.readOffset += frameSizeBytes + recBytes + padBytes
	return nil
}

func decodeFrameSize(lenField int64) (recBytes int64, padBytes int64) {
	// the record size is stored in the lower 56 bits of the 64-bit length
	recBytes = int64(uint64(lenField) & ^(uint64(0xff) << 56))
	// non-zero padding is indicated by set MSb / a negative length
	if lenField < 0 {
		// padding is stored in lower 3 bits of length MSB
		padBytes = int64((uint64(lenField) >> 56) & 0x7)
	}
	return recBytes, padBytes
}

func readInt64(r io.Reader) (int64, error) {
	var n int64
	err := binary.Read(r, binary.LittleEndian, &n)
	return n, err
}
