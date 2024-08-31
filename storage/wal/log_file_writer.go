package wal

import (
	"encoding/binary"
	"github.com/mrdxy/raft-kv-example/pb/walpb"
	"github.com/mrdxy/raft-kv-example/util"
	"hash"
	"io"
	"os"
)

var (
	minSectorSize = 512
	logPageBytes  = 8 * minSectorSize
)

type LogFileWriter struct {
	file      *os.File
	offset    int64
	pw        *PageWriter
	crc       hash.Hash32
	buf       []byte
	uint64buf []byte
}

func NewLogFileWriter(f *os.File, offset int64) (*LogFileWriter, error) {
	_, err := f.Seek(offset, io.SeekStart)
	if err != nil {
		return nil, err
	}
	return &LogFileWriter{
		file:      f,
		offset:    offset,
		pw:        NewPageWriter(f, logPageBytes, int(offset)),
		buf:       make([]byte, 1024*1024), // 1MB buffer
		uint64buf: make([]byte, 8),
	}, nil
}

func (lw *LogFileWriter) Write(record *walpb.Record) error {
	var (
		data []byte
		err  error
		n    int
	)

	// use buffer can reduce new memory allocation
	if record.Size() > len(lw.buf) {
		data, err = record.Marshal()
		if err != nil {
			return err
		}
	} else {
		n, err = record.MarshalTo(lw.buf)
		if err != nil {
			return err
		}
		data = lw.buf[:n]
	}

	data, lenField := prepareDataWithPadding(data)

	// write padding info
	var paddingInfo = make([]byte, 8)
	binary.LittleEndian.PutUint64(paddingInfo, lenField)
	_, err = lw.pw.Write(paddingInfo)
	if err != nil {
		return err
	}

	// write the record with padding
	_, err = lw.pw.Write(data)
	return err
}

func (lw *LogFileWriter) Flush() error {
	return lw.pw.Flush()
}

func (lw *LogFileWriter) Sync() error {
	if err := lw.Flush(); err != nil {
		return err
	}
	if err := util.Fdatasync(lw.file); err != nil {
		return err
	}
	return nil
}

func (lw *LogFileWriter) CurOffset() (int64, error) {
	return lw.file.Seek(0, io.SeekCurrent)
}

func (lw *LogFileWriter) Close() error {
	return lw.file.Close()
}

func prepareDataWithPadding(data []byte) ([]byte, uint64) {
	lenField, padBytes := encodeFrameSize(len(data))
	if padBytes != 0 {
		data = append(data, make([]byte, padBytes)...)
	}
	return data, lenField
}

// encodeFrameSize calculates the padding info, and put it into the first byte of lenField
// since unsigned int 64 is quite big, first byte is very unlikely to be used.
func encodeFrameSize(dataBytes int) (lenField uint64, padBytes int) {
	lenField = uint64(dataBytes)
	// force 8 byte alignment so length never gets a torn write
	padBytes = (8 - (dataBytes % 8)) % 8
	if padBytes != 0 {
		lenField |= uint64(0x80|padBytes) << 56
	}
	return lenField, padBytes
}
