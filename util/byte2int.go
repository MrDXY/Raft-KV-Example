package util

import (
	"bytes"
	"encoding/binary"
	"log"
)

// Int64ToBytes for int64 to byte array
func Int64ToBytes(n uint64) []byte {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.BigEndian, n)
	if err != nil {
		log.Fatal("binary.Write failed:", err)
	}
	return buf.Bytes()
}

// BytesToInt64 for byte array to int64
func BytesToInt64(b []byte) uint64 {
	buf := bytes.NewReader(b)
	var n uint64
	err := binary.Read(buf, binary.BigEndian, &n)
	if err != nil {
		log.Fatal("binary.Read failed:", err)
	}
	return n
}
