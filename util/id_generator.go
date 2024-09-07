package util

import (
	"sync/atomic"
	"time"
)

// SnowflakeIDGenerator is a thread-safe ID generator with semantic ID
type SnowflakeIDGenerator struct {
	lastTimestamp uint64 // Stores the last generated timestamp
	sequence      uint64 // Sequence number for IDs generated within the same millisecond
	nodeID        uint64 // Unique ID of the node or machine (0 - 1023)
}

const (
	nodeIDBits     = 10 // Number of bits for NodeID (1024 nodes)
	sequenceBits   = 12 // Number of bits for sequence (4096 IDs per millisecond)
	maxNodeID      = -1 ^ (-1 << nodeIDBits)
	maxSequence    = -1 ^ (-1 << sequenceBits)
	nodeIDShift    = sequenceBits
	timestampShift = sequenceBits + nodeIDBits
	sequenceMask   = maxSequence
)

// NewSnowflakeIDGenerator creates a new ID generator with a node ID
func NewSnowflakeIDGenerator(nodeID uint64) *SnowflakeIDGenerator {
	if nodeID > maxNodeID {
		panic("Node ID out of range")
	}
	return &SnowflakeIDGenerator{nodeID: nodeID}
}

// GenerateID generates a unique and ordered ID with time, node, and sequence
func (g *SnowflakeIDGenerator) GenerateID() uint64 {
	timestamp := uint64(time.Now().UnixNano() / 1e6) // Millisecond precision timestamp

	// Protect the critical section with atomic operations
	atomicTimestamp := atomic.LoadUint64(&g.lastTimestamp)
	if timestamp == atomicTimestamp {
		// Increment sequence if it's the same millisecond
		sequence := atomic.AddUint64(&g.sequence, 1) & sequenceMask
		if sequence == 0 {
			// Sequence overflow within the same millisecond, wait for next millisecond
			for timestamp <= atomicTimestamp {
				timestamp = uint64(time.Now().UnixNano() / 1e6)
			}
		}
	} else {
		// Reset sequence for a new timestamp
		atomic.StoreUint64(&g.sequence, 0)
	}

	// Update last timestamp
	atomic.StoreUint64(&g.lastTimestamp, timestamp)

	// Construct the ID (timestamp | nodeID | sequence)
	id := (timestamp << timestampShift) | (g.nodeID << nodeIDShift) | (atomic.LoadUint64(&g.sequence))

	return id
}
