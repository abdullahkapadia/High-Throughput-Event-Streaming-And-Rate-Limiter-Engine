package partitioner

import (
	"hash/fnv"
)

// Partitioner defines the interface for routing an event to a partition based on its key.
type Partitioner interface {
	Partition(key string, numPartitions int) int
}

// HashPartitioner implements deterministic hashing so the same key always goes to the same partition.
type HashPartitioner struct{}

// Partition computes the partition ID using FNV-1a hash.
func (hp *HashPartitioner) Partition(key string, numPartitions int) int {
	if numPartitions <= 0 {
		return 0
	}
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32()) % numPartitions
}
