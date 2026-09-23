package broker

// Topic groups multiple partitions under a single logical name.
type Topic struct {
	Name       string
	Partitions []*Partition
}

// NewTopic creates a new topic with the specified number of partitions and capacity per partition.
func NewTopic(name string, numPartitions int, capacity int) *Topic {
	partitions := make([]*Partition, numPartitions)
	for i := 0; i < numPartitions; i++ {
		partitions[i] = NewPartition(i, capacity)
	}
	
	return &Topic{
		Name:       name,
		Partitions: partitions,
	}
}

// GetPartition safely retrieves a partition by its ID. Returns nil if out of bounds.
func (t *Topic) GetPartition(id int) *Partition {
	if id < 0 || id >= len(t.Partitions) {
		return nil
	}
	return t.Partitions[id]
}
