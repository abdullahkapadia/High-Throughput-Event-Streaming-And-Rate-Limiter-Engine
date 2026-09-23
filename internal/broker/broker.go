package broker

import (
	"context"
	"errors"
	"event-engine/internal/event"
	"event-engine/internal/partitioner"
	"sync"
)

var (
	ErrTopicNotFound = errors.New("topic not found")
)

// Broker defines the contract for an event streaming backend.
// This interface allows us to seamlessly swap between the custom MemoryBroker and Apache Kafka.
type Broker interface {
	CreateTopic(ctx context.Context, name string, numPartitions int, capacity int) error
	Produce(ctx context.Context, e *event.Event) error
	GetTopic(name string) *Topic
	Close() error
}

// MemoryBroker is the custom, in-memory implementation of the Broker interface for learning and benchmarking.
type MemoryBroker struct {
	topics      map[string]*Topic
	mu          sync.RWMutex
	partitioner partitioner.Partitioner
}

// NewMemoryBroker creates a new in-memory broker with the given partitioner.
func NewMemoryBroker(p partitioner.Partitioner) *MemoryBroker {
	return &MemoryBroker{
		topics:      make(map[string]*Topic),
		partitioner: p,
	}
}

// CreateTopic initializes a new topic with the specified number of partitions and capacity.
func (b *MemoryBroker) CreateTopic(ctx context.Context, name string, numPartitions int, capacity int) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, exists := b.topics[name]; !exists {
		b.topics[name] = NewTopic(name, numPartitions, capacity)
	}
	return nil
}

// GetTopic safely retrieves a topic by name.
func (b *MemoryBroker) GetTopic(name string) *Topic {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.topics[name]
}

// Produce routes an event to the correct partition based on its key.
func (b *MemoryBroker) Produce(ctx context.Context, e *event.Event) error {
	b.mu.RLock()
	topic, exists := b.topics[e.Topic]
	b.mu.RUnlock()

	if !exists {
		return ErrTopicNotFound
	}

	partID := b.partitioner.Partition(e.Key, len(topic.Partitions))
	part := topic.GetPartition(partID)

	// Append safely blocks if the partition is at capacity (Backpressure)
	return part.Append(ctx, e)
}

// Close gracefully closes all partitions in all topics.
func (b *MemoryBroker) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, t := range b.topics {
		for _, p := range t.Partitions {
			p.Close()
		}
	}
	return nil
}
