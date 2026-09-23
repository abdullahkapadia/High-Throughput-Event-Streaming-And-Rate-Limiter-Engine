package broker

import (
	"context"
	"event-engine/internal/event"
	"sync"
)

// Partition holds an ordered channel of events and manages its own offset.
type Partition struct {
	ID     int
	ch     chan *event.Event
	offset uint64
	mu     sync.Mutex
}

// NewPartition creates a new partition with the given ID and capacity for backpressure.
func NewPartition(id int, capacity int) *Partition {
	return &Partition{
		ID: id,
		ch: make(chan *event.Event, capacity),
	}
}

// Append adds an event to the partition, assigning it the correct partition ID and an increasing offset.
// If the partition is at capacity, it blocks until space is available or the context is canceled (Backpressure).
func (p *Partition) Append(ctx context.Context, e *event.Event) error {
	p.mu.Lock()
	e.Partition = p.ID
	e.Offset = p.offset
	p.offset++
	p.mu.Unlock()

	select {
	case p.ch <- e:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Consume returns a read-only channel to read events from this partition.
func (p *Partition) Consume() <-chan *event.Event {
	return p.ch
}

// Close gracefully closes the partition channel.
func (p *Partition) Close() {
	close(p.ch)
}
