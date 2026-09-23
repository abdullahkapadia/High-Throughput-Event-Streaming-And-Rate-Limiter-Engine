package consumer

import (
	"context"
	"event-engine/internal/broker"
	"event-engine/internal/event"
	"event-engine/internal/offset"
	"sync"
)

// Handler is a callback function for processing an event. Returns an error if processing fails.
type Handler func(e *event.Event) error

// Consumer reads events from assigned partitions concurrently.
type Consumer struct {
	ID         string
	partitions []*broker.Partition
	handler    Handler
	offsets    *offset.Manager
}

// NewConsumer creates a new consumer with a specific event handler and offset manager.
func NewConsumer(id string, handler Handler, offsets *offset.Manager) *Consumer {
	return &Consumer{
		ID:      id,
		handler: handler,
		offsets: offsets,
	}
}

// Assign links a partition to this consumer.
func (c *Consumer) Assign(p *broker.Partition) {
	c.partitions = append(c.partitions, p)
}

// ClearAssignments removes all partition assignments (used during rebalancing).
func (c *Consumer) ClearAssignments() {
	c.partitions = nil
}

// Start launches a goroutine for each assigned partition.
// Implements at-least-once delivery: process first, then commit offset.
func (c *Consumer) Start(ctx context.Context, wg *sync.WaitGroup) {
	for _, p := range c.partitions {
		wg.Add(1)
		go func(part *broker.Partition) {
			defer wg.Done()
			ch := part.Consume()

			for {
				select {
				case <-ctx.Done():
					return
				case e, ok := <-ch:
					if !ok {
						return
					}
					// At-least-once: process, then commit.
					// If we crash after processing but before commit, we replay and re-process.
					err := c.handler(e)
					if err == nil && c.offsets != nil {
						c.offsets.Commit(c.ID, part.ID, e.Offset)
					}
				}
			}
		}(p)
	}
}
