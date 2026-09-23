package consumer

import (
	"context"
	"event-engine/internal/broker"
	"event-engine/internal/event"
	"sync"
)

// Handler is a callback function for processing an event.
type Handler func(e *event.Event)

// Consumer reads events from assigned partitions concurrently.
type Consumer struct {
	ID         string
	partitions []*broker.Partition
	handler    Handler
}

// NewConsumer creates a new consumer with a specific event handler.
func NewConsumer(id string, handler Handler) *Consumer {
	return &Consumer{
		ID:      id,
		handler: handler,
	}
}

// assign is an internal method to link a partition to this consumer.
func (c *Consumer) assign(p *broker.Partition) {
	c.partitions = append(c.partitions, p)
}

// Start launches a goroutine for each assigned partition.
// It listens until the context is canceled or the partition channels are closed.
func (c *Consumer) Start(ctx context.Context, wg *sync.WaitGroup) {
	for _, p := range c.partitions {
		wg.Add(1)
		go func(part *broker.Partition) {
			defer wg.Done()
			ch := part.Consume()
			
			for {
				select {
				case <-ctx.Done():
					return // Graceful shutdown initiated via context
				case e, ok := <-ch:
					if !ok {
						return // Partition was closed
					}
					c.handler(e)
				}
			}
		}(p)
	}
}
