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

	cancelCtx context.CancelFunc
	wg        sync.WaitGroup
	mu        sync.Mutex
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
func (c *Consumer) Start(ctx context.Context) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Stop any existing consumption before restarting
	if c.cancelCtx != nil {
		c.stopInternal()
	}

	ctx, cancel := context.WithCancel(ctx)
	c.cancelCtx = cancel

	for _, p := range c.partitions {
		c.wg.Add(1)
		go func(part *broker.Partition) {
			defer c.wg.Done()
			
			// Simulate restarting by rewinding the partition read pointer to the committed offset
			// In a real system, the partition/broker would handle this, but here the partition 
			// just streams new events to 'ch'. However, for replay, we actually need to 
			// fetch from the partition's log. 
			// Wait, in our current design Partition.Consume() returns a channel of live events.
			// It doesn't replay from an offset automatically! 
			// We need to implement replay from offset in the consumer loop.
			
			// Let's do a basic replay loop first.
			startOffset := uint64(0)
			if c.offsets != nil {
				startOffset = c.offsets.GetCommitted(c.ID, part.ID)
			}
			
			// Replay loop
			for offset := startOffset; offset < part.CurrentOffset(); offset++ {
				e := part.ReadAt(offset)
				if e == nil {
					continue
				}
				
				select {
				case <-ctx.Done():
					return
				default:
					err := c.handler(e)
					if err == nil && c.offsets != nil {
						c.offsets.Commit(c.ID, part.ID, e.Offset+1) // Commit next offset
					}
				}
			}

			// Live consume loop
			ch := part.Consume()
			for {
				select {
				case <-ctx.Done():
					return
				case e, ok := <-ch:
					if !ok {
						return
					}
					// Check if we already processed this offset during replay
					if c.offsets != nil {
						committed := c.offsets.GetCommitted(c.ID, part.ID)
						if e.Offset < committed {
							continue // Skip already processed
						}
					}
					
					err := c.handler(e)
					if err == nil && c.offsets != nil {
						c.offsets.Commit(c.ID, part.ID, e.Offset+1)
					}
				}
			}
		}(p)
	}
}

func (c *Consumer) stopInternal() {
	if c.cancelCtx != nil {
		c.cancelCtx()
		c.wg.Wait()
		c.cancelCtx = nil
	}
}

// Stop halts the consumer and waits for all partition goroutines to exit.
func (c *Consumer) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stopInternal()
}
