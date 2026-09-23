package consumer

import (
	"context"
	"event-engine/internal/broker"
	"sync"
)

// Group represents a Consumer Group that coordinates partition assignment.
type Group struct {
	ID        string
	consumers []*Consumer
}

// NewGroup creates a new Consumer Group.
func NewGroup(id string, consumers []*Consumer) *Group {
	return &Group{
		ID:        id,
		consumers: consumers,
	}
}

// Assign distributes a topic's partitions across the consumers in this group.
// It implements a basic deterministic assignment (round-robin by partition ID).
func (g *Group) Assign(topic *broker.Topic) {
	numConsumers := len(g.consumers)
	if numConsumers == 0 {
		return
	}
	
	// Deterministic partition assignment: Hash/Modulo based on Partition ID
	for _, p := range topic.Partitions {
		consumerIdx := p.ID % numConsumers
		g.consumers[consumerIdx].assign(p)
	}
}

// Start launches all consumers in this group.
func (g *Group) Start(ctx context.Context, wg *sync.WaitGroup) {
	for _, c := range g.consumers {
		c.Start(ctx, wg)
	}
}
