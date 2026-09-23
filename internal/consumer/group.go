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

func NewGroup(id string, consumers []*Consumer) *Group {
	return &Group{
		ID:        id,
		consumers: consumers,
	}
}

// Assign distributes a topic's partitions across the consumers in this group.
// Uses round-robin: partition i goes to consumer i % len(consumers).
// If there are more consumers than partitions, excess consumers sit idle.
func (g *Group) Assign(topic *broker.Topic) {
	numConsumers := len(g.consumers)
	if numConsumers == 0 {
		return
	}

	for _, c := range g.consumers {
		c.ClearAssignments()
	}

	for _, p := range topic.Partitions {
		consumerIdx := p.ID % numConsumers
		g.consumers[consumerIdx].Assign(p)
	}
}

// Rebalance clears all assignments and redistributes. Call when consumers join or leave.
func (g *Group) Rebalance(topic *broker.Topic) {
	g.Assign(topic)
}

// AddConsumer adds a consumer to the group.
func (g *Group) AddConsumer(c *Consumer) {
	g.consumers = append(g.consumers, c)
}

// RemoveConsumer removes a consumer by ID from the group.
func (g *Group) RemoveConsumer(id string) {
	for i, c := range g.consumers {
		if c.ID == id {
			g.consumers = append(g.consumers[:i], g.consumers[i+1:]...)
			return
		}
	}
}

func (g *Group) Start(ctx context.Context, wg *sync.WaitGroup) {
	for _, c := range g.consumers {
		c.Start(ctx, wg)
	}
}
