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
	mu        sync.Mutex
}

func NewGroup(id string, consumers []*Consumer) *Group {
	return &Group{
		ID:        id,
		consumers: consumers,
	}
}

// Assign distributes a topic's partitions across the consumers in this group.
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

// Rebalance clears all assignments, stops consumers, redistributes, and restarts.
func (g *Group) Rebalance(topic *broker.Topic, ctx context.Context) {
	g.mu.Lock()
	defer g.mu.Unlock()

	for _, c := range g.consumers {
		c.Stop()
	}

	g.Assign(topic)

	for _, c := range g.consumers {
		c.Start(ctx)
	}
}

// AddConsumer adds a consumer to the group.
func (g *Group) AddConsumer(c *Consumer) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.consumers = append(g.consumers, c)
}

// RemoveConsumer removes a consumer by ID from the group and stops it.
func (g *Group) RemoveConsumer(id string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	
	for i, c := range g.consumers {
		if c.ID == id {
			c.Stop()
			g.consumers = append(g.consumers[:i], g.consumers[i+1:]...)
			return
		}
	}
}

func (g *Group) Start(ctx context.Context) {
	g.mu.Lock()
	defer g.mu.Unlock()
	
	for _, c := range g.consumers {
		c.Start(ctx)
	}
}

func (g *Group) Stop() {
	g.mu.Lock()
	defer g.mu.Unlock()
	
	for _, c := range g.consumers {
		c.Stop()
	}
}
