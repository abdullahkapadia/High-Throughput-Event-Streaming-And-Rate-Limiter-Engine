package producer

import (
	"context"
	"event-engine/internal/broker"
	"event-engine/internal/event"
	"fmt"
	"sync/atomic"
	"time"
)

// Producer is responsible for constructing events and sending them to the broker.
type Producer struct {
	broker  broker.Broker // now uses interface
	id      string
	counter uint64
}

// NewProducer creates a new Producer instance.
func NewProducer(id string, b broker.Broker) *Producer {
	return &Producer{
		broker: b,
		id:     id,
	}
}

// Send constructs an Event and passes it to the broker.
func (p *Producer) Send(ctx context.Context, topic, key string, payload []byte) error {
	seq := atomic.AddUint64(&p.counter, 1)
	
	e := &event.Event{
		ID:        fmt.Sprintf("%s-%d", p.id, seq),
		Type:      "GenericEvent",
		Topic:     topic,
		Key:       key,
		Timestamp: time.Now(),
		Payload:   payload,
	}
	
	return p.broker.Produce(ctx, e)
}
