package broker

import (
	"context"
	"errors"
	"event-engine/internal/event"
	"sync"
	"sync/atomic"
)

var ErrPartitionFull = errors.New("partition is full")

// BackpressureStrategy defines how a partition behaves when its buffer is full.
type BackpressureStrategy int

const (
	Block  BackpressureStrategy = iota // Producer blocks until space is available
	Reject                             // Producer gets an error immediately
	Drop                               // Event is silently dropped
)

type Partition struct {
	ID       int
	events   []*event.Event
	ch       chan *event.Event
	offset   uint64
	mu       sync.Mutex
	strategy BackpressureStrategy
	closed   int32
}

func NewPartition(id int, capacity int) *Partition {
	return &Partition{
		ID:       id,
		events:   make([]*event.Event, 0, capacity),
		ch:       make(chan *event.Event, capacity),
		strategy: Block,
	}
}

// SetBackpressure changes the strategy used when the partition buffer is full.
func (p *Partition) SetBackpressure(s BackpressureStrategy) {
	p.mu.Lock()
	p.strategy = s
	p.mu.Unlock()
}

// Append adds an event to the partition. Behavior when full depends on BackpressureStrategy.
func (p *Partition) Append(ctx context.Context, e *event.Event) error {
	p.mu.Lock()
	e.Partition = p.ID
	e.Offset = p.offset
	p.offset++
	strategy := p.strategy
	p.events = append(p.events, e)
	p.mu.Unlock()

	switch strategy {
	case Block:
		select {
		case p.ch <- e:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	case Reject:
		select {
		case p.ch <- e:
			return nil
		default:
			return ErrPartitionFull
		}
	case Drop:
		select {
		case p.ch <- e:
		default:
			// silently dropped
		}
		return nil
	}
	return nil
}

// ReadAt retrieves an event at the given offset from the persisted log.
func (p *Partition) ReadAt(offset uint64) *event.Event {
	p.mu.Lock()
	defer p.mu.Unlock()
	if offset >= uint64(len(p.events)) {
		return nil
	}
	return p.events[offset]
}

// CurrentOffset returns the next offset that will be assigned.
func (p *Partition) CurrentOffset() uint64 {
	return atomic.LoadUint64(&p.offset)
}

func (p *Partition) Consume() <-chan *event.Event {
	return p.ch
}

func (p *Partition) Close() {
	if atomic.CompareAndSwapInt32(&p.closed, 0, 1) {
		close(p.ch)
	}
}
