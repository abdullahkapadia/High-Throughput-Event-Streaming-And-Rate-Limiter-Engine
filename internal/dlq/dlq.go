package dlq

import (
	"event-engine/internal/event"
	"sync"
	"time"
)

// Entry represents a failed event stored in the Dead Letter Queue.
type Entry struct {
	Event         *event.Event
	OriginalTopic string
	Partition     int
	Offset        uint64
	Error         string
	RetryCount    int
	FailedAt      time.Time
}

// Queue is an in-memory Dead Letter Queue.
type Queue struct {
	mu      sync.RWMutex
	entries []Entry
}

func NewQueue() *Queue {
	return &Queue{
		entries: make([]Entry, 0),
	}
}

// Push adds a failed event to the DLQ.
func (q *Queue) Push(e *event.Event, err error, retryCount int) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.entries = append(q.entries, Entry{
		Event:         e,
		OriginalTopic: e.Topic,
		Partition:     e.Partition,
		Offset:        e.Offset,
		Error:         err.Error(),
		RetryCount:    retryCount,
		FailedAt:      time.Now(),
	})
}

// List returns all DLQ entries (snapshot).
func (q *Queue) List() []Entry {
	q.mu.RLock()
	defer q.mu.RUnlock()
	out := make([]Entry, len(q.entries))
	copy(out, q.entries)
	return out
}

// Len returns the number of entries in the DLQ.
func (q *Queue) Len() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return len(q.entries)
}
