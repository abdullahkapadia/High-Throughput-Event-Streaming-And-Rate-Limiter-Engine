package ratelimiter

import (
	"context"
	"sync"
	"time"
)

// FixedWindow allows N requests per window duration.
// Limitation: boundary burst — a user can make N requests at the end of one window
// and N requests at the start of the next, effectively getting 2N requests in a short span.
type FixedWindow struct {
	mu     sync.Mutex
	limit  int
	window time.Duration
	counts map[string]*fixedWindowEntry
}

type fixedWindowEntry struct {
	count     int
	windowEnd time.Time
}

func NewFixedWindow(limit int, window time.Duration) *FixedWindow {
	return &FixedWindow{
		limit:  limit,
		window: window,
		counts: make(map[string]*fixedWindowEntry),
	}
}

func (fw *FixedWindow) Allow(_ context.Context, key string) bool {
	fw.mu.Lock()
	defer fw.mu.Unlock()

	now := time.Now()
	entry, ok := fw.counts[key]

	if !ok || now.After(entry.windowEnd) {
		fw.counts[key] = &fixedWindowEntry{
			count:     1,
			windowEnd: now.Add(fw.window),
		}
		return true
	}

	if entry.count < fw.limit {
		entry.count++
		return true
	}
	return false
}
