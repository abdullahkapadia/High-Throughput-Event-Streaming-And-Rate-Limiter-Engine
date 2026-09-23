package ratelimiter

import (
	"context"
	"sync"
	"time"
)

// SlidingWindow solves the fixed window boundary burst problem by weighting
// the previous window's count into the current window.
// effectiveCount = prevCount * overlapRatio + currentCount
// This gives a smoother approximation of the true request rate.
type SlidingWindow struct {
	mu     sync.Mutex
	limit  int
	window time.Duration
	state  map[string]*slidingWindowEntry
}

type slidingWindowEntry struct {
	prevCount    int
	currCount    int
	currStart    time.Time
	prevStart    time.Time
}

func NewSlidingWindow(limit int, window time.Duration) *SlidingWindow {
	return &SlidingWindow{
		limit:  limit,
		window: window,
		state:  make(map[string]*slidingWindowEntry),
	}
}

func (sw *SlidingWindow) Allow(_ context.Context, key string) bool {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	now := time.Now()
	entry, ok := sw.state[key]

	if !ok {
		sw.state[key] = &slidingWindowEntry{
			currCount: 1,
			currStart: now,
		}
		return true
	}

	// If we've moved past the current window, rotate
	if now.Sub(entry.currStart) >= sw.window {
		entry.prevCount = entry.currCount
		entry.prevStart = entry.currStart
		entry.currCount = 0
		entry.currStart = now
	}

	// Calculate weighted count from previous window overlap
	elapsed := now.Sub(entry.currStart)
	overlapRatio := 1.0 - (float64(elapsed) / float64(sw.window))
	if overlapRatio < 0 {
		overlapRatio = 0
	}

	effectiveCount := float64(entry.prevCount)*overlapRatio + float64(entry.currCount)

	if effectiveCount < float64(sw.limit) {
		entry.currCount++
		return true
	}
	return false
}
