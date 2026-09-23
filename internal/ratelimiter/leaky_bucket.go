package ratelimiter

import (
	"context"
	"sync"
	"time"
)

// LeakyBucket enforces a constant, smooth output rate.
// Unlike TokenBucket, it does NOT allow bursts. Even if the bucket is full,
// requests leak out at a fixed rate. This is useful when the downstream
// service requires a predictable, even load.
type LeakyBucket struct {
	mu       sync.Mutex
	rate     float64 // requests per second (leak rate)
	capacity int
	buckets  map[string]*leakyEntry
}

type leakyEntry struct {
	water    float64 // current water level (queued requests)
	lastLeak time.Time
}

func NewLeakyBucket(capacity int, ratePerSecond float64) *LeakyBucket {
	return &LeakyBucket{
		rate:     ratePerSecond,
		capacity: capacity,
		buckets:  make(map[string]*leakyEntry),
	}
}

func (lb *LeakyBucket) Allow(_ context.Context, key string) bool {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	now := time.Now()
	entry, ok := lb.buckets[key]

	if !ok {
		lb.buckets[key] = &leakyEntry{
			water:    1,
			lastLeak: now,
		}
		return true
	}

	// Leak water based on elapsed time
	elapsed := now.Sub(entry.lastLeak).Seconds()
	leaked := elapsed * lb.rate
	entry.water -= leaked
	if entry.water < 0 {
		entry.water = 0
	}
	entry.lastLeak = now

	if entry.water < float64(lb.capacity) {
		entry.water++
		return true
	}
	return false
}
