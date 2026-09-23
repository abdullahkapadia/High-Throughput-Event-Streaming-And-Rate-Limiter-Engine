package ratelimiter

import (
	"context"
	"sync"
	"time"
)

// TokenBucket allows bursts up to the bucket capacity, then refills at a steady rate.
// Difference from LeakyBucket: Token Bucket allows bursts (spend saved-up tokens fast),
// while Leaky Bucket enforces a constant output rate regardless of burst.
type TokenBucket struct {
	mu         sync.Mutex
	capacity   float64
	refillRate float64 // tokens per second
	buckets    map[string]*bucket
}

type bucket struct {
	tokens   float64
	lastFill time.Time
}

func NewTokenBucket(capacity int, refillRate float64) *TokenBucket {
	return &TokenBucket{
		capacity:   float64(capacity),
		refillRate: refillRate,
		buckets:    make(map[string]*bucket),
	}
}

func (tb *TokenBucket) Allow(_ context.Context, key string) bool {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	now := time.Now()
	b, ok := tb.buckets[key]

	if !ok {
		tb.buckets[key] = &bucket{
			tokens:   tb.capacity - 1, // consume one token
			lastFill: now,
		}
		return true
	}

	// Refill tokens based on elapsed time
	elapsed := now.Sub(b.lastFill).Seconds()
	b.tokens += elapsed * tb.refillRate
	if b.tokens > tb.capacity {
		b.tokens = tb.capacity
	}
	b.lastFill = now

	if b.tokens >= 1 {
		b.tokens--
		return true
	}
	return false
}
