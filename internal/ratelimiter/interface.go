package ratelimiter

import "context"

// RateLimiter is the common interface for all rate limiting algorithms.
type RateLimiter interface {
	Allow(ctx context.Context, key string) bool
}
