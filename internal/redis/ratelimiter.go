package redis

import (
	"context"
	"log"
)

// FailurePolicy defines system behavior when Redis is unreachable.
//
// fail-open: Allow all requests when Redis is down. Prioritizes availability.
//   Risk: Rate limiting is effectively disabled during an outage.
//
// fail-closed: Reject all requests when Redis is down. Prioritizes safety.
//   Risk: Complete service denial during a Redis outage.
type FailurePolicy int

const (
	FailOpen   FailurePolicy = iota // allow requests if Redis is unavailable
	FailClosed                      // reject requests if Redis is unavailable
)

// RateLimiter wraps a Redis-backed rate limiting implementation with failure handling.
type RateLimiter struct {
	client *Client
	policy FailurePolicy
	impl   LuaRateLimiter
}

// LuaRateLimiter defines the contract for a Redis+Lua rate limiting algorithm.
type LuaRateLimiter interface {
	Allow(ctx context.Context, client *Client, key string) (bool, error)
}

// NewRateLimiter creates a Redis rate limiter with the given failure policy.
func NewRateLimiter(client *Client, policy FailurePolicy, impl LuaRateLimiter) *RateLimiter {
	return &RateLimiter{
		client: client,
		policy: policy,
		impl:   impl,
	}
}

// Allow checks the rate limit via Redis. On Redis failure, falls back to the configured policy.
func (rl *RateLimiter) Allow(ctx context.Context, key string) bool {
	allowed, err := rl.impl.Allow(ctx, rl.client, key)
	if err != nil {
		log.Printf("[ratelimiter] redis error: %v, policy=%d", err, rl.policy)
		// Fail-open: let the request through. Fail-closed: block it.
		return rl.policy == FailOpen
	}
	return allowed
}
