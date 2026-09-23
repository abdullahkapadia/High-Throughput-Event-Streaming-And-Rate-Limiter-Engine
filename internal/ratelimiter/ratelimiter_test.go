package ratelimiter

import (
	"context"
	"testing"
	"time"
)

func TestFixedWindowAllowsUpToLimit(t *testing.T) {
	rl := NewFixedWindow(5, time.Second)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		if !rl.Allow(ctx, "user1") {
			t.Fatalf("request %d should have been allowed", i+1)
		}
	}
	if rl.Allow(ctx, "user1") {
		t.Fatal("6th request should have been rejected")
	}
}

func TestFixedWindowDifferentKeys(t *testing.T) {
	rl := NewFixedWindow(2, time.Second)
	ctx := context.Background()

	rl.Allow(ctx, "user1")
	rl.Allow(ctx, "user1")
	if rl.Allow(ctx, "user1") {
		t.Fatal("user1 should be rate limited")
	}
	if !rl.Allow(ctx, "user2") {
		t.Fatal("user2 should NOT be affected by user1's limit")
	}
}

func TestSlidingWindowSmoothing(t *testing.T) {
	rl := NewSlidingWindow(10, 100*time.Millisecond)
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		rl.Allow(ctx, "k")
	}
	if rl.Allow(ctx, "k") {
		t.Fatal("should be rate limited after 10 requests")
	}
}

func TestTokenBucketBurstAndRefill(t *testing.T) {
	rl := NewTokenBucket(3, 10.0)
	ctx := context.Background()

	// Burst: 3 tokens available immediately
	for i := 0; i < 3; i++ {
		if !rl.Allow(ctx, "k") {
			t.Fatalf("burst request %d should be allowed", i+1)
		}
	}
	if rl.Allow(ctx, "k") {
		t.Fatal("4th request should fail — bucket empty")
	}

	// Wait for refill (10 tokens/sec = 1 token per 100ms)
	time.Sleep(150 * time.Millisecond)
	if !rl.Allow(ctx, "k") {
		t.Fatal("should have refilled at least 1 token")
	}
}

func TestLeakyBucketConstantRate(t *testing.T) {
	rl := NewLeakyBucket(3, 10.0)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		if !rl.Allow(ctx, "k") {
			t.Fatalf("request %d should be allowed", i+1)
		}
	}
	if rl.Allow(ctx, "k") {
		t.Fatal("4th request should fail — bucket full")
	}

	time.Sleep(150 * time.Millisecond)
	if !rl.Allow(ctx, "k") {
		t.Fatal("should have leaked enough for 1 more request")
	}
}
