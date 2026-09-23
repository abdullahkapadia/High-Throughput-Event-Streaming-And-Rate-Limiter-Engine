package redis

import (
	"context"
	"os"
	"testing"
	"time"
)

// These tests require a running Redis instance.
// Set REDIS_ADDR env var (default: localhost:6379).
// Skip if Redis is not available.

func getTestClient(t *testing.T) *Client {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	client := NewClient(addr, "", 0)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Ping(ctx); err != nil {
		t.Skipf("Redis not available at %s: %v", addr, err)
	}
	return client
}

func TestRedisFixedWindowLua(t *testing.T) {
	client := getTestClient(t)
	defer client.Close()

	ctx := context.Background()
	fw := &FixedWindowLua{Limit: 3, Window: 10 * time.Second}

	key := "test:fw:" + time.Now().Format("150405.000")

	for i := 0; i < 3; i++ {
		allowed, err := fw.Allow(ctx, client, key)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !allowed {
			t.Fatalf("request %d should be allowed", i+1)
		}
	}

	allowed, err := fw.Allow(ctx, client, key)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if allowed {
		t.Fatal("4th request should be rejected")
	}
}

func TestRedisTokenBucketLua(t *testing.T) {
	client := getTestClient(t)
	defer client.Close()

	ctx := context.Background()
	tb := &TokenBucketLua{Capacity: 2, RefillRate: 10.0}

	key := "test:tb:" + time.Now().Format("150405.000")

	// Burst 2 tokens
	for i := 0; i < 2; i++ {
		allowed, err := tb.Allow(ctx, client, key)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !allowed {
			t.Fatalf("burst request %d should be allowed", i+1)
		}
	}

	allowed, err := tb.Allow(ctx, client, key)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if allowed {
		t.Fatal("3rd request should be rejected — bucket empty")
	}

	// Wait for refill
	time.Sleep(200 * time.Millisecond)
	allowed, err = tb.Allow(ctx, client, key)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !allowed {
		t.Fatal("should have refilled after waiting")
	}
}

func TestRedisLeakyBucketLua(t *testing.T) {
	client := getTestClient(t)
	defer client.Close()

	ctx := context.Background()
	lb := &LeakyBucketLua{Capacity: 2, LeakRate: 10.0}

	key := "test:lb:" + time.Now().Format("150405.000")

	for i := 0; i < 2; i++ {
		allowed, err := lb.Allow(ctx, client, key)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !allowed {
			t.Fatalf("request %d should be allowed", i+1)
		}
	}

	allowed, err := lb.Allow(ctx, client, key)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if allowed {
		t.Fatal("3rd request should be rejected — bucket full")
	}
}

func TestFailOpenPolicy(t *testing.T) {
	// Connect to a fake Redis address so it fails
	client := NewClient("localhost:19999", "", 0)
	defer client.Close()

	fw := &FixedWindowLua{Limit: 5, Window: time.Second}
	rl := NewRateLimiter(client, FailOpen, fw)

	// With fail-open, requests should be allowed even though Redis is unreachable
	if !rl.Allow(context.Background(), "any-key") {
		t.Fatal("fail-open should allow requests when Redis is unavailable")
	}
}

func TestFailClosedPolicy(t *testing.T) {
	client := NewClient("localhost:19999", "", 0)
	defer client.Close()

	fw := &FixedWindowLua{Limit: 5, Window: time.Second}
	rl := NewRateLimiter(client, FailClosed, fw)

	if rl.Allow(context.Background(), "any-key") {
		t.Fatal("fail-closed should reject requests when Redis is unavailable")
	}
}
