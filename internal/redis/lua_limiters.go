package redis

import (
	"context"
	"fmt"
	"time"
)

// Why Lua scripts?
// Without Lua, a rate limit check looks like:
//   1. GET counter
//   2. if counter < limit, INCR counter
//   3. if new key, EXPIRE key
// Between steps 1 and 2, another request can sneak in (TOCTOU race).
// A Lua script runs atomically inside Redis — no other command can interleave.

// FixedWindowLua implements fixed window rate limiting via a Redis Lua script.
type FixedWindowLua struct {
	Limit  int
	Window time.Duration
}

var fixedWindowScript = `
local key = KEYS[1]
local limit = tonumber(ARGV[1])
local window = tonumber(ARGV[2])

local current = redis.call("INCR", key)
if current == 1 then
    redis.call("EXPIRE", key, window)
end

if current > limit then
    return 0
end
return 1
`

func (fw *FixedWindowLua) Allow(ctx context.Context, client *Client, key string) (bool, error) {
	rkey := KeyFor("fw", key)
	windowSec := int(fw.Window.Seconds())

	result, err := client.Eval(ctx, fixedWindowScript, []string{rkey}, fw.Limit, windowSec)
	if err != nil {
		return false, fmt.Errorf("fixed window lua: %w", err)
	}
	return result.(int64) == 1, nil
}

// SlidingWindowLua implements sliding window rate limiting via Redis Lua.
// Uses two keys (current window + previous window) and weights the previous count.
type SlidingWindowLua struct {
	Limit  int
	Window time.Duration
}

var slidingWindowScript = `
local curr_key = KEYS[1]
local prev_key = KEYS[2]
local limit = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local now = tonumber(ARGV[3])

local curr_count = tonumber(redis.call("GET", curr_key) or "0")
local prev_count = tonumber(redis.call("GET", prev_key) or "0")

local curr_start = tonumber(redis.call("GET", curr_key .. ":start") or "0")
local elapsed = now - curr_start
if elapsed < 0 then elapsed = 0 end
local overlap = 1.0 - (elapsed / window)
if overlap < 0 then overlap = 0 end

local effective = prev_count * overlap + curr_count
if effective >= limit then
    return 0
end

redis.call("INCR", curr_key)
redis.call("EXPIRE", curr_key, window * 2)

if curr_start == 0 then
    redis.call("SET", curr_key .. ":start", now)
    redis.call("EXPIRE", curr_key .. ":start", window * 2)
end

return 1
`

func (sw *SlidingWindowLua) Allow(ctx context.Context, client *Client, key string) (bool, error) {
	now := time.Now().Unix()
	windowSec := int(sw.Window.Seconds())
	currKey := KeyFor("sw:curr", key)
	prevKey := KeyFor("sw:prev", key)

	result, err := client.Eval(ctx, slidingWindowScript, []string{currKey, prevKey}, sw.Limit, windowSec, now)
	if err != nil {
		return false, fmt.Errorf("sliding window lua: %w", err)
	}
	return result.(int64) == 1, nil
}

// TokenBucketLua implements token bucket rate limiting via Redis Lua.
type TokenBucketLua struct {
	Capacity   int
	RefillRate float64 // tokens per second
}

var tokenBucketScript = `
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])
local now = tonumber(ARGV[3])

local data = redis.call("HMGET", key, "tokens", "last_fill")
local tokens = tonumber(data[1])
local last_fill = tonumber(data[2])

if tokens == nil then
    tokens = capacity - 1
    redis.call("HMSET", key, "tokens", tokens, "last_fill", now)
    redis.call("EXPIRE", key, capacity)
    return 1
end

local elapsed = now - last_fill
local refilled = tokens + elapsed * refill_rate
if refilled > capacity then
    refilled = capacity
end

if refilled >= 1 then
    redis.call("HMSET", key, "tokens", refilled - 1, "last_fill", now)
    redis.call("EXPIRE", key, capacity)
    return 1
end

redis.call("HMSET", key, "tokens", refilled, "last_fill", now)
return 0
`

func (tb *TokenBucketLua) Allow(ctx context.Context, client *Client, key string) (bool, error) {
	rkey := KeyFor("tb", key)
	now := float64(time.Now().UnixMicro()) / 1_000_000.0

	result, err := client.Eval(ctx, tokenBucketScript, []string{rkey}, tb.Capacity, tb.RefillRate, now)
	if err != nil {
		return false, fmt.Errorf("token bucket lua: %w", err)
	}
	return result.(int64) == 1, nil
}

// LeakyBucketLua implements leaky bucket rate limiting via Redis Lua.
type LeakyBucketLua struct {
	Capacity int
	LeakRate float64 // leaks per second
}

var leakyBucketScript = `
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local leak_rate = tonumber(ARGV[2])
local now = tonumber(ARGV[3])

local data = redis.call("HMGET", key, "water", "last_leak")
local water = tonumber(data[1])
local last_leak = tonumber(data[2])

if water == nil then
    redis.call("HMSET", key, "water", 1, "last_leak", now)
    redis.call("EXPIRE", key, capacity)
    return 1
end

local elapsed = now - last_leak
local leaked = elapsed * leak_rate
water = water - leaked
if water < 0 then water = 0 end

if water < capacity then
    redis.call("HMSET", key, "water", water + 1, "last_leak", now)
    redis.call("EXPIRE", key, capacity)
    return 1
end

redis.call("HMSET", key, "water", water, "last_leak", now)
return 0
`

func (lb *LeakyBucketLua) Allow(ctx context.Context, client *Client, key string) (bool, error) {
	rkey := KeyFor("lb", key)
	now := float64(time.Now().UnixMicro()) / 1_000_000.0

	result, err := client.Eval(ctx, leakyBucketScript, []string{rkey}, lb.Capacity, lb.LeakRate, now)
	if err != nil {
		return false, fmt.Errorf("leaky bucket lua: %w", err)
	}
	return result.(int64) == 1, nil
}
