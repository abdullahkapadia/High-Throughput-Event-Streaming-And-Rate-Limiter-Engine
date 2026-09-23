package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Client wraps the go-redis client with health tracking.
type Client struct {
	rdb *redis.Client
}

// NewClient creates a new Redis client.
func NewClient(addr, password string, db int) *Client {
	rdb := redis.NewClient(&redis.Options{
		Addr:         addr,
		Password:     password,
		DB:           db,
		DialTimeout:  2 * time.Second,
		ReadTimeout:  1 * time.Second,
		WriteTimeout: 1 * time.Second,
	})
	return &Client{rdb: rdb}
}

// Ping checks Redis connectivity.
func (c *Client) Ping(ctx context.Context) error {
	return c.rdb.Ping(ctx).Err()
}

// Eval runs a Lua script atomically on Redis.
func (c *Client) Eval(ctx context.Context, script string, keys []string, args ...interface{}) (interface{}, error) {
	return c.rdb.Eval(ctx, script, keys, args...).Result()
}

// Close shuts down the Redis connection.
func (c *Client) Close() error {
	return c.rdb.Close()
}

// KeyFor builds a namespaced Redis key.
func KeyFor(prefix, key string) string {
	return fmt.Sprintf("ratelimit:%s:%s", prefix, key)
}
