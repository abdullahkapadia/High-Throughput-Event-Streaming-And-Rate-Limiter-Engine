package retry

import (
	"context"
	"event-engine/internal/event"
	"math"
	"math/rand"
	"time"
)

// Handler processes an event. Returns non-nil error on failure.
type Handler func(e *event.Event) error

// DLQSink is called when an event exhausts all retries.
type DLQSink func(e *event.Event, lastErr error)

// Config controls retry behavior.
type Config struct {
	MaxRetries  int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
	JitterRatio float64 // 0.0 to 1.0 fraction of delay added as random jitter
}

func DefaultConfig() Config {
	return Config{
		MaxRetries:  3,
		BaseDelay:   100 * time.Millisecond,
		MaxDelay:    5 * time.Second,
		JitterRatio: 0.3,
	}
}

// Retrier wraps a handler with exponential backoff + jitter retry logic.
type Retrier struct {
	handler Handler
	dlq     DLQSink
	cfg     Config
}

func NewRetrier(handler Handler, dlq DLQSink, cfg Config) *Retrier {
	return &Retrier{
		handler: handler,
		dlq:     dlq,
		cfg:     cfg,
	}
}

// Process attempts to handle the event, retrying on failure with exponential backoff.
// If all retries are exhausted, the event is sent to the DLQ sink.
func (r *Retrier) Process(ctx context.Context, e *event.Event) error {
	var lastErr error

	for attempt := 0; attempt <= r.cfg.MaxRetries; attempt++ {
		lastErr = r.handler(e)
		if lastErr == nil {
			return nil
		}

		if attempt == r.cfg.MaxRetries {
			break
		}

		delay := r.backoff(attempt)
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	if r.dlq != nil {
		r.dlq(e, lastErr)
	}
	return lastErr
}

func (r *Retrier) backoff(attempt int) time.Duration {
	delay := float64(r.cfg.BaseDelay) * math.Pow(2, float64(attempt))
	if delay > float64(r.cfg.MaxDelay) {
		delay = float64(r.cfg.MaxDelay)
	}
	jitter := delay * r.cfg.JitterRatio * rand.Float64()
	return time.Duration(delay + jitter)
}
