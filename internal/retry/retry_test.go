package retry

import (
	"context"
	"errors"
	"event-engine/internal/event"
	"sync/atomic"
	"testing"
)

func TestRetrySuccess(t *testing.T) {
	var attempts int32
	handler := func(e *event.Event) error {
		if atomic.AddInt32(&attempts, 1) < 3 {
			return errors.New("transient failure")
		}
		return nil
	}

	cfg := DefaultConfig()
	cfg.BaseDelay = 0 // speed up test
	r := NewRetrier(handler, nil, cfg)

	err := r.Process(context.Background(), &event.Event{ID: "e1"})
	if err != nil {
		t.Fatalf("expected success after retries, got %v", err)
	}
	if atomic.LoadInt32(&attempts) != 3 {
		t.Errorf("expected 3 attempts, got %d", attempts)
	}
}

func TestRetryExhaustedGoesToDLQ(t *testing.T) {
	handler := func(e *event.Event) error {
		return errors.New("permanent failure")
	}

	var dlqCalled bool
	dlqSink := func(e *event.Event, err error) {
		dlqCalled = true
	}

	cfg := DefaultConfig()
	cfg.MaxRetries = 2
	cfg.BaseDelay = 0
	r := NewRetrier(handler, dlqSink, cfg)

	err := r.Process(context.Background(), &event.Event{ID: "e2"})
	if err == nil {
		t.Fatal("expected error after exhausting retries")
	}
	if !dlqCalled {
		t.Fatal("expected DLQ sink to be called")
	}
}
