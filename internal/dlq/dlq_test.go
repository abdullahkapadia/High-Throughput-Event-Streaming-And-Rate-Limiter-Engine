package dlq

import (
	"errors"
	"event-engine/internal/event"
	"testing"
)

func TestDLQPushAndList(t *testing.T) {
	q := NewQueue()

	e := &event.Event{ID: "e1", Topic: "orders", Partition: 2, Offset: 5}
	q.Push(e, errors.New("processing failed"), 3)

	if q.Len() != 1 {
		t.Fatalf("expected 1 DLQ entry, got %d", q.Len())
	}

	entries := q.List()
	entry := entries[0]

	if entry.Event.ID != "e1" {
		t.Errorf("expected event ID 'e1', got '%s'", entry.Event.ID)
	}
	if entry.OriginalTopic != "orders" {
		t.Errorf("expected original topic 'orders', got '%s'", entry.OriginalTopic)
	}
	if entry.RetryCount != 3 {
		t.Errorf("expected retry count 3, got %d", entry.RetryCount)
	}
	if entry.Error != "processing failed" {
		t.Errorf("expected error 'processing failed', got '%s'", entry.Error)
	}
}
