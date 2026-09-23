package broker

import (
	"context"
	"event-engine/internal/event"
	"testing"
)

func TestPartitionAppendAndConsume(t *testing.T) {
	p := NewPartition(1, 10)
	defer p.Close()

	ctx := context.Background()

	e1 := &event.Event{ID: "e1", Payload: []byte("first")}
	err := p.Append(ctx, e1)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if e1.Partition != 1 {
		t.Errorf("expected partition 1, got %d", e1.Partition)
	}
	if e1.Offset != 0 {
		t.Errorf("expected offset 0, got %d", e1.Offset)
	}

	e2 := &event.Event{ID: "e2", Payload: []byte("second")}
	p.Append(ctx, e2)

	ch := p.Consume()
	r1 := <-ch
	if r1.ID != "e1" {
		t.Errorf("expected e1, got %s", r1.ID)
	}
	r2 := <-ch
	if r2.ID != "e2" {
		t.Errorf("expected e2, got %s", r2.ID)
	}
}

func TestPartitionReadAt(t *testing.T) {
	p := NewPartition(0, 10)
	defer p.Close()

	ctx := context.Background()
	p.Append(ctx, &event.Event{ID: "a"})
	p.Append(ctx, &event.Event{ID: "b"})
	p.Append(ctx, &event.Event{ID: "c"})

	e := p.ReadAt(1)
	if e == nil || e.ID != "b" {
		t.Errorf("expected event 'b' at offset 1")
	}

	if p.ReadAt(99) != nil {
		t.Error("expected nil for out of bounds offset")
	}
}

func TestPartitionRejectBackpressure(t *testing.T) {
	p := NewPartition(0, 1)
	defer p.Close()

	ctx := context.Background()
	p.SetBackpressure(Reject)

	p.Append(ctx, &event.Event{ID: "a"})

	err := p.Append(ctx, &event.Event{ID: "b"})
	if err != ErrPartitionFull {
		t.Errorf("expected ErrPartitionFull, got %v", err)
	}
}

func TestPartitionCASClose(t *testing.T) {
	p := NewPartition(0, 5)
	p.Close()
	p.Close() // double close should not panic
}
