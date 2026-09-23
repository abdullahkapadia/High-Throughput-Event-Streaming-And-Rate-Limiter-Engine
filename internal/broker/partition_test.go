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

	// Append first event
	e1 := &event.Event{ID: "e1", Payload: []byte("first")}
	err1 := p.Append(ctx, e1)

	if err1 != nil {
		t.Errorf("expected no error, got %v", err1)
	}
	if e1.Partition != 1 {
		t.Errorf("expected event partition to be set to 1, got %d", e1.Partition)
	}
	if e1.Offset != 0 {
		t.Errorf("expected event offset to be set to 0, got %d", e1.Offset)
	}

	// Append second event
	e2 := &event.Event{ID: "e2", Payload: []byte("second")}
	err2 := p.Append(ctx, e2)

	if err2 != nil {
		t.Errorf("expected no error, got %v", err2)
	}

	// Consume back
	ch := p.Consume()
	readE1 := <-ch
	if readE1 == nil || readE1.ID != "e1" {
		t.Errorf("expected to read e1 at offset 0")
	}

	readE2 := <-ch
	if readE2 == nil || readE2.ID != "e2" {
		t.Errorf("expected to read e2 at offset 1")
	}
}
