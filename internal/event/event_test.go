package event

import (
	"testing"
	"time"
)

func TestEventCreation(t *testing.T) {
	now := time.Now()
	
	e := Event{
		ID:        "evt-123",
		Type:      "TicketRequested",
		Topic:     "orders",
		Key:       "user-456",
		Timestamp: now,
		Payload:   []byte(`{"order_id": 999}`),
		Metadata:  map[string]string{"source": "mobile"},
		Partition: 0,
		Offset:    10,
	}

	if e.ID != "evt-123" {
		t.Errorf("expected ID 'evt-123', got '%s'", e.ID)
	}
	if e.Type != "TicketRequested" {
		t.Errorf("expected Type 'TicketRequested', got '%s'", e.Type)
	}
	if e.Topic != "orders" {
		t.Errorf("expected Topic 'orders', got '%s'", e.Topic)
	}
	if e.Key != "user-456" {
		t.Errorf("expected Key 'user-456', got '%s'", e.Key)
	}
	if e.Timestamp != now {
		t.Errorf("expected Timestamp %v, got %v", now, e.Timestamp)
	}
	if string(e.Payload) != `{"order_id": 999}` {
		t.Errorf("expected Payload to match, got '%s'", string(e.Payload))
	}
	if e.Metadata["source"] != "mobile" {
		t.Errorf("expected Metadata source 'mobile'")
	}
}
