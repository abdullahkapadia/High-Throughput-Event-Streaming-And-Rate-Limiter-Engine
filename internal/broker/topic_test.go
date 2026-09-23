package broker

import "testing"

func TestTopicCreation(t *testing.T) {
	topic := NewTopic("orders", 3, 100)

	if topic.Name != "orders" {
		t.Errorf("expected topic name 'orders', got %s", topic.Name)
	}

	if len(topic.Partitions) != 3 {
		t.Fatalf("expected 3 partitions, got %d", len(topic.Partitions))
	}

	// Verify partitions are initialized properly with correct IDs
	for i := 0; i < 3; i++ {
		p := topic.GetPartition(i)
		if p == nil {
			t.Fatalf("expected partition %d to not be nil", i)
		}
		if p.ID != i {
			t.Errorf("expected partition ID %d, got %d", i, p.ID)
		}
	}

	// Test out of bounds partition
	p99 := topic.GetPartition(99)
	if p99 != nil {
		t.Errorf("expected nil for non-existent partition")
	}
}
