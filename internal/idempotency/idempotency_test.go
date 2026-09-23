package idempotency

import "testing"

func TestIdempotencyStore(t *testing.T) {
	s := NewStore()

	if s.Contains("evt-1") {
		t.Fatal("store should be empty initially")
	}

	s.Mark("evt-1")
	if !s.Contains("evt-1") {
		t.Fatal("store should contain evt-1 after marking")
	}

	// Marking again should be safe (no panic)
	s.Mark("evt-1")
	if !s.Contains("evt-1") {
		t.Fatal("evt-1 should still be present")
	}

	if s.Contains("evt-2") {
		t.Fatal("evt-2 should not be present")
	}
}
