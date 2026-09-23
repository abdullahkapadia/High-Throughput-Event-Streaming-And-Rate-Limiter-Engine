package offset

import "testing"

func TestOffsetAdvanceAndCommit(t *testing.T) {
	m := NewManager()

	// Advance moves the current read pointer forward
	off := m.Advance("c1", 0)
	if off != 0 {
		t.Errorf("expected first advance to return 0, got %d", off)
	}
	off = m.Advance("c1", 0)
	if off != 1 {
		t.Errorf("expected second advance to return 1, got %d", off)
	}

	// Committed should still be at 0 (we haven't committed anything)
	if m.GetCommitted("c1", 0) != 0 {
		t.Error("committed offset should be 0 before any commit")
	}

	// Commit offset 1
	m.Commit("c1", 0, 1)
	if m.GetCommitted("c1", 0) != 1 {
		t.Errorf("expected committed offset 1, got %d", m.GetCommitted("c1", 0))
	}

	// Current should still be 2 (we advanced twice)
	if m.GetCurrent("c1", 0) != 2 {
		t.Errorf("expected current offset 2, got %d", m.GetCurrent("c1", 0))
	}
}

func TestOffsetResetToCommitted(t *testing.T) {
	m := NewManager()

	// Simulate processing offsets 0, 1, 2 but only committing 1
	m.Advance("c1", 0)
	m.Advance("c1", 0)
	m.Advance("c1", 0)
	m.Commit("c1", 0, 1)

	// Simulate a restart — reset current back to committed
	m.ResetToCommitted("c1", 0)

	if m.GetCurrent("c1", 0) != 1 {
		t.Errorf("expected current to reset to committed (1), got %d", m.GetCurrent("c1", 0))
	}
}
