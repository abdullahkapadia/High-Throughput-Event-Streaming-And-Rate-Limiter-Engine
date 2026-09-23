package offset

import "sync"

// Manager tracks per-consumer, per-partition offsets.
// It distinguishes between the "current" offset (what the consumer is about to read next)
// and the "committed" offset (what the consumer has acknowledged as durably processed).
//
// processed != committed.
// A consumer may process offset 5 but crash before committing.
// On restart, it replays from the last committed offset, which may be 3.
// This is the foundation of at-least-once delivery.
type Manager struct {
	mu        sync.RWMutex
	current   map[string]map[int]uint64 // consumerID -> partitionID -> offset
	committed map[string]map[int]uint64
}

func NewManager() *Manager {
	return &Manager{
		current:   make(map[string]map[int]uint64),
		committed: make(map[string]map[int]uint64),
	}
}

func (m *Manager) initConsumer(store map[string]map[int]uint64, consumerID string) {
	if _, ok := store[consumerID]; !ok {
		store[consumerID] = make(map[int]uint64)
	}
}

// Advance moves the current read offset forward for a consumer on a partition.
func (m *Manager) Advance(consumerID string, partitionID int) uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.initConsumer(m.current, consumerID)
	off := m.current[consumerID][partitionID]
	m.current[consumerID][partitionID] = off + 1
	return off
}

// Commit records that a consumer has durably processed up to the given offset on a partition.
func (m *Manager) Commit(consumerID string, partitionID int, offset uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.initConsumer(m.committed, consumerID)
	m.committed[consumerID][partitionID] = offset
}

// GetCommitted returns the last committed offset for a consumer on a partition.
func (m *Manager) GetCommitted(consumerID string, partitionID int) uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if offsets, ok := m.committed[consumerID]; ok {
		return offsets[partitionID]
	}
	return 0
}

// GetCurrent returns the current read offset for a consumer on a partition.
func (m *Manager) GetCurrent(consumerID string, partitionID int) uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if offsets, ok := m.current[consumerID]; ok {
		return offsets[partitionID]
	}
	return 0
}

// ResetToCurrent sets the current offset back to the committed offset (simulating a restart/replay).
func (m *Manager) ResetToCommitted(consumerID string, partitionID int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.initConsumer(m.current, consumerID)
	m.initConsumer(m.committed, consumerID)
	m.current[consumerID][partitionID] = m.committed[consumerID][partitionID]
}
