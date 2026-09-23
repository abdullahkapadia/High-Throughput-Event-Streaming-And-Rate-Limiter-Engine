package idempotency

import "sync"

// Store tracks which event IDs have been successfully processed.
// When at-least-once delivery causes a duplicate, the consumer checks
// this store before re-processing, preventing duplicate side effects.
type Store struct {
	mu   sync.RWMutex
	seen map[string]struct{}
}

func NewStore() *Store {
	return &Store{
		seen: make(map[string]struct{}),
	}
}

// Contains checks if an event ID has already been processed.
func (s *Store) Contains(eventID string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.seen[eventID]
	return ok
}

// Mark records an event ID as processed.
func (s *Store) Mark(eventID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seen[eventID] = struct{}{}
}
