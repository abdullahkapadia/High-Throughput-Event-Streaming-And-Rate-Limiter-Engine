package event

import "time"

// Event represents a generic, immutable data record in the streaming engine.
// Once published, its fields should not be mutated.
type Event struct {
	ID        string
	Type      string
	Topic     string // Target topic for the event
	Key       string // Partition key
	Payload   []byte
	Timestamp time.Time
	Metadata  map[string]string
	
	// Broker-assigned fields. These are populated by the broker upon appending to a partition.
	Partition int
	Offset    uint64
}
