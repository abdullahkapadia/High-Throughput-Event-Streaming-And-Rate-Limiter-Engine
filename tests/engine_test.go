package tests

import (
	"context"
	"event-engine/internal/broker"
	"event-engine/internal/consumer"
	"event-engine/internal/event"
	"event-engine/internal/partitioner"
	"event-engine/internal/producer"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestEngineFullFlow(t *testing.T) {
	// 1. Setup Broker & Partitioner
	p := &partitioner.HashPartitioner{}
	b := broker.NewBroker(p)
	
	// Topic with 3 partitions and a small capacity to trigger backpressure tests
	capacity := 10
	b.CreateTopic("orders", 3, capacity)

	// 2. Setup Consumer Group
	var consumedCount int32
	handler := func(e *event.Event) {
		atomic.AddInt32(&consumedCount, 1)
	}

	c1 := consumer.NewConsumer("c1", handler)
	c2 := consumer.NewConsumer("c2", handler)
	cg := consumer.NewGroup("cg-orders", []*consumer.Consumer{c1, c2})
	
	topic := b.GetTopic("orders")
	cg.Assign(topic)

	// 3. Start Consumers with Graceful Shutdown Context
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	cg.Start(ctx, &wg)

	// 4. Start Producers concurrently
	prod1 := producer.NewProducer("p1", b)
	prod2 := producer.NewProducer("p2", b)

	var prodWg sync.WaitGroup
	produceEvents := func(prod *producer.Producer, count int) {
		defer prodWg.Done()
		for i := 0; i < count; i++ {
			// Using identical keys to test deterministic partitioning
			key := "user-123"
			if i%2 == 0 {
				key = "user-456" 
			}
			err := prod.Send(ctx, "orders", key, []byte("data"))
			if err != nil && err != context.Canceled {
				t.Errorf("failed to send: %v", err)
			}
		}
	}

	totalEvents := 100 // Exceeds capacity, testing Backpressure
	prodWg.Add(2)
	go produceEvents(prod1, totalEvents/2)
	go produceEvents(prod2, totalEvents/2)

	// Wait for producers
	prodWg.Wait()

	// Wait for consumers to catch up
	// Small sleep loop to allow concurrent handlers to process
	for i := 0; i < 50; i++ {
		if atomic.LoadInt32(&consumedCount) == int32(totalEvents) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if atomic.LoadInt32(&consumedCount) != int32(totalEvents) {
		t.Errorf("expected %d events consumed, got %d", totalEvents, consumedCount)
	}

	// 5. Test Graceful Shutdown
	cancel() // Cancel the context to stop consumers
	wg.Wait() // Wait for all consumer goroutines to cleanly exit
	b.Close() // Close broker channels
}
