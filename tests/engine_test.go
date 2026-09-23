package tests

import (
	"context"
	"event-engine/internal/broker"
	"event-engine/internal/consumer"
	"event-engine/internal/event"
	"event-engine/internal/offset"
	"event-engine/internal/partitioner"
	"event-engine/internal/producer"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestEngineFullFlow(t *testing.T) {
	p := &partitioner.HashPartitioner{}
	b := broker.NewMemoryBroker(p)
	ctx := context.Background()
	b.CreateTopic(ctx, "orders", 3, 10)

	offsets := offset.NewManager()
	var consumedCount int32
	handler := func(e *event.Event) error {
		atomic.AddInt32(&consumedCount, 1)
		return nil
	}

	c1 := consumer.NewConsumer("c1", handler, offsets)
	c2 := consumer.NewConsumer("c2", handler, offsets)
	cg := consumer.NewGroup("cg-orders", []*consumer.Consumer{c1, c2})

	topic := b.GetTopic("orders")
	cg.Assign(topic)

	ctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	cg.Start(ctx, &wg)

	prod1 := producer.NewProducer("p1", b)
	prod2 := producer.NewProducer("p2", b)

	var prodWg sync.WaitGroup
	produceEvents := func(prod *producer.Producer, count int) {
		defer prodWg.Done()
		for i := 0; i < count; i++ {
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

	totalEvents := 100
	prodWg.Add(2)
	go produceEvents(prod1, totalEvents/2)
	go produceEvents(prod2, totalEvents/2)
	prodWg.Wait()

	for i := 0; i < 50; i++ {
		if atomic.LoadInt32(&consumedCount) == int32(totalEvents) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if atomic.LoadInt32(&consumedCount) != int32(totalEvents) {
		t.Errorf("expected %d events consumed, got %d", totalEvents, consumedCount)
	}

	cancel()
	wg.Wait()
	b.Close()
}

func TestSameKeyGoesToSamePartition(t *testing.T) {
	p := &partitioner.HashPartitioner{}
	key := "user-999"

	results := make(map[int]bool)
	for i := 0; i < 100; i++ {
		partition := p.Partition(key, 5)
		results[partition] = true
	}

	if len(results) != 1 {
		t.Errorf("expected same key to always go to 1 partition, went to %d", len(results))
	}
}

func TestBackpressureReject(t *testing.T) {
	p := &partitioner.HashPartitioner{}
	b := broker.NewMemoryBroker(p)
	ctx := context.Background()
	b.CreateTopic(ctx, "tiny", 1, 2) // capacity of 2

	topic := b.GetTopic("tiny")
	topic.GetPartition(0).SetBackpressure(broker.Reject)

	prod := producer.NewProducer("p1", b)
	prod.Send(ctx, "tiny", "k", nil)
	prod.Send(ctx, "tiny", "k", nil)

	err := prod.Send(ctx, "tiny", "k", nil)
	if err != broker.ErrPartitionFull {
		t.Errorf("expected ErrPartitionFull, got %v", err)
	}
}

func TestGracefulShutdownNoLeak(t *testing.T) {
	p := &partitioner.HashPartitioner{}
	b := broker.NewMemoryBroker(p)
	ctx := context.Background()
	b.CreateTopic(ctx, "shutdown-test", 2, 100)

	offsets := offset.NewManager()
	handler := func(e *event.Event) error { return nil }
	c1 := consumer.NewConsumer("c1", handler, offsets)
	cg := consumer.NewGroup("cg", []*consumer.Consumer{c1})
	cg.Assign(b.GetTopic("shutdown-test"))

	ctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	cg.Start(ctx, &wg)

	cancel()
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// OK — consumers shut down cleanly
	case <-time.After(2 * time.Second):
		t.Fatal("graceful shutdown timed out — possible goroutine leak or deadlock")
	}
	b.Close()
}

func TestConsumerGroupRebalance(t *testing.T) {
	p := &partitioner.HashPartitioner{}
	b := broker.NewMemoryBroker(p)
	ctx := context.Background()
	b.CreateTopic(ctx, "rebalance", 4, 100)

	offsets := offset.NewManager()
	handler := func(e *event.Event) error { return nil }
	c1 := consumer.NewConsumer("c1", handler, offsets)
	c2 := consumer.NewConsumer("c2", handler, offsets)
	c3 := consumer.NewConsumer("c3", handler, offsets)

	cg := consumer.NewGroup("cg", []*consumer.Consumer{c1, c2})
	topic := b.GetTopic("rebalance")
	cg.Assign(topic)

	// After assigning 4 partitions to 2 consumers, add a 3rd and rebalance
	cg.AddConsumer(c3)
	cg.Rebalance(topic)

	// Now remove c2 and rebalance again
	cg.RemoveConsumer("c2")
	cg.Rebalance(topic)

	b.Close()
}
