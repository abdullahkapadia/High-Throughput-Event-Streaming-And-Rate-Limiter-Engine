package tests

import (
	"context"
	"event-engine/internal/broker"
	"event-engine/internal/consumer"
	"event-engine/internal/event"
	"event-engine/internal/offset"
	"event-engine/internal/partitioner"
	"event-engine/internal/producer"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestConsumerCrashAndRecovery(t *testing.T) {
	// 1. Setup Broker, Topic (6 partitions)
	p := &partitioner.HashPartitioner{}
	b := broker.NewMemoryBroker(p)
	ctx := context.Background()
	
	// Create topic with 6 partitions
	b.CreateTopic(ctx, "recovery-test", 6, 10000)
	topic := b.GetTopic("recovery-test")

	// 2. Setup Offset Manager and 3 Consumers
	offsets := offset.NewManager()
	
	var consumedTotal int32
	// We'll track how many events each consumer instance processes
	consumedByC1FirstRun := int32(0)
	consumedByC1SecondRun := int32(0)

	// Create 3 handler functions to track consumption
	createHandler := func(counter *int32) consumer.Handler {
		return func(e *event.Event) error {
			atomic.AddInt32(&consumedTotal, 1)
			if counter != nil {
				atomic.AddInt32(counter, 1)
			}
			// Simulate some processing time
			time.Sleep(1 * time.Millisecond)
			return nil
		}
	}

	c0 := consumer.NewConsumer("c0", createHandler(nil), offsets)
	c1 := consumer.NewConsumer("c1", createHandler(&consumedByC1FirstRun), offsets)
	c2 := consumer.NewConsumer("c2", createHandler(nil), offsets)

	cg := consumer.NewGroup("cg-recovery", []*consumer.Consumer{c0, c1, c2})
	
	// Assign partitions and start
	cg.Assign(topic)
	runCtx, cancelRun := context.WithCancel(ctx)
	cg.Start(runCtx)

	// 3. Start producing 1000 events
	prod := producer.NewProducer("p1", b)
	var prodWg sync.WaitGroup
	prodWg.Add(1)
	
	go func() {
		defer prodWg.Done()
		for i := 0; i < 1000; i++ {
			key := fmt.Sprintf("user-%d", i)
			prod.Send(ctx, "recovery-test", key, []byte(fmt.Sprintf("data-%d", i)))
			// Add a tiny delay so consumption happens concurrently with production
			time.Sleep(2 * time.Millisecond)
		}
	}()

	// Wait until some events are consumed
	for {
		if atomic.LoadInt32(&consumedTotal) > 200 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// 4. "Kill" Consumer 1
	// Remove from group and rebalance
	t.Logf("Killing c1... Events consumed so far: %d", atomic.LoadInt32(&consumedTotal))
	cg.RemoveConsumer("c1")
	cg.Rebalance(topic, runCtx)
	t.Log("c1 killed and partitions reassigned.")

	// Wait a bit to let other consumers process events that would have gone to c1
	time.Sleep(500 * time.Millisecond)

	// 5. Restart Consumer 1 (simulate pod restart)
	// Create a new instance of c1 but with the SAME ID so it uses the same offsets
	c1Restarted := consumer.NewConsumer("c1", createHandler(&consumedByC1SecondRun), offsets)
	cg.AddConsumer(c1Restarted)
	cg.Rebalance(topic, runCtx)
	t.Log("c1 restarted and partitions reassigned back.")

	// Wait for producer to finish
	prodWg.Wait()

	// Wait for all 1000 events to be consumed
	// Note: Because of at-least-once delivery, if a consumer crashes before committing,
	// some events might be re-processed. We expect AT LEAST 1000 events processed.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&consumedTotal) >= 1000 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	cancelRun()
	cg.Stop()
	b.Close()

	// 6 & 7. Verify
	total := atomic.LoadInt32(&consumedTotal)
	if total < 1000 {
		t.Errorf("Expected at least 1000 events to be consumed, got %d", total)
	}

	c1First := atomic.LoadInt32(&consumedByC1FirstRun)
	c1Second := atomic.LoadInt32(&consumedByC1SecondRun)
	
	t.Logf("Total Consumed: %d", total)
	t.Logf("c1 first run consumed: %d", c1First)
	t.Logf("c1 second run consumed: %d", c1Second)

	if c1First == 0 {
		t.Error("Expected c1 to consume events before crash")
	}
	if c1Second == 0 {
		t.Error("Expected c1 to consume events after restart")
	}
}
