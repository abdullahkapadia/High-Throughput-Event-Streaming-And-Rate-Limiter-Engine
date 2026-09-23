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

func BenchmarkEngine(b *testing.B) {
	p := &partitioner.HashPartitioner{}
	brk := broker.NewMemoryBroker(p)
	ctx := context.Background()
	brk.CreateTopic(ctx, "bench-topic", 5, 10000)

	offsets := offset.NewManager()
	var consumed int64
	handler := func(e *event.Event) error {
		atomic.AddInt64(&consumed, 1)
		return nil
	}

	consumers := make([]*consumer.Consumer, 3)
	for i := 0; i < 3; i++ {
		consumers[i] = consumer.NewConsumer(fmt.Sprintf("c%d", i), handler, offsets)
	}
	cg := consumer.NewGroup("cg1", consumers)
	cg.Assign(brk.GetTopic("bench-topic"))

	ctx, cancel := context.WithCancel(ctx)
	cg.Start(ctx)

	producers := make([]*producer.Producer, 5)
	for i := 0; i < 5; i++ {
		producers[i] = producer.NewProducer(fmt.Sprintf("p%d", i), brk)
	}

	b.ResetTimer()

	var prodWg sync.WaitGroup
	eventsPerProducer := b.N / 5

	for i := 0; i < 5; i++ {
		prodWg.Add(1)
		go func(prod *producer.Producer, count int) {
			defer prodWg.Done()
			for j := 0; j < count; j++ {
				key := fmt.Sprintf("user-%d", j%100)
				prod.Send(ctx, "bench-topic", key, nil)
			}
		}(producers[i], eventsPerProducer)
	}

	prodWg.Wait()

	for atomic.LoadInt64(&consumed) < int64(eventsPerProducer*5) {
		time.Sleep(1 * time.Millisecond)
	}

	b.StopTimer()
	cancel()
	cg.Stop()
}
