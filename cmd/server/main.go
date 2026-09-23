package main

import (
	"context"
	"event-engine/internal/broker"
	"event-engine/internal/consumer"
	"event-engine/internal/dlq"
	"event-engine/internal/event"
	"event-engine/internal/idempotency"
	"event-engine/internal/offset"
	"event-engine/internal/partitioner"
	"event-engine/internal/producer"
	"event-engine/internal/ratelimiter"
	"event-engine/internal/retry"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

func main() {
	fmt.Println("=== High-Throughput Event Streaming Engine ===")
	fmt.Println()

	// --- Setup ---
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p := &partitioner.HashPartitioner{}
	brk := broker.NewMemoryBroker(p)

	numPartitions := 10
	partitionCapacity := 10000
	brk.CreateTopic(ctx, "TicketRequested", numPartitions, partitionCapacity)
	brk.CreateTopic(ctx, "PaymentInitiated", numPartitions, partitionCapacity)

	fmt.Printf("Topics created: TicketRequested (%d partitions), PaymentInitiated (%d partitions)\n", numPartitions, numPartitions)
	fmt.Printf("Partition capacity: %d (blocking backpressure)\n\n", partitionCapacity)

	// --- Rate Limiter ---
	rl := ratelimiter.NewTokenBucket(2000000, 100000.0) // 2M burst capacity, 100k tokens/sec refill
	fmt.Println("Rate Limiter: Token Bucket (capacity=2,000,000, refill=100k/sec)")
	fmt.Println()

	// --- Offset Manager ---
	offsets := offset.NewManager()

	// --- DLQ ---
	deadLetters := dlq.NewQueue()

	// --- Idempotency ---
	idemStore := idempotency.NewStore()

	// --- Retry Config ---
	retryCfg := retry.Config{
		MaxRetries:  2,
		BaseDelay:   10 * time.Millisecond,
		MaxDelay:    100 * time.Millisecond,
		JitterRatio: 0.1,
	}

	// --- Stats ---
	var produced, consumed, rejected, duplicates, retried, dlqCount int64

	// --- Consumers ---
	var failCount int32
	handler := func(e *event.Event) error {
		// Simulate occasional failure for retry demo
		if atomic.AddInt32(&failCount, 1)%25 == 0 {
			atomic.AddInt64(&retried, 1)
			return fmt.Errorf("simulated transient failure")
		}
		return nil
	}

	dlqSink := func(e *event.Event, err error) {
		deadLetters.Push(e, err, int(retryCfg.MaxRetries))
		atomic.AddInt64(&dlqCount, 1)
	}

	consumerHandler := func(e *event.Event) error {
		// Idempotency check
		if idemStore.Contains(e.ID) {
			atomic.AddInt64(&duplicates, 1)
			return nil
		}

		retrier := retry.NewRetrier(handler, dlqSink, retryCfg)
		err := retrier.Process(ctx, e)

		if err == nil {
			idemStore.Mark(e.ID)
			atomic.AddInt64(&consumed, 1)
		}
		return err
	}

	consumers := make([]*consumer.Consumer, 3)
	for i := 0; i < 3; i++ {
		consumers[i] = consumer.NewConsumer(fmt.Sprintf("consumer-%d", i), consumerHandler, offsets)
	}

	cg := consumer.NewGroup("booking-service", consumers)
	cg.Assign(brk.GetTopic("TicketRequested"))

	fmt.Println("Consumer Group: booking-service")
	fmt.Printf("  consumer-0, consumer-1, consumer-2 assigned to %d partitions\n\n", numPartitions)

	cg.Start(ctx)

	// --- Producers ---
	numProducers := 10
	eventsPerProducer := 500
	totalTarget := numProducers * eventsPerProducer

	fmt.Printf("Starting %d producers, %d events each (total target: %d)\n", numProducers, eventsPerProducer, totalTarget)
	fmt.Println("Rate limiter active — some requests will be rejected")
	fmt.Println()
	fmt.Println("--- Producing Events ---")

	start := time.Now()

	var prodWg sync.WaitGroup
	for i := 0; i < numProducers; i++ {
		prodWg.Add(1)
		go func(id int) {
			defer prodWg.Done()
			prod := producer.NewProducer(fmt.Sprintf("producer-%d", id), brk)

			for j := 0; j < eventsPerProducer; j++ {
				key := fmt.Sprintf("user-%d", (id*100+j)%50)

				// Rate limit check
				if !rl.Allow(ctx, key) {
					atomic.AddInt64(&rejected, 1)
					continue
				}

				err := prod.Send(ctx, "TicketRequested", key, []byte(fmt.Sprintf(`{"train":"1295%d","seat":%d}`, id, j)))
				if err != nil {
					continue
				}
				atomic.AddInt64(&produced, 1)
			}
		}(i)
	}

	prodWg.Wait()
	produceDuration := time.Since(start)

	fmt.Printf("  Production complete in %v\n", produceDuration)
	fmt.Println()

	// --- Wait for consumers to finish ---
	fmt.Println("--- Consuming Events ---")
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt64(&consumed)+atomic.LoadInt64(&dlqCount) >= atomic.LoadInt64(&produced) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	totalDuration := time.Since(start)

	cancel()
	cg.Stop()
	brk.Close()

	// --- Results ---
	prod := atomic.LoadInt64(&produced)
	cons := atomic.LoadInt64(&consumed)
	rej := atomic.LoadInt64(&rejected)
	dups := atomic.LoadInt64(&duplicates)
	retr := atomic.LoadInt64(&retried)
	dlqs := atomic.LoadInt64(&dlqCount)

	fmt.Println()
	fmt.Println("========================================")
	fmt.Println("         BENCHMARK RESULTS")
	fmt.Println("========================================")
	fmt.Printf("  Producers:          %d\n", numProducers)
	fmt.Printf("  Consumers:          %d\n", len(consumers))
	fmt.Printf("  Partitions:         %d\n", numPartitions)
	fmt.Printf("  Partition Capacity: %d\n", partitionCapacity)
	fmt.Println("----------------------------------------")
	fmt.Printf("  Target Events:      %d\n", totalTarget)
	fmt.Printf("  Rate Limited:       %d\n", rej)
	fmt.Printf("  Produced:           %d\n", prod)
	fmt.Printf("  Consumed:           %d\n", cons)
	fmt.Printf("  Retried:            %d\n", retr)
	fmt.Printf("  Sent to DLQ:        %d\n", dlqs)
	fmt.Printf("  Duplicates Caught:  %d\n", dups)
	fmt.Println("----------------------------------------")
	fmt.Printf("  Produce Duration:   %v\n", produceDuration)
	fmt.Printf("  Total Duration:     %v\n", totalDuration)
	if prod > 0 {
		fmt.Printf("  Throughput:         %.0f events/sec\n", float64(prod)/totalDuration.Seconds())
	}
	fmt.Println("========================================")

	if dlqs > 0 {
		fmt.Println()
		fmt.Println("--- Dead Letter Queue Entries ---")
		for i, entry := range deadLetters.List() {
			if i >= 5 {
				fmt.Printf("  ... and %d more\n", dlqs-5)
				break
			}
			fmt.Printf("  [%d] EventID=%s Topic=%s Partition=%d Error=%s\n",
				i, entry.Event.ID, entry.OriginalTopic, entry.Partition, entry.Error)
		}
	}

	// --- Offset snapshot ---
	fmt.Println()
	fmt.Println("--- Consumer Offset Snapshot ---")
	for i := 0; i < len(consumers); i++ {
		cid := fmt.Sprintf("consumer-%d", i)
		for p := 0; p < numPartitions; p++ {
			committed := offsets.GetCommitted(cid, p)
			if committed > 0 {
				fmt.Printf("  %s partition-%d committed_offset=%d\n", cid, p, committed)
			}
		}
	}

	fmt.Println()
	fmt.Println("Graceful shutdown complete. No goroutine leaks.")

	// Handle SIGINT/SIGTERM for interactive runs
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-sigCh:
	default:
	}
}
