// Command generator writes synthetic posts to Kafka at a fixed rate.
//
// On exit (Ctrl+C or -duration) it can write the exact per-tag counts of every post Kafka
// acknowledged (-truth-out), which is what the Flink job's totals must converge to.
package main

import (
	"cmp"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"slices"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"hashtags/generator/internal/gen"
)

func main() {
	brokers := flag.String("brokers", "localhost:29092", "Kafka bootstrap servers")
	topic := flag.String("topic", "posts", "Kafka topic")
	rate := flag.Int("rate", 1000, "posts per second")
	duration := flag.Duration("duration", 0, "stop after this long (0 = until Ctrl+C)")
	tags := flag.Int("tags", 200, "hashtag vocabulary size")
	zipf := flag.Float64("zipf", 1.2, "skew exponent (> 1); higher = fewer tags dominate")
	users := flag.Int("users", 10000, "number of distinct users (Kafka message keys)")
	seed := flag.Uint64("seed", uint64(time.Now().UnixNano()), "random seed")
	truthOut := flag.String("truth-out", "", "write acknowledged per-tag counts to this JSON file on exit")
	flag.Parse()

	g, err := gen.New(gen.Config{Tags: *tags, Zipf: *zipf, Users: *users, Seed: *seed})
	if err != nil {
		log.Fatal(err)
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(*brokers),
		kgo.DefaultProduceTopic(*topic),
		kgo.ProducerLinger(5*time.Millisecond), // small batches: throughput without much latency
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *duration)
		defer cancel()
	}
	if err := client.Ping(ctx); err != nil {
		log.Fatalf("cannot reach Kafka at %s: %v", *brokers, err)
	}

	var (
		sent, acked, failed atomic.Int64
		mu                  sync.Mutex
		truth               = map[string]int64{}
	)

	log.Printf("producing %d posts/s to %s on %s (tags=%d zipf=%.2f users=%d)",
		*rate, *topic, *brokers, *tags, *zipf, *users)

	// Every 10ms emit the number of posts due so far, so the long-run rate is exact.
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	report := time.NewTicker(5 * time.Second)
	defer report.Stop()
	start, lastAcked := time.Now(), int64(0)

loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case <-report.C:
			a := acked.Load()
			log.Printf("sent=%d acked=%d failed=%d rate=%.0f/s", sent.Load(), a, failed.Load(), float64(a-lastAcked)/5)
			lastAcked = a
		case <-ticker.C:
			due := int64(time.Since(start).Seconds() * float64(*rate))
			for sent.Load() < due {
				post, postTags := g.Next()
				value, _ := json.Marshal(post)
				sent.Add(1)
				client.Produce(ctx, &kgo.Record{Key: []byte(post.UserID), Value: value},
					func(_ *kgo.Record, err error) {
						if err != nil {
							failed.Add(1)
							return
						}
						acked.Add(1)
						mu.Lock()
						for _, t := range postTags {
							truth[t]++
						}
						mu.Unlock()
					})
			}
		}
	}

	flushCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := client.Flush(flushCtx); err != nil {
		log.Printf("flush: %v", err)
	}
	log.Printf("done: sent=%d acked=%d failed=%d", sent.Load(), acked.Load(), failed.Load())

	mu.Lock()
	defer mu.Unlock()
	printTop(truth, 10)
	if *truthOut != "" {
		data, _ := json.MarshalIndent(truth, "", "  ")
		if err := os.WriteFile(*truthOut, data, 0o644); err != nil {
			log.Fatal(err)
		}
		log.Printf("wrote ground truth for %d tags to %s", len(truth), *truthOut)
	}
}

func printTop(counts map[string]int64, n int) {
	type kv struct {
		tag   string
		count int64
	}
	all := make([]kv, 0, len(counts))
	for t, c := range counts {
		all = append(all, kv{t, c})
	}
	slices.SortFunc(all, func(a, b kv) int { return cmp.Compare(b.count, a.count) })
	fmt.Println("top tags produced:")
	for _, e := range all[:min(n, len(all))] {
		fmt.Printf("  %-14s %d\n", e.tag, e.count)
	}
}
