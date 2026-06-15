// Package bench measures broker throughput and end-to-end latency.
//
// Run:  go test -bench . -benchtime 3s ./bench/
//
// Note on numbers: every mutation is fsync'd before it is acknowledged,
// so SINGLE-threaded enqueue throughput is bounded by disk sync latency
// (~1/fsync-time). The WAL group-commits: concurrent producers share one
// fsync, so the parallel benchmarks scale far past the single-thread
// floor. tmpfs vs real disk changes the floor dramatically.
package bench

import (
	"fmt"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/skkompella/distqueue/broker"
)

func benchBroker(b *testing.B) *broker.Broker {
	b.Helper()
	cfg := broker.DefaultConfig(filepath.Join(b.TempDir(), "bench.wal"))
	cfg.CompactEvery = 0 // measure raw append path, not compaction
	br, err := broker.New(cfg)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { br.Close() })
	return br
}

func BenchmarkEnqueue(b *testing.B) {
	br := benchBroker(b)
	payload := make([]byte, 256)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := br.Enqueue(&broker.Task{Payload: payload, Priority: i % 10}); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "tasks/sec")
}

func BenchmarkEnqueueParallel(b *testing.B) {
	br := benchBroker(b)
	payload := make([]byte, 256)
	// 8× GOMAXPROCS producers: models a fleet of clients and lets group
	// commit batch many records per fsync.
	b.SetParallelism(8)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := br.Enqueue(&broker.Task{Payload: payload, Priority: 1}); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "tasks/sec")
}

func BenchmarkDequeue(b *testing.B) {
	br := benchBroker(b)
	payload := make([]byte, 256)
	// Fill with concurrent producers: group commit batches their fsyncs,
	// so setup doesn't take b.N disk syncs on a slow disk. The measured
	// dequeue loop below does no I/O at all.
	var wg sync.WaitGroup
	const fillers = 64
	for f := 0; f < fillers; f++ {
		wg.Add(1)
		go func(f int) {
			defer wg.Done()
			for i := f; i < b.N; i += fillers {
				if err := br.Enqueue(&broker.Task{Payload: payload, Priority: i % 10}); err != nil {
					b.Error(err)
					return
				}
			}
		}(f)
	}
	wg.Wait()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := br.Dequeue(); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "tasks/sec")
}

// BenchmarkEndToEnd measures the full broker round trip per task:
// enqueue → dequeue → ack, the path a producer+worker pair exercises.
func BenchmarkEndToEnd(b *testing.B) {
	br := benchBroker(b)
	payload := make([]byte, 256)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := br.Enqueue(&broker.Task{Payload: payload, Priority: 1}); err != nil {
			b.Fatal(err)
		}
		task, err := br.Dequeue()
		if err != nil {
			b.Fatal(err)
		}
		if err := br.Ack(task.ID); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "tasks/sec")
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/1000, "µs/task")
}

// TestEnqueueScaling sweeps producer concurrency and prints throughput at
// each level, so the group-commit story is a real curve, not two points:
// N goroutines hammer Enqueue for a fixed window and we report tasks/sec.
// Single-threaded is fsync-bound; concurrency lets group commit batch many
// records per sync. Run: go test -run TestEnqueueScaling -v ./bench/
func TestEnqueueScaling(t *testing.T) {
	if testing.Short() {
		t.Skip("scaling sweep skipped in -short")
	}
	cfg := broker.DefaultConfig(filepath.Join(t.TempDir(), "scale.wal"))
	cfg.CompactEvery = 0
	br, err := broker.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer br.Close()

	payload := make([]byte, 256)
	const window = 1500 * time.Millisecond
	for _, producers := range []int{1, 2, 4, 8, 16, 32, 64, 128} {
		var ops atomic.Int64
		var wg sync.WaitGroup
		deadline := time.Now().Add(window)
		for p := 0; p < producers; p++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for time.Now().Before(deadline) {
					if err := br.Enqueue(&broker.Task{Payload: payload, Priority: 1}); err != nil {
						t.Error(err)
						return
					}
					ops.Add(1)
				}
			}()
		}
		wg.Wait()
		rate := float64(ops.Load()) / window.Seconds()
		fmt.Printf("scaling producers=%d tasks_per_sec=%.0f\n", producers, rate)
	}
}

// TestLatencyDistribution is a -run-only helper (not a Benchmark) that
// prints P50/P99 enqueue→ack latency for the README table.
// Run: go test -run TestLatencyDistribution -v ./bench/
func TestLatencyDistribution(t *testing.T) {
	if testing.Short() {
		t.Skip("latency sampling skipped in -short")
	}
	cfg := broker.DefaultConfig(filepath.Join(t.TempDir(), "lat.wal"))
	br, err := broker.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer br.Close()

	const samples = 5000
	payload := make([]byte, 256)
	lat := make([]time.Duration, 0, samples)
	for i := 0; i < samples; i++ {
		start := time.Now()
		if err := br.Enqueue(&broker.Task{Payload: payload, Priority: 1}); err != nil {
			t.Fatal(err)
		}
		task, err := br.Dequeue()
		if err != nil {
			t.Fatal(err)
		}
		if err := br.Ack(task.ID); err != nil {
			t.Fatal(err)
		}
		lat = append(lat, time.Since(start))
	}
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	p := func(q float64) time.Duration {
		return lat[int(q*float64(len(lat)-1))]
	}
	fmt.Printf("round-trip latency over %d samples: P50=%v P95=%v P99=%v\n",
		samples, p(0.50), p(0.95), p(0.99))
}
