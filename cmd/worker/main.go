// The worker binary: connects to a broker and runs a demo handler that logs
// each payload. --fail-rate makes a fraction of tasks fail, to exercise the
// retry and dead-letter paths in demos.
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/skkompella/distqueue/client"
	"github.com/skkompella/distqueue/gen/queuepb"
	"github.com/skkompella/distqueue/worker"
)

func main() {
	var (
		brokerAddr  = flag.String("broker", "localhost:9000", "broker address(es), comma-separated for a cluster")
		concurrency = flag.Int("concurrency", 1, "starting number of concurrent task handlers")
		autoScale   = flag.Bool("auto-scale", true, "resize the pool to the control plane's advised worker count (polled via Stats)")
		maxWorkers  = flag.Int("max-workers", 64, "hard cap on the autoscaled pool")
		failRate    = flag.Float64("fail-rate", 0, "fraction of tasks the demo handler fails (0..1)")
		workTime    = flag.Duration("work-time", 50*time.Millisecond, "simulated processing time per task")
	)
	flag.Parse()

	qc := client.New(strings.Split(*brokerAddr, ","))
	defer qc.Close()

	handler := func(ctx context.Context, task *queuepb.Task) error {
		time.Sleep(*workTime)
		if rand.Float64() < *failRate {
			log.Printf("FAIL task=%s retry=%d", task.GetId(), task.GetRetryCount())
			return errors.New("simulated failure")
		}
		log.Printf("done task=%s priority=%d payload=%q", task.GetId(), task.GetPriority(), task.GetPayload())
		return nil
	}

	w := worker.New(qc, handler, worker.Config{
		Concurrency: *concurrency,
		AutoScale:   *autoScale,
		MaxWorkers:  *maxWorkers,
		Logger:      log.Default(),
	})

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.Printf("worker connected to %s (concurrency=%d auto-scale=%v fail-rate=%.2f)",
		*brokerAddr, *concurrency, *autoScale, *failRate)
	w.Run(ctx)
	log.Println("worker stopped")
}
