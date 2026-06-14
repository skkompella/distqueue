// The produce binary: enqueues N demo tasks, for quickstarts and failure
// demos. Also prints broker stats with --stats.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"time"

	"github.com/skkompella/distqueue/client"
	"github.com/skkompella/distqueue/gen/queuepb"
)

func main() {
	var (
		brokerAddr = flag.String("broker", "localhost:9000", "broker address(es), comma-separated for a cluster")
		count      = flag.Int("count", 10, "number of tasks to enqueue")
		priorities = flag.Int("priorities", 3, "spread tasks across this many priority levels")
		stats      = flag.Bool("stats", false, "print broker stats and exit")
		dlq        = flag.Bool("dlq", false, "list dead-letter queue and exit")
	)
	flag.Parse()

	qc := client.New(strings.Split(*brokerAddr, ","))
	defer qc.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	switch {
	case *stats:
		s, err := qc.Stats(ctx, &queuepb.StatsRequest{})
		if err != nil {
			log.Fatalf("stats: %v", err)
		}
		fmt.Printf("pending=%d in_flight=%d dlq=%d acked=%d\n",
			s.GetPending(), s.GetInFlight(), s.GetDlq(), s.GetAcked())
	case *dlq:
		resp, err := qc.ListDLQ(ctx, &queuepb.ListDLQRequest{})
		if err != nil {
			log.Fatalf("dlq: %v", err)
		}
		for _, t := range resp.GetTasks() {
			fmt.Printf("dead task=%s retries=%d payload=%q\n", t.GetId(), t.GetRetryCount(), t.GetPayload())
		}
		fmt.Printf("%d dead-lettered task(s)\n", len(resp.GetTasks()))
	default:
		for i := 0; i < *count; i++ {
			resp, err := qc.Enqueue(ctx, &queuepb.EnqueueRequest{Task: &queuepb.Task{
				Payload:  []byte(fmt.Sprintf("job-%d", i)),
				Priority: int32(rand.Intn(*priorities)),
			}})
			if err != nil {
				log.Fatalf("enqueue %d: %v", i, err)
			}
			_ = resp
		}
		fmt.Printf("enqueued %d tasks\n", *count)
	}
}
