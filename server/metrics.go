package server

import (
	"fmt"
	"log"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/skkompella/distqueue/broker"
	"github.com/skkompella/distqueue/raft"
)

// StatsSource is anything that can report queue stats (single-node Broker
// or replicated StateMachine).
type StatsSource interface {
	Stats() broker.Stats
}

// ServeMetrics exposes Prometheus metrics on /metrics at the given port.
// node may be nil (single-node mode: queue metrics only).
func ServeMetrics(port int, nodeID string, node *raft.Node, stats StatsSource) {
	reg := prometheus.NewRegistry()
	labels := prometheus.Labels{"node": nodeID}

	gauge := func(name, help string, fn func() float64) {
		reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: name, Help: help, ConstLabels: labels,
		}, fn))
	}

	gauge("queue_pending", "Tasks waiting for delivery", func() float64 {
		return float64(stats.Stats().Pending)
	})
	gauge("queue_in_flight", "Tasks delivered but not yet acked", func() float64 {
		return float64(stats.Stats().InFlight)
	})
	gauge("queue_dlq_depth", "Dead-lettered tasks", func() float64 {
		return float64(stats.Stats().DLQ)
	})
	gauge("queue_acked_total", "Tasks acknowledged since startup", func() float64 {
		return float64(stats.Stats().Acked)
	})

	if node != nil {
		gauge("raft_term", "Current Raft term", func() float64 {
			return float64(node.Metrics().Term)
		})
		gauge("raft_is_leader", "1 if this node is the leader", func() float64 {
			if node.Metrics().State == raft.Leader {
				return 1
			}
			return 0
		})
		gauge("raft_log_entries", "Log entries currently held (post-compaction)", func() float64 {
			return float64(node.Metrics().LogLength)
		})
		gauge("raft_commit_index", "Highest committed log index", func() float64 {
			return float64(node.Metrics().CommitIndex)
		})
		gauge("raft_last_applied", "Highest applied log index", func() float64 {
			return float64(node.Metrics().LastApplied)
		})
		gauge("raft_snapshot_index", "Log index covered by the latest snapshot", func() float64 {
			return float64(node.Metrics().SnapshotIndex)
		})
		gauge("raft_elections_started_total", "Elections this node has started", func() float64 {
			return float64(node.Metrics().ElectionsStarted)
		})
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	go func() {
		if err := http.ListenAndServe(fmt.Sprintf(":%d", port), mux); err != nil {
			log.Printf("metrics server: %v", err)
		}
	}()
}
