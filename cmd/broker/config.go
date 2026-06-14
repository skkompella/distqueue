package main

import (
	"time"

	"github.com/BurntSushi/toml"

	"github.com/skkompella/distqueue/broker"
)

// FileConfig is the on-disk tuning file (`broker.conf`) the adaptive
// control plane writes and the broker hot-reloads on SIGHUP. Only the
// runtime-tunable knobs live here; everything else stays a startup flag.
//
//	[tuning]
//	task_timeout_seconds = 12.5
//	max_retries = 3
//	worker_count = 6          # advisory: consumed by the worker, ignored here
type FileConfig struct {
	Tuning struct {
		TaskTimeoutSeconds float64 `toml:"task_timeout_seconds"`
		MaxRetries         int     `toml:"max_retries"`
		WorkerCount        int     `toml:"worker_count"`
	} `toml:"tuning"`
}

// loadFileConfig reads and parses the tuning file. A missing or malformed
// file returns an error; callers must keep the running config on error and
// never crash (a bad config push must not take the broker down).
func loadFileConfig(path string) (FileConfig, error) {
	var fc FileConfig
	_, err := toml.DecodeFile(path, &fc)
	return fc, err
}

// applyFileConfig folds a parsed FileConfig into a live broker. Fields left
// at zero are ignored by ApplyTuning, so a partial file only touches what
// it specifies.
func applyFileConfig(b *broker.Broker, fc FileConfig) (timeout time.Duration) {
	if fc.Tuning.TaskTimeoutSeconds > 0 {
		timeout = time.Duration(fc.Tuning.TaskTimeoutSeconds * float64(time.Second))
	}
	b.ApplyTuning(timeout, fc.Tuning.MaxRetries)
	return timeout
}
