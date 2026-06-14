package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/skkompella/distqueue/broker"
)

func writeTemp(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "broker.conf")
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestLoadFileConfigValid(t *testing.T) {
	p := writeTemp(t, `
[tuning]
task_timeout_seconds = 12.5
max_retries = 3
worker_count = 6
`)
	fc, err := loadFileConfig(p)
	if err != nil {
		t.Fatal(err)
	}
	if fc.Tuning.TaskTimeoutSeconds != 12.5 || fc.Tuning.MaxRetries != 3 || fc.Tuning.WorkerCount != 6 {
		t.Fatalf("parsed wrong values: %+v", fc.Tuning)
	}
}

func TestLoadFileConfigMalformed(t *testing.T) {
	p := writeTemp(t, "this is = not valid = toml [[[")
	if _, err := loadFileConfig(p); err == nil {
		t.Fatal("expected an error from malformed TOML")
	}
}

func TestLoadFileConfigAbsent(t *testing.T) {
	if _, err := loadFileConfig(filepath.Join(t.TempDir(), "nope.conf")); err == nil {
		t.Fatal("expected an error for an absent file")
	}
}

func TestApplyFileConfigPartial(t *testing.T) {
	// A file that sets only the timeout must leave max_retries untouched.
	b, err := broker.New(func() broker.Config {
		c := broker.DefaultConfig(filepath.Join(t.TempDir(), "w.wal"))
		c.TaskTimeout = 30 * time.Second
		c.MaxRetries = 5
		return c
	}())
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	var fc FileConfig
	fc.Tuning.TaskTimeoutSeconds = 8 // max_retries left zero → unchanged
	timeout := applyFileConfig(b, fc)
	if timeout != 8*time.Second {
		t.Fatalf("expected 8s timeout, got %s", timeout)
	}

	// Verify via a dequeue that the timeout applied and retries are intact.
	if err := b.Enqueue(&broker.Task{ID: "x", Priority: 1}); err != nil {
		t.Fatal(err)
	}
	task, err := b.Dequeue()
	if err != nil {
		t.Fatal(err)
	}
	if d := time.Until(task.Deadline); d < 7*time.Second || d > 9*time.Second {
		t.Fatalf("expected ~8s deadline, got %s", d)
	}
}
