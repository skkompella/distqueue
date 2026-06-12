package broker

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func tempWAL(t *testing.T) (*WAL, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.wal")
	w, err := OpenWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { w.Close() })
	return w, path
}

func mkTask(id string, prio int) *Task {
	return &Task{
		ID:        id,
		Payload:   []byte("payload-" + id),
		Priority:  prio,
		CreatedAt: time.Now().Truncate(time.Microsecond),
	}
}

func TestWALRoundtrip(t *testing.T) {
	w, _ := tempWAL(t)

	task := mkTask("t1", 3)
	task.RetryCount = 2
	if err := w.AppendTask(OpEnqueue, task); err != nil {
		t.Fatal(err)
	}
	if err := w.AppendID(OpAck, "t1"); err != nil {
		t.Fatal(err)
	}
	if err := w.AppendID(OpNack, "t2"); err != nil {
		t.Fatal(err)
	}
	if err := w.AppendID(OpDead, "t3"); err != nil {
		t.Fatal(err)
	}

	entries, err := w.Replay()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 4 {
		t.Fatalf("expected 4 entries, got %d", len(entries))
	}
	e := entries[0]
	if e.Op != OpEnqueue || e.Task == nil || e.Task.ID != "t1" ||
		string(e.Task.Payload) != "payload-t1" || e.Task.Priority != 3 || e.Task.RetryCount != 2 {
		t.Fatalf("enqueue entry mismatch: %+v task=%+v", e, e.Task)
	}
	for i, want := range []struct {
		op byte
		id string
	}{{OpAck, "t1"}, {OpNack, "t2"}, {OpDead, "t3"}} {
		e := entries[i+1]
		if e.Op != want.op || e.TaskID != want.id {
			t.Fatalf("entry %d: expected op=%#x id=%s, got op=%#x id=%s", i+1, want.op, want.id, e.Op, e.TaskID)
		}
	}
}

func TestWALReplaySurvivesReopen(t *testing.T) {
	w, path := tempWAL(t)
	for i := 0; i < 10; i++ {
		if err := w.AppendTask(OpEnqueue, mkTask(string(rune('a'+i)), i)); err != nil {
			t.Fatal(err)
		}
	}
	w.Close()

	w2, err := OpenWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	entries, err := w2.Replay()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 10 {
		t.Fatalf("expected 10 entries after reopen, got %d", len(entries))
	}
}

func TestWALCorruptTailTruncated(t *testing.T) {
	w, path := tempWAL(t)
	for i := 0; i < 5; i++ {
		if err := w.AppendTask(OpEnqueue, mkTask(string(rune('a'+i)), i)); err != nil {
			t.Fatal(err)
		}
	}
	w.Close()

	// Flip a byte inside the last record's payload to simulate a torn write.
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-10] ^= 0xFF
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}

	w2, err := OpenWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	entries, err := w2.Replay()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 4 {
		t.Fatalf("expected 4 valid entries after corruption, got %d", len(entries))
	}

	// The file must have been truncated to the valid prefix, and appends
	// after truncation must replay cleanly.
	if err := w2.AppendTask(OpEnqueue, mkTask("post-corruption", 1)); err != nil {
		t.Fatal(err)
	}
	entries, err = w2.Replay()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 5 || entries[4].TaskID != "post-corruption" {
		t.Fatalf("expected 5 entries ending in post-corruption, got %d", len(entries))
	}
}

func TestWALPartialRecordTruncated(t *testing.T) {
	w, path := tempWAL(t)
	for i := 0; i < 3; i++ {
		if err := w.AppendTask(OpEnqueue, mkTask(string(rune('a'+i)), i)); err != nil {
			t.Fatal(err)
		}
	}
	w.Close()

	// Chop the file mid-record: a crash during a write leaves a short tail.
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data[:len(data)-7], 0o644); err != nil {
		t.Fatal(err)
	}

	w2, err := OpenWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	entries, err := w2.Replay()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 valid entries after torn write, got %d", len(entries))
	}
}

func TestWALRewrite(t *testing.T) {
	w, _ := tempWAL(t)
	for i := 0; i < 20; i++ {
		if err := w.AppendTask(OpEnqueue, mkTask(string(rune('a'+i)), i)); err != nil {
			t.Fatal(err)
		}
	}

	keep := mkTask("survivor", 1)
	if err := w.Rewrite([]LogEntry{{Op: OpEnqueue, Task: keep, TaskID: keep.ID}}); err != nil {
		t.Fatal(err)
	}
	entries, err := w.Replay()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].TaskID != "survivor" {
		t.Fatalf("expected single survivor entry, got %+v", entries)
	}
	if w.EntriesSinceOpen() != 0 {
		t.Fatalf("expected entry counter reset after rewrite, got %d", w.EntriesSinceOpen())
	}

	// Appends after a rewrite must land in the new file.
	if err := w.AppendID(OpAck, "survivor"); err != nil {
		t.Fatal(err)
	}
	entries, err = w.Replay()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries after post-rewrite append, got %d", len(entries))
	}
}
