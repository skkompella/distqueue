package raft

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// Raft requires currentTerm, votedFor, and log[] to be durable before a
// node responds to any RPC (Figure 2): a restarted node must not vote
// twice in one term or forget entries it acknowledged. State is written
// with the same atomic pattern as the Phase 1 WAL compaction: temp file,
// fsync, rename, fsync directory.
//
// The whole state is rewritten on each persist. That is O(log) per write,
// which is acceptable because snapshots keep the log short (see
// DESIGN.md for the trade-off discussion).

type persistentState struct {
	CurrentTerm int
	VotedFor    string
	Log         []LogEntry
}

func (n *Node) statePath() string    { return filepath.Join(n.cfg.DataDir, "raft-state.gob") }
func (n *Node) snapshotPath() string { return filepath.Join(n.cfg.DataDir, "snapshot.bin") }

// persistLocked durably saves term, vote, and log. Caller holds mu.
// A persistence failure is fatal by design: continuing after losing
// durability would let this node break its Raft promises.
func (n *Node) persistLocked() {
	if n.cfg.DataDir == "" {
		return // ephemeral node (tests)
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(persistentState{
		CurrentTerm: n.currentTerm,
		VotedFor:    n.votedFor,
		Log:         n.log,
	}); err != nil {
		panic(fmt.Sprintf("raft: encode persistent state: %v", err))
	}
	if err := atomicWrite(n.statePath(), buf.Bytes()); err != nil {
		panic(fmt.Sprintf("raft: persist: %v", err))
	}
}

// persistWithSnapshotLocked saves state and snapshot together (used when
// the log is trimmed, so a crash can't leave a log that starts after a
// snapshot we don't have).
func (n *Node) persistWithSnapshotLocked(snapshot []byte) {
	if n.cfg.DataDir == "" {
		return // ephemeral node (tests)
	}
	if err := atomicWrite(n.snapshotPath(), snapshot); err != nil {
		panic(fmt.Sprintf("raft: persist snapshot: %v", err))
	}
	n.persistLocked()
}

// readPersist restores state from disk on startup. Missing files mean a
// fresh node.
func (n *Node) readPersist() error {
	if n.cfg.DataDir == "" {
		return nil // ephemeral node (tests)
	}
	if err := os.MkdirAll(n.cfg.DataDir, 0o755); err != nil {
		return err
	}

	data, err := os.ReadFile(n.statePath())
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var ps persistentState
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&ps); err != nil {
		return fmt.Errorf("decode %s: %w", n.statePath(), err)
	}
	n.currentTerm = ps.CurrentTerm
	n.votedFor = ps.VotedFor
	n.log = ps.Log

	snap, err := os.ReadFile(n.snapshotPath())
	if err == nil && len(snap) > 0 {
		n.snapshot = snap
		// The state machine restores from this snapshot before consuming
		// any commands.
		n.pendingSnapshot = &ApplyMsg{
			SnapshotValid: true,
			Snapshot:      snap,
			SnapshotIndex: n.log[0].Index,
			SnapshotTerm:  n.log[0].Term,
		}
	} else if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}

// atomicWrite writes data to path via temp file + fsync + rename + dir
// fsync, so the file is either fully old or fully new after a crash.
func atomicWrite(path string, data []byte) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer os.Remove(tmp)
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	if dir, err := os.Open(filepath.Dir(path)); err == nil {
		dir.Sync()
		dir.Close()
	}
	return nil
}
