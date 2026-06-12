package broker

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// WAL entry layout on disk:
//
//	| Magic (4) | EntryType (1) | PayloadLen (4) | Payload (N) | CRC32 (4) |
//
// The CRC covers EntryType + Payload. OpEnqueue payloads are a gob-encoded
// Task; OpAck/OpNack/OpDead payloads are the raw task ID bytes — that's all
// replay needs, and it keeps ack records ~30 bytes instead of re-serializing
// the whole task.

const (
	OpEnqueue byte = 0x01
	OpAck     byte = 0x02
	OpNack    byte = 0x03
	OpDead    byte = 0x04
)

var walMagic = [4]byte{'D', 'Q', 'W', 'L'}

// LogEntry is one replayed WAL record. Task is set for OpEnqueue; TaskID is
// set for all entry types.
type LogEntry struct {
	Op     byte
	TaskID string
	Task   *Task
}

// WAL is an append-only write-ahead log. A record is durable once
// WaitSync returns for its sequence number; the Append* convenience
// methods write and wait in one call.
//
// Syncs are group-committed: concurrent appenders write their records
// under the file lock, then one of them fsyncs on behalf of everyone
// waiting — so N concurrent writers cost ~1 disk sync, not N. This is
// what makes throughput scale past the per-fsync floor (see bench/).
type WAL struct {
	mu       sync.Mutex
	path     string
	file     *os.File
	entries  int    // appends since open/compaction, for the compaction trigger
	writeSeq uint64 // records fully written to the file (guarded by mu)

	syncMu    sync.Mutex
	syncCond  *sync.Cond
	syncedSeq uint64 // records known durable (guarded by syncMu)
	syncing   bool   // a sync leader is currently fsyncing
	syncErr   error  // a failed fsync poisons the WAL permanently
}

func OpenWAL(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("wal: open %s: %w", path, err)
	}
	w := &WAL{path: path, file: f}
	w.syncCond = sync.NewCond(&w.syncMu)
	return w, nil
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}

// AppendTask records a full task (OpEnqueue) and waits for durability.
func (w *WAL) AppendTask(op byte, t *Task) error {
	seq, err := w.AppendTaskNoSync(op, t)
	if err != nil {
		return err
	}
	return w.WaitSync(seq)
}

// AppendID records an ID-only entry (OpAck/OpNack/OpDead) and waits for
// durability.
func (w *WAL) AppendID(op byte, taskID string) error {
	seq, err := w.AppendIDNoSync(op, taskID)
	if err != nil {
		return err
	}
	return w.WaitSync(seq)
}

// AppendTaskNoSync writes the record and returns its sequence number
// without waiting for fsync. The record is durable only after
// WaitSync(seq) returns nil. Callers use this to apply state under their
// own lock and pay for the sync outside it.
func (w *WAL) AppendTaskNoSync(op byte, t *Task) (uint64, error) {
	payload, err := encodeTask(t)
	if err != nil {
		return 0, err
	}
	return w.appendNoSync(op, payload)
}

// AppendIDNoSync is AppendTaskNoSync for ID-only entries.
func (w *WAL) AppendIDNoSync(op byte, taskID string) (uint64, error) {
	return w.appendNoSync(op, []byte(taskID))
}

func (w *WAL) appendNoSync(op byte, payload []byte) (uint64, error) {
	rec := encodeRecord(op, payload)
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, err := w.file.Write(rec); err != nil {
		return 0, fmt.Errorf("wal: write: %w", err)
	}
	w.entries++
	w.writeSeq++
	return w.writeSeq, nil
}

// WaitSync blocks until record seq is durable. Group commit: the first
// waiter to find no sync in progress becomes the leader, fsyncs once, and
// that sync covers every record written before it started — all other
// waiters just wait for the broadcast. A failed fsync poisons the WAL:
// the durability of already-written records is unknown, so every current
// and future call returns the error (recovery happens via restart+replay).
func (w *WAL) WaitSync(seq uint64) error {
	w.syncMu.Lock()
	defer w.syncMu.Unlock()
	for {
		if w.syncErr != nil {
			return w.syncErr
		}
		if w.syncedSeq >= seq {
			return nil
		}
		if w.syncing {
			w.syncCond.Wait()
			continue
		}
		w.syncing = true
		w.syncMu.Unlock()

		w.mu.Lock()
		covered := w.writeSeq // everything written so far is on disk after this fsync
		file := w.file
		w.mu.Unlock()
		err := file.Sync()

		w.syncMu.Lock()
		w.syncing = false
		if err != nil {
			w.syncErr = fmt.Errorf("wal: fsync: %w", err)
		} else if covered > w.syncedSeq {
			w.syncedSeq = covered
		}
		w.syncCond.Broadcast()
	}
}

// EntriesSinceOpen reports appends since the WAL was opened or compacted,
// used by the broker to decide when to compact.
func (w *WAL) EntriesSinceOpen() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.entries
}

// Replay reads the log from the beginning and returns all valid entries in
// order. If it hits a corrupt or torn record (bad magic, bad CRC, short
// read — the signature of a crash mid-write), it truncates the file at the
// last valid entry and returns the valid prefix.
func (w *WAL) Replay() ([]LogEntry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("wal: seek: %w", err)
	}

	var entries []LogEntry
	var validOffset int64
	r := newCountingReader(w.file)

	for {
		entry, err := readRecord(r)
		if err == io.EOF {
			break
		}
		if err != nil {
			// Torn or corrupt tail: drop everything from the last valid
			// entry onward.
			if terr := w.file.Truncate(validOffset); terr != nil {
				return nil, fmt.Errorf("wal: truncate corrupt tail: %w", terr)
			}
			if serr := w.file.Sync(); serr != nil {
				return nil, fmt.Errorf("wal: fsync after truncate: %w", serr)
			}
			break
		}
		entries = append(entries, entry)
		validOffset = r.n
	}

	if _, err := w.file.Seek(0, io.SeekEnd); err != nil {
		return nil, fmt.Errorf("wal: seek end: %w", err)
	}
	return entries, nil
}

// Rewrite atomically replaces the log contents with the given entries:
// write temp file, fsync, rename over the original, fsync the directory.
// Used for compaction. The entries must capture the effects of every
// record written so far (the broker builds them from its applied state),
// so a successful rewrite makes all outstanding records durable: pending
// WaitSync waiters are released.
func (w *WAL) Rewrite(entries []LogEntry) error {
	// Exclude sync leaders for the duration: an fsync racing the handle
	// swap below would sync a closed file and falsely poison the WAL.
	w.syncMu.Lock()
	defer w.syncMu.Unlock()
	for w.syncing {
		w.syncCond.Wait()
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	tmpPath := w.path + ".tmp"
	tmp, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("wal: create temp: %w", err)
	}
	defer os.Remove(tmpPath) // no-op after successful rename

	for _, e := range entries {
		var payload []byte
		if e.Op == OpEnqueue {
			if payload, err = encodeTask(e.Task); err != nil {
				tmp.Close()
				return err
			}
		} else {
			payload = []byte(e.TaskID)
		}
		if _, err := tmp.Write(encodeRecord(e.Op, payload)); err != nil {
			tmp.Close()
			return fmt.Errorf("wal: write temp: %w", err)
		}
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("wal: fsync temp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("wal: close temp: %w", err)
	}

	if err := os.Rename(tmpPath, w.path); err != nil {
		return fmt.Errorf("wal: rename: %w", err)
	}
	if dir, err := os.Open(filepath.Dir(w.path)); err == nil {
		dir.Sync()
		dir.Close()
	}

	old := w.file
	f, err := os.OpenFile(w.path, os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("wal: reopen after compaction: %w", err)
	}
	old.Close()
	w.file = f
	w.entries = 0

	// The compacted file is durable and embodies every written record.
	w.syncedSeq = w.writeSeq
	w.syncCond.Broadcast()
	return nil
}

// --- record encoding ---

func encodeRecord(op byte, payload []byte) []byte {
	buf := make([]byte, 0, 4+1+4+len(payload)+4)
	buf = append(buf, walMagic[:]...)
	buf = append(buf, op)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(payload)))
	buf = append(buf, payload...)

	crc := crc32.NewIEEE()
	crc.Write([]byte{op})
	crc.Write(payload)
	buf = binary.BigEndian.AppendUint32(buf, crc.Sum32())
	return buf
}

var errCorrupt = errors.New("wal: corrupt record")

func readRecord(r io.Reader) (LogEntry, error) {
	header := make([]byte, 4+1+4)
	if _, err := io.ReadFull(r, header); err != nil {
		if err == io.EOF {
			return LogEntry{}, io.EOF
		}
		return LogEntry{}, errCorrupt // short read mid-record = torn write
	}
	if !bytes.Equal(header[:4], walMagic[:]) {
		return LogEntry{}, errCorrupt
	}
	op := header[4]
	plen := binary.BigEndian.Uint32(header[5:9])
	if plen > 64<<20 { // sanity cap: a corrupt length would OOM us
		return LogEntry{}, errCorrupt
	}

	rest := make([]byte, plen+4)
	if _, err := io.ReadFull(r, rest); err != nil {
		return LogEntry{}, errCorrupt
	}
	payload, stored := rest[:plen], binary.BigEndian.Uint32(rest[plen:])

	crc := crc32.NewIEEE()
	crc.Write([]byte{op})
	crc.Write(payload)
	if crc.Sum32() != stored {
		return LogEntry{}, errCorrupt
	}

	entry := LogEntry{Op: op}
	if op == OpEnqueue {
		t, err := decodeTask(payload)
		if err != nil {
			return LogEntry{}, errCorrupt
		}
		entry.Task = t
		entry.TaskID = t.ID
	} else {
		entry.TaskID = string(payload)
	}
	return entry, nil
}

func encodeTask(t *Task) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(t); err != nil {
		return nil, fmt.Errorf("wal: encode task: %w", err)
	}
	return buf.Bytes(), nil
}

func decodeTask(b []byte) (*Task, error) {
	var t Task
	if err := gob.NewDecoder(bytes.NewReader(b)).Decode(&t); err != nil {
		return nil, err
	}
	return &t, nil
}

// countingReader tracks bytes consumed so Replay knows the offset of the
// last fully-valid record.
type countingReader struct {
	r io.Reader
	n int64
}

func newCountingReader(r io.Reader) *countingReader { return &countingReader{r: r} }

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}
