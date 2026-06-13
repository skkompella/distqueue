package raft

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
)

// This test layers a single-register KV on top of the raft package and
// checks the operation history with Porcupine while the leader is
// repeatedly killed. Reads are routed through the log, so every operation
// linearizes at its apply point. The queue's at-least-once redelivery
// semantics deliberately aren't linearizable, so the consensus core is
// verified here, with a strongly-consistent state machine.

type kvCmd struct {
	Op  string // "put" or "get"
	Val string
}

type kvInput struct {
	Op  string
	Val string
}

type kvOutput struct {
	Val string
}

var kvModel = porcupine.Model{
	Init: func() any { return "" },
	Step: func(state, input, output any) (bool, any) {
		st := state.(string)
		in := input.(kvInput)
		if in.Op == "put" {
			return true, in.Val
		}
		out := output.(kvOutput)
		return out.Val == st, st
	},
	DescribeOperation: func(input, output any) string {
		in := input.(kvInput)
		if in.Op == "put" {
			return fmt.Sprintf("put(%q)", in.Val)
		}
		return fmt.Sprintf("get() -> %q", output.(kvOutput).Val)
	},
}

// kvNode wraps one raft node with a register state machine and a
// propose-and-wait API (the same waiter pattern as the cluster server).
type kvNode struct {
	node *Node

	mu      sync.Mutex
	state   string
	waiters map[int]chan kvResult
	stopCh  chan struct{}
}

type kvResult struct {
	term int
	val  string
}

func newKVNode(node *Node, applyCh chan ApplyMsg) *kvNode {
	kv := &kvNode{
		node:    node,
		waiters: make(map[int]chan kvResult),
		stopCh:  make(chan struct{}),
	}
	go kv.applyLoop(applyCh)
	return kv
}

func (kv *kvNode) applyLoop(applyCh chan ApplyMsg) {
	for {
		select {
		case <-kv.stopCh:
			return
		case msg, ok := <-applyCh:
			if !ok {
				return
			}
			if !msg.CommandValid || len(msg.Command) == 0 {
				continue
			}
			var cmd kvCmd
			if err := gob.NewDecoder(bytes.NewReader(msg.Command)).Decode(&cmd); err != nil {
				panic(err)
			}
			kv.mu.Lock()
			if cmd.Op == "put" {
				kv.state = cmd.Val
			}
			if ch, ok := kv.waiters[msg.CommandIndex]; ok {
				delete(kv.waiters, msg.CommandIndex)
				ch <- kvResult{term: msg.CommandTerm, val: kv.state}
			}
			kv.mu.Unlock()
		}
	}
}

type kvStatus int

const (
	kvOK kvStatus = iota
	kvNotApplied      // definitely did not take effect
	kvMaybeApplied    // accepted by a leader; outcome unknown
)

// do proposes cmd and waits for it to apply on this node.
func (kv *kvNode) do(cmd kvCmd) (string, kvStatus) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(cmd); err != nil {
		panic(err)
	}
	index, term, isLeader := kv.node.Propose(buf.Bytes())
	if !isLeader {
		return "", kvNotApplied
	}
	ch := make(chan kvResult, 1)
	kv.mu.Lock()
	kv.waiters[index] = ch
	kv.mu.Unlock()

	select {
	case res := <-ch:
		if res.term != term {
			return "", kvNotApplied // a different leader's entry took our slot
		}
		return res.val, kvOK
	case <-time.After(2 * time.Second):
		kv.mu.Lock()
		delete(kv.waiters, index)
		kv.mu.Unlock()
		// The entry was accepted by a leader and could still commit
		// later (e.g. after a partition heals): ambiguous.
		return "", kvMaybeApplied
	}
}

func TestLinearizability(t *testing.T) {
	if testing.Short() {
		t.Skip("linearizability check is slow")
	}
	// The shared cluster harness owns each node's applyCh, so this test
	// wires its own 3 nodes with kvNode consumers instead.
	net := newMemNetwork()
	ids := []string{"kv1", "kv2", "kv3"}
	kvs := make(map[string]*kvNode)
	var nodes []*Node
	for _, id := range ids {
		var peers []string
		for _, p := range ids {
			if p != id {
				peers = append(peers, p)
			}
		}
		applyCh := make(chan ApplyMsg, 256)
		n, err := NewNode(Config{
			ID:                 id,
			Peers:              peers,
			ElectionTimeoutMin: 60 * time.Millisecond,
			ElectionTimeoutMax: 120 * time.Millisecond,
			HeartbeatInterval:  20 * time.Millisecond,
		}, &memTransport{net: net, from: id}, applyCh)
		if err != nil {
			t.Fatal(err)
		}
		net.mu.Lock()
		net.nodes[id] = n
		net.mu.Unlock()
		kvs[id] = newKVNode(n, applyCh)
		nodes = append(nodes, n)
		n.Start()
	}
	defer func() {
		for _, n := range nodes {
			n.Stop()
		}
		for _, kv := range kvs {
			close(kv.stopCh)
		}
	}()

	// Chaos: every 250ms, cut the current leader off for 150ms.
	chaosDone := make(chan struct{})
	go func() {
		defer close(chaosDone)
		for round := 0; round < 8; round++ {
			time.Sleep(250 * time.Millisecond)
			for _, id := range ids {
				if _, isLeader := kvs[id].node.State(); isLeader {
					net.mu.Lock()
					net.cut[id] = true
					net.mu.Unlock()
					time.Sleep(150 * time.Millisecond)
					net.mu.Lock()
					delete(net.cut, id)
					net.mu.Unlock()
					break
				}
			}
		}
	}()

	// Concurrent clients hammering the register through whichever node
	// will take the op.
	const clients = 5
	const opsPerClient = 30
	var (
		opsMu sync.Mutex
		ops   []porcupine.Operation
	)
	var wg sync.WaitGroup
	for cid := 0; cid < clients; cid++ {
		wg.Add(1)
		go func(cid int) {
			defer wg.Done()
			for i := 0; i < opsPerClient; i++ {
				isPut := rand.Intn(2) == 0
				cmd := kvCmd{Op: "get"}
				if isPut {
					cmd = kvCmd{Op: "put", Val: fmt.Sprintf("c%d-%d", cid, i)}
				}

				call := time.Now().UnixNano()
				var val string
				status := kvNotApplied
				// Try nodes until one takes it (or give up this op).
				for attempt := 0; attempt < 12 && status == kvNotApplied; attempt++ {
					id := ids[attempt%len(ids)]
					val, status = kvs[id].do(cmd)
					if status == kvNotApplied {
						time.Sleep(20 * time.Millisecond)
					}
				}
				ret := time.Now().UnixNano()

				switch status {
				case kvOK:
					opsMu.Lock()
					ops = append(ops, porcupine.Operation{
						ClientId: cid,
						Input:    kvInput{Op: cmd.Op, Val: cmd.Val},
						Output:   kvOutput{Val: val},
						Call:     call,
						Return:   ret,
					})
					opsMu.Unlock()
				case kvMaybeApplied:
					if cmd.Op == "put" {
						// May take effect at any later moment: open-ended.
						opsMu.Lock()
						ops = append(ops, porcupine.Operation{
							ClientId: cid,
							Input:    kvInput{Op: cmd.Op, Val: cmd.Val},
							Output:   kvOutput{},
							Call:     call,
							Return:   math.MaxInt64,
						})
						opsMu.Unlock()
					}
					// Ambiguous gets have no effect: drop them.
				case kvNotApplied:
					// Never accepted anywhere: no effect on history.
				}
			}
		}(cid)
	}
	wg.Wait()
	<-chaosDone

	opsMu.Lock()
	history := append([]porcupine.Operation(nil), ops...)
	opsMu.Unlock()
	if len(history) < clients*opsPerClient/2 {
		t.Fatalf("too few completed operations to be meaningful: %d", len(history))
	}

	res, info := porcupine.CheckOperationsVerbose(kvModel, history, 30*time.Second)
	if res != porcupine.Ok {
		_ = info
		t.Fatalf("history is NOT linearizable (%d ops)", len(history))
	}
	t.Logf("linearizable: %d operations across %d clients with leader kills", len(history), clients)
}
