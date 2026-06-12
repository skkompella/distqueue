package broker

import (
	"testing"
	"time"
)

func TestPriorityOrdering(t *testing.T) {
	pq := NewPriorityQueue()
	now := time.Now()
	pq.Push(&Task{ID: "low", Priority: 5, CreatedAt: now})
	pq.Push(&Task{ID: "high", Priority: 1, CreatedAt: now})
	pq.Push(&Task{ID: "mid", Priority: 3, CreatedAt: now})

	for _, want := range []string{"high", "mid", "low"} {
		got := pq.Pop()
		if got == nil || got.ID != want {
			t.Fatalf("expected %q, got %v", want, got)
		}
	}
	if pq.Pop() != nil {
		t.Fatal("expected nil from empty queue")
	}
}

func TestFIFOTiebreak(t *testing.T) {
	pq := NewPriorityQueue()
	base := time.Now()
	for i := 0; i < 5; i++ {
		pq.Push(&Task{
			ID:        string(rune('a' + i)),
			Priority:  1,
			CreatedAt: base.Add(time.Duration(i) * time.Millisecond),
		})
	}
	for i := 0; i < 5; i++ {
		got := pq.Pop()
		want := string(rune('a' + i))
		if got.ID != want {
			t.Fatalf("position %d: expected %q, got %q", i, want, got.ID)
		}
	}
}

func TestLen(t *testing.T) {
	pq := NewPriorityQueue()
	if pq.Len() != 0 {
		t.Fatalf("expected 0, got %d", pq.Len())
	}
	pq.Push(&Task{ID: "x", Priority: 1, CreatedAt: time.Now()})
	pq.Push(&Task{ID: "y", Priority: 2, CreatedAt: time.Now()})
	if pq.Len() != 2 {
		t.Fatalf("expected 2, got %d", pq.Len())
	}
	pq.Pop()
	if pq.Len() != 1 {
		t.Fatalf("expected 1, got %d", pq.Len())
	}
}
