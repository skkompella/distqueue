package broker

import "container/heap"

// PriorityQueue is a min-heap of tasks ordered by Priority (lower value =
// dequeued first), with CreatedAt as the tiebreak so equal-priority tasks
// are FIFO. It is not goroutine-safe; the Broker serializes access.
type PriorityQueue struct {
	h taskHeap
}

func NewPriorityQueue() *PriorityQueue {
	pq := &PriorityQueue{}
	heap.Init(&pq.h)
	return pq
}

func (pq *PriorityQueue) Push(t *Task) {
	heap.Push(&pq.h, t)
}

// Pop removes and returns the highest-priority task, or nil if empty.
func (pq *PriorityQueue) Pop() *Task {
	if pq.h.Len() == 0 {
		return nil
	}
	return heap.Pop(&pq.h).(*Task)
}

func (pq *PriorityQueue) Len() int { return pq.h.Len() }

// taskHeap implements heap.Interface.
type taskHeap []*Task

func (h taskHeap) Len() int { return len(h) }

func (h taskHeap) Less(i, j int) bool {
	if h[i].Priority != h[j].Priority {
		return h[i].Priority < h[j].Priority
	}
	return h[i].CreatedAt.Before(h[j].CreatedAt)
}

func (h taskHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *taskHeap) Push(x any) { *h = append(*h, x.(*Task)) }

func (h *taskHeap) Pop() any {
	old := *h
	n := len(old)
	t := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return t
}
