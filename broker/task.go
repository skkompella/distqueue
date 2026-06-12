package broker

import "time"

// TaskStatus tracks where a task is in its lifecycle.
type TaskStatus int

const (
	Pending TaskStatus = iota
	InFlight
	Done
	Dead
)

func (s TaskStatus) String() string {
	switch s {
	case Pending:
		return "PENDING"
	case InFlight:
		return "IN_FLIGHT"
	case Done:
		return "DONE"
	case Dead:
		return "DEAD"
	default:
		return "UNKNOWN"
	}
}

// Task is the unit of work flowing through the queue.
// Lower Priority values are dequeued first.
type Task struct {
	ID         string
	Payload    []byte
	Priority   int
	Status     TaskStatus
	RetryCount int
	CreatedAt  time.Time
	Deadline   time.Time // set when dequeued; zero while pending
}

// Clone returns a copy of the task with its own payload buffer, so callers
// can't mutate broker-owned state through a returned pointer.
func (t *Task) Clone() *Task {
	c := *t
	c.Payload = append([]byte(nil), t.Payload...)
	return &c
}
