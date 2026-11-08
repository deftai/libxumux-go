package pipe

import (
	"context"
	"os"
	"sync"
	"time"
)

// deadline is one direction's read or write deadline. Its channel closes
// when the deadline passes. Setting a new deadline keeps the channel open
// (or reopens it) so that calls already waiting see the new deadline, as
// net.Conn requires.
type deadline struct {
	mu     sync.Mutex
	timer  *time.Timer
	cancel chan struct{}
	final  bool // the Conn is closed: the channel stays closed
}

func newDeadline() *deadline { return &deadline{cancel: make(chan struct{})} }

func (d *deadline) set(t time.Time) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.final {
		return
	}
	if d.timer != nil && !d.timer.Stop() {
		<-d.cancel // the timer fired; wait for it to close cancel
	}
	d.timer = nil
	closed := isClosed(d.cancel)
	if t.IsZero() {
		if closed {
			d.cancel = make(chan struct{})
		}
		return
	}
	if dur := time.Until(t); dur > 0 {
		if closed {
			d.cancel = make(chan struct{})
		}
		cancel := d.cancel
		d.timer = time.AfterFunc(dur, func() { close(cancel) })
		return
	}
	if !closed {
		close(d.cancel)
	}
}

// expire closes the channel for good (Conn.Close).
func (d *deadline) expire() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.timer != nil && !d.timer.Stop() {
		<-d.cancel
	}
	d.timer = nil
	if !isClosed(d.cancel) {
		close(d.cancel)
	}
	d.final = true
}

// ctx is a Context that ends at the deadline.
func (d *deadline) ctx() context.Context {
	d.mu.Lock()
	defer d.mu.Unlock()
	return deadlineCtx(d.cancel)
}

func isClosed(c chan struct{}) bool {
	select {
	case <-c:
		return true
	default:
		return false
	}
}

// deadlineCtx ends when its channel closes, with os.ErrDeadlineExceeded,
// which is a net.Error whose Timeout() is true.
type deadlineCtx chan struct{}

func (deadlineCtx) Deadline() (time.Time, bool) { return time.Time{}, false }
func (c deadlineCtx) Done() <-chan struct{}     { return c }
func (deadlineCtx) Value(any) any               { return nil }
func (c deadlineCtx) Err() error {
	if isClosed(c) {
		return os.ErrDeadlineExceeded
	}
	return nil
}
