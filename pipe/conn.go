package pipe

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	xumux "github.com/deftai/libxumux-go"
)

// ErrWriteClosed is returned by Write after CloseWrite.
var ErrWriteClosed = errors.New("pipe: write after CloseWrite")

// closeTimeout bounds how long Close waits to send EOF and close the
// channel.
const closeTimeout = 10 * time.Second

// maxResetReason caps the reason a RESET carries, so it always fits in a
// message.
const maxResetReason = 1024

// closeChannel closes the channel in the background when pipe/1 says the
// pipe is over. It lives as long as the session at most; Channel.Close
// itself bounds the wait for earlier sends by Options.CloseTimeout.
func (c *Conn) closeChannel(code int, reason string) {
	go func() { _ = c.ch.Close(c.ch.Session().Context(), code, reason) }()
}

// Conn is a pipe/1 channel as a net.Conn. It also has CloseWrite, for
// half-close, like *net.TCPConn.
//
// Read returns bytes from the peer's DATA messages, and io.EOF after the
// peer's EOF. Credit goes back to the peer only as Read hands bytes to the
// caller, so a reader that stops reading stops the writer (pipe/1
// backpressure). Write sends DATA without fragmenting it.
//
// The channel closes itself once both sides have sent EOF, or after a
// RESET. Close sends EOF, unless CloseWrite already did, then closes the
// channel without waiting for the peer's EOF, like closing a socket.
type Conn struct {
	ch *xumux.Channel

	rmu  sync.Mutex // serialises Read
	rbuf []byte
	rerr error // sticky: io.EOF, a *ResetError, a channel close
	rdl  *deadline

	wmu sync.Mutex // serialises Write and CloseWrite
	wdl *deadline

	mu      sync.Mutex
	werr    error // set when the pipe has failed: every Write returns it
	sentEOF bool
	gotEOF  bool
	closed  bool
	once    sync.Once
}

// NewConn wraps an open pipe/1 channel, such as one accepted from a
// xumux.ChannelListener. Call it before anything else receives on ch.
func NewConn(ch *xumux.Channel) *Conn {
	ch.SetManualCredit(true)
	return &Conn{ch: ch, rdl: newDeadline(), wdl: newDeadline()}
}

// Channel returns the underlying channel.
func (c *Conn) Channel() *xumux.Channel { return c.ch }

// Target returns the target named in the channel request, or "".
func (c *Conn) Target() string {
	t, _ := c.ch.Metadata()["target"].(string)
	return t
}

func (c *Conn) isClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

// Read implements io.Reader.
func (c *Conn) Read(b []byte) (int, error) {
	c.rmu.Lock()
	defer c.rmu.Unlock()
	if c.isClosed() {
		return 0, c.opErr("read", net.ErrClosed)
	}
	// A deadline that has passed fails the call even when data is waiting.
	if err := c.rdl.ctx().Err(); err != nil {
		return 0, c.opErr("read", err)
	}
	for len(c.rbuf) == 0 {
		if len(b) == 0 && c.rerr == nil {
			return 0, nil
		}
		if err := c.fillLocked(); err != nil {
			return 0, err
		}
	}
	n := copy(b, c.rbuf)
	c.rbuf = c.rbuf[n:]
	c.ch.ReturnCredit(n)
	return n, nil
}

// WriteTo implements io.WriterTo, which io.Copy uses when copying from a
// Conn. It writes the peer's bytes to w as they arrive, without an extra
// buffer, and returns credit only once w has taken them, so a slow w
// slows the peer exactly (pipe/1 requires credit to follow writes to the
// local end; Read can only return it as bytes reach the caller's buffer).
// It returns nil at EOF, as io.Copy expects, and honours the read
// deadline.
func (c *Conn) WriteTo(w io.Writer) (int64, error) {
	c.rmu.Lock()
	defer c.rmu.Unlock()
	var total int64
	for {
		if len(c.rbuf) == 0 {
			if err := c.fillLocked(); err != nil {
				if err == io.EOF {
					return total, nil
				}
				return total, err
			}
			continue
		}
		n, err := w.Write(c.rbuf)
		total += int64(n)
		c.rbuf = c.rbuf[n:]
		c.ch.ReturnCredit(n)
		if err != nil {
			return total, err
		}
	}
}

// fillLocked receives the next message, with rmu held. It fills rbuf for
// DATA (possibly with nothing, for an empty DATA), or returns the error
// that ends reading: io.EOF, a *ResetError, a closed channel, a timeout.
func (c *Conn) fillLocked() error {
	if c.isClosed() {
		return c.opErr("read", net.ErrClosed)
	}
	if c.rerr != nil {
		return c.rerr
	}
	m, err := c.ch.Receive(c.rdl.ctx())
	if err != nil {
		if c.isClosed() {
			return c.opErr("read", net.ErrClosed)
		}
		if errors.Is(err, context.DeadlineExceeded) || isTimeout(err) {
			return c.opErr("read", err) // not sticky: a new deadline can retry
		}
		c.rerr = err // io.EOF after a normal close, else a *xumux.CloseError
		return err
	}
	switch m.Type {
	case TypeData:
		c.rbuf = m.Payload
	case TypeEOF:
		c.rerr = io.EOF
		c.mu.Lock()
		c.gotEOF = true
		done := c.sentEOF
		c.mu.Unlock()
		if done {
			c.closeChannel(xumux.CodeNormal, "")
		}
		return io.EOF
	case TypeReset:
		var r resetMsg
		_ = json.Unmarshal(m.Payload, &r)
		c.rerr = &ResetError{Code: r.Code, Reason: r.Reason}
		c.fail(c.rerr, xumux.CodeGoingAway, "")
		return c.rerr
	default:
		c.rerr = fmt.Errorf("pipe: unknown pipe/1 message type %#x", m.Type)
		c.fail(c.rerr, xumux.CodeProtocolError, c.rerr.Error())
		return c.rerr
	}
	return nil
}

// Write implements io.Writer. It returns once every byte is handed to the
// transport, waiting for flow-control credit as needed.
func (c *Conn) Write(b []byte) (int, error) {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	if c.isClosed() {
		return 0, c.opErr("write", net.ErrClosed)
	}
	c.mu.Lock()
	sent := c.sentEOF
	c.mu.Unlock()
	if sent {
		return 0, c.opErr("write", ErrWriteClosed)
	}
	if werr := c.writeErr(); werr != nil {
		return 0, werr
	}
	// A deadline that has passed fails the call even when credit is free.
	if err := c.wdl.ctx().Err(); err != nil {
		return 0, c.opErr("write", err)
	}
	n := 0
	for n < len(b) {
		k, err := c.ch.SendPrefix(c.wdl.ctx(), TypeData, b[n:min(len(b), n+maxChunk)])
		n += k
		if err != nil {
			if werr := c.writeErr(); werr != nil {
				return n, werr
			}
			if c.isClosed() {
				err = net.ErrClosed
			}
			return n, c.opErr("write", err)
		}
	}
	return n, nil
}

// CloseWrite sends EOF: no more data will be written, but reading goes on
// (like shutdown(SHUT_WR)).
func (c *Conn) CloseWrite() error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	if c.isClosed() {
		return c.opErr("close", net.ErrClosed)
	}
	return c.sendEOFLocked(c.wdl.ctx())
}

// sendEOFLocked sends EOF once, with wmu held, and closes the channel if
// the peer's EOF has already arrived.
func (c *Conn) sendEOFLocked(ctx context.Context) error {
	c.mu.Lock()
	if c.sentEOF {
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()
	_, err := c.ch.SendPrefix(ctx, TypeEOF, nil)
	if err != nil && !errors.Is(err, xumux.ErrQueued) {
		return c.opErr("close", err)
	}
	// Sent, or queued and certain to be sent: no DATA may follow either way.
	c.mu.Lock()
	c.sentEOF = true
	done := c.gotEOF
	c.mu.Unlock()
	if done {
		c.closeChannel(xumux.CodeNormal, "")
	}
	return c.opErr("close", err)
}

func (c *Conn) writeErr() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.werr
}

// fail ends the pipe after a RESET or a protocol error: pending and future
// Writes return err, which also frees the channel's send slot so the
// close can go out at once.
func (c *Conn) fail(err error, code int, reason string) {
	c.mu.Lock()
	if c.werr == nil {
		c.werr = err
	}
	c.mu.Unlock()
	c.wdl.expire()
	c.closeChannel(code, reason)
}

// Reset reports a failure of this end to the peer (a RESET message), as a
// relay does when its local connection breaks, and closes the channel.
func (c *Conn) Reset(code int, reason string) error {
	if !c.shut() { // also ends a pending Write
		return c.opErr("reset", net.ErrClosed)
	}
	if len(reason) > maxResetReason {
		reason = reason[:maxResetReason]
	}
	msg, _ := json.Marshal(resetMsg{Code: code, Reason: reason})
	ctx, cancel := context.WithTimeout(context.Background(), closeTimeout)
	defer cancel()
	c.wmu.Lock()
	// RESET is one JSON message, so it goes whole (fragmented if need be,
	// which the profile allows for everything but DATA).
	err := c.ch.Send(ctx, TypeReset, msg)
	c.wmu.Unlock()
	if cerr := c.ch.Close(ctx, xumux.CodeGoingAway, reason); err == nil {
		err = cerr
	}
	return err
}

// shut marks the Conn closed and ends pending and future Reads and Writes.
func (c *Conn) shut() bool {
	first := false
	c.once.Do(func() {
		first = true
		c.mu.Lock()
		c.closed = true
		c.mu.Unlock()
		c.rdl.expire()
		c.wdl.expire()
	})
	return first
}

// Close implements io.Closer: it unblocks pending Reads and Writes, sends
// EOF if CloseWrite has not, and closes the channel. Bytes the peer sends
// afterwards are discarded.
func (c *Conn) Close() error {
	if !c.shut() {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), closeTimeout)
	defer cancel()
	c.wmu.Lock() // a pending Write has returned: its deadline expired
	_ = c.sendEOFLocked(ctx)
	c.wmu.Unlock()
	return c.ch.Close(ctx, xumux.CodeNormal, "")
}

// LocalAddr and RemoteAddr identify the channel; they are not network
// addresses.
func (c *Conn) LocalAddr() net.Addr { return Addr{ID: c.ch.ID(), Name: c.ch.Name()} }
func (c *Conn) RemoteAddr() net.Addr {
	return Addr{ID: c.ch.ID(), Name: c.ch.Name(), Target: c.Target()}
}

// SetDeadline sets both deadlines.
func (c *Conn) SetDeadline(t time.Time) error {
	c.rdl.set(t)
	c.wdl.set(t)
	return nil
}

// SetReadDeadline sets the deadline for pending and future Reads.
func (c *Conn) SetReadDeadline(t time.Time) error { c.rdl.set(t); return nil }

// SetWriteDeadline sets the deadline for pending and future Writes.
func (c *Conn) SetWriteDeadline(t time.Time) error { c.wdl.set(t); return nil }

func (c *Conn) opErr(op string, err error) error {
	if err == nil || err == io.EOF {
		return err
	}
	return &net.OpError{Op: op, Net: "xumux", Source: c.LocalAddr(), Addr: c.RemoteAddr(), Err: err}
}

func isTimeout(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

// Addr is a pipe's address: its channel ID, name and requested target.
type Addr struct {
	ID     uint16
	Name   string
	Target string
}

// Network returns "xumux".
func (Addr) Network() string { return "xumux" }

func (a Addr) String() string {
	s := fmt.Sprintf("channel %d", a.ID)
	if a.Name != "" {
		s += " (" + a.Name + ")"
	}
	if a.Target != "" {
		s += " -> " + a.Target
	}
	return s
}
