package xumux

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"
)

type chanState int

const (
	chanOpening chanState = iota
	chanOpen
	chanClosing
	chanClosed
)

// Message is one application message: the frame Type and the reassembled payload.
type Message struct {
	Type    byte
	Payload []byte
}

type reassembly struct {
	typ   byte
	parts [][]byte
	size  int64
}

// Channel is one logical channel of a Session.
//
// Send and Receive may be used from different goroutines. Messages are
// sent one at a time per channel, in call order.
type Channel struct {
	s           *Session
	id          uint16
	name        string
	metadata    map[string]any
	localOpener bool
	opened      chan struct{} // closed on ACK, REJECT or close (local opener only)
	done        chan struct{}
	sendSem     chan struct{}

	// Guarded by s.mu.
	state                              chanState
	props                              Properties
	sentClose, recvClose               bool
	abandon                            bool
	closeInfo                          *CloseError
	openErr                            error
	sendCredit, recvCredit, unreturned int64
	creditSig                          chan struct{}
	inbox                              []Message
	inboxSig                           chan struct{}
	manualCredit                       bool
	reasm                              *reassembly
	held                               []Frame
	heldBytes                          int64
	heldPings                          int
	pings                              map[uint32]*pendingPing
	stats                              statsRecorder
	// Receive-window auto-tuning (see Options.MaxChannelWindow).
	recvWindow int64     // current window: credit the peer may hold
	tuneStart  time.Time // start of the current measurement
	tuneBytes  int64     // bytes returned since tuneStart
}

func (spec ChannelSpec) request(id uint16) ChannelRequest {
	return ChannelRequest{
		ID: id, Name: spec.Name, Reliable: spec.Reliable, Ordered: spec.Ordered,
		MaxRetransmits: spec.MaxRetransmits, MaxPacketLifeTime: spec.MaxPacketLifeTime, Metadata: spec.Metadata,
	}
}

func (s *Session) newChannel(r ChannelRequest, localOpener bool, state chanState) *Channel {
	c := &Channel{
		s: s, id: r.ID, name: r.Name, metadata: r.Metadata, localOpener: localOpener,
		opened: make(chan struct{}), done: make(chan struct{}), sendSem: make(chan struct{}, 1),
		state: state, props: Properties{Reliable: r.Reliable, Ordered: r.Ordered, MaxRetransmits: r.MaxRetransmits, MaxPacketLifeTime: r.MaxPacketLifeTime},
		recvCredit: int64(s.local.InitialWindow), creditSig: make(chan struct{}), inboxSig: make(chan struct{}),
		pings: map[uint32]*pendingPing{}, recvWindow: int64(s.local.InitialWindow),
		// The first measurement starts when the initial window is granted.
		tuneStart: time.Now(),
	}
	s.windowTotal += c.recvWindow
	s.chans[r.ID] = c
	return c
}

// ID is the channel ID.
func (c *Channel) ID() uint16 { return c.id }

// Name is the channel's name.
func (c *Channel) Name() string { return c.name }

// Metadata is the metadata from the channel request.
func (c *Channel) Metadata() map[string]any { return c.metadata }

// Session is the session this channel belongs to.
func (c *Channel) Session() *Session { return c.s }

// Properties are the effective properties (core §5.3).
func (c *Channel) Properties() Properties {
	c.s.mu.Lock()
	defer c.s.mu.Unlock()
	return c.props
}

// Stats returns this channel's probe statistics.
func (c *Channel) Stats() Stats {
	c.s.mu.Lock()
	defer c.s.mu.Unlock()
	return c.stats.Stats
}

// ReceiveWindow is the channel's current receive window: the most this side
// lets the peer send ahead of what it has consumed. It starts at the
// initial window and grows with auto-tuning.
func (c *Channel) ReceiveWindow() int64 {
	c.s.mu.Lock()
	defer c.s.mu.Unlock()
	return c.recvWindow
}

// Done is closed once the channel is fully closed.
func (c *Channel) Done() <-chan struct{} { return c.done }

// CloseInfo reports how the channel closed (valid once Done is closed).
func (c *Channel) CloseInfo() CloseError {
	c.s.mu.Lock()
	defer c.s.mu.Unlock()
	if c.closeInfo == nil {
		return CloseError{}
	}
	return *c.closeInfo
}

// Ping measures the round-trip time on this channel.
func (c *Channel) Ping(ctx context.Context) (time.Duration, error) { return c.s.ping(ctx, c) }

// SetManualCredit makes Receive stop returning flow-control credit; the
// caller returns it with ReturnCredit once it has really used the bytes
// (pipe/1 does this after writing them to its local end). Call it before
// the first Receive.
func (c *Channel) SetManualCredit(on bool) {
	c.s.mu.Lock()
	defer c.s.mu.Unlock()
	c.manualCredit = on
}

// ReturnCredit returns credit for n consumed bytes (with SetManualCredit).
func (c *Channel) ReturnCredit(n int) {
	c.s.mu.Lock()
	defer c.s.mu.Unlock()
	c.consumedLocked(int64(n))
}

// Send sends one message. It returns once every frame has been handed to
// the transport, waiting for flow-control credit as needed. If ctx ends
// after part of a fragmented message was sent, the channel is closed
// with 1001, since the rest of the message can no longer be sent.
func (c *Channel) Send(ctx context.Context, typ byte, payload []byte) error {
	select {
	case c.sendSem <- struct{}{}:
		defer func() { <-c.sendSem }()
	case <-c.done:
		return errChannelClosed(c.id)
	case <-ctx.Done():
		return ctx.Err()
	}
	s := c.s
	s.mu.Lock()
	if c.state != chanOpen || c.sentClose || c.recvClose {
		s.mu.Unlock()
		return errChannelClosed(c.id)
	}
	reliable, fragmentable := c.props.Reliable, c.props.Reliable && c.props.Ordered
	remote := s.remote
	s.mu.Unlock()

	if uint64(len(payload)) > uint64(remote.MaxMessageSize) {
		return fmt.Errorf("xumux: message of %d bytes exceeds the peer's maxMessageSize %d", len(payload), remote.MaxMessageSize)
	}
	chunk := min(s.opts.FragmentSize, int(remote.MaxFramePayload))
	if !fragmentable {
		if uint64(len(payload)) > uint64(remote.MaxFramePayload) {
			return fmt.Errorf("xumux: message of %d bytes exceeds the peer's maxFramePayload on an unfragmentable channel", len(payload))
		}
		chunk = max(len(payload), 1)
	}

	for off := 0; ; {
		n := min(chunk, len(payload)-off)
		if reliable && n > 0 {
			got, err := c.takeCredit(ctx, n, fragmentable)
			if err != nil {
				if off > 0 {
					s.mu.Lock()
					s.sendCloseChannelLocked(c, CodeGoingAway, "send abandoned mid-message")
					s.mu.Unlock()
				}
				return err
			}
			n = got
		}
		end := off + n
		f := Frame{Channel: c.id, Type: typ, Payload: payload[off:end]}
		if end < len(payload) {
			f.Flags = FlagMore
		}
		s.mu.Lock()
		var it *outItem
		var err error
		if c.sentClose || c.recvClose || c.state != chanOpen {
			err = errChannelClosed(c.id)
		} else {
			it, err = s.enqueueLocked(f, nil, true)
		}
		s.mu.Unlock()
		if err != nil {
			return err
		}
		if err := <-it.done; err != nil {
			return err
		}
		off = end
		if off >= len(payload) {
			return nil
		}
	}
}

// SendPrefix sends as much of payload as fits in one unfragmented frame
// and in the send credit available, waiting until there is at least one
// byte of credit, and reports how many bytes it sent. It suits byte
// streams such as pipe/1 DATA, whose messages must not be fragmented:
// call it in a loop until all of payload has gone. An empty payload sends
// an empty message. Unlike Send, a ctx that ends never leaves part of a
// message sent: n bytes went as a whole message, or none.
//
// If ctx ends while the frame waits for a slow transport, SendPrefix
// returns n with an error that matches both ErrQueued and ctx's error: the
// frame is queued and will still be sent, from a copy, so payload can be
// reused at once. Later sends and Close on the channel wait for it, so
// nothing overtakes it.
func (c *Channel) SendPrefix(ctx context.Context, typ byte, payload []byte) (int, error) {
	select {
	case c.sendSem <- struct{}{}:
	case <-c.done:
		return 0, errChannelClosed(c.id)
	case <-ctx.Done():
		return 0, ctx.Err()
	}
	s := c.s
	s.mu.Lock()
	if c.state != chanOpen || c.sentClose || c.recvClose {
		s.mu.Unlock()
		return 0, errChannelClosed(c.id)
	}
	reliable := c.props.Reliable
	n := min(len(payload), s.opts.FragmentSize, int(s.remote.MaxFramePayload))
	s.mu.Unlock()
	release := func() { <-c.sendSem }
	if reliable && n > 0 {
		got, err := c.takeCredit(ctx, n, true)
		if err != nil {
			release()
			return 0, err
		}
		n = got
	}
	frame := Frame{Channel: c.id, Type: typ, Payload: append([]byte(nil), payload[:n]...)}
	s.mu.Lock()
	var it *outItem
	var err error
	if c.sentClose || c.recvClose || c.state != chanOpen {
		err = errChannelClosed(c.id)
	} else {
		it, err = s.enqueueLocked(frame, nil, true)
	}
	s.mu.Unlock()
	if err != nil {
		release()
		return 0, err
	}
	select {
	case err := <-it.done:
		release()
		if err != nil {
			return 0, err
		}
		return n, nil
	case <-ctx.Done():
		// Keep the send slot until the frame is written, so that a later
		// frame (or CLOSE_CHANNEL) cannot overtake it.
		go func() { <-it.done; release() }()
		return n, queuedError{ctx.Err()}
	}
}

// ErrQueued is matched by the error SendPrefix returns when ctx ended after
// its frame was queued: the frame will still be sent.
var ErrQueued = errors.New("xumux: frame queued; it will still be sent")

// queuedError is ctx's error after a frame was queued. It matches both
// ErrQueued and the ctx error, and keeps a timeout a net.Error timeout.
type queuedError struct{ err error }

func (e queuedError) Error() string   { return e.err.Error() + " (frame queued; it will still be sent)" }
func (e queuedError) Unwrap() []error { return []error{ErrQueued, e.err} }
func (e queuedError) Timeout() bool {
	t, ok := e.err.(interface{ Timeout() bool })
	return ok && t.Timeout()
}
func (e queuedError) Temporary() bool { return e.Timeout() }

// takeCredit waits for n bytes of send credit. With partial, it takes
// whatever is available (at least 1 byte, at most n).
func (c *Channel) takeCredit(ctx context.Context, n int, partial bool) (int, error) {
	s := c.s
	for {
		s.mu.Lock()
		if c.sentClose || c.recvClose || c.state != chanOpen || s.state != stateOpen {
			s.mu.Unlock()
			return 0, errChannelClosed(c.id)
		}
		avail := c.sendCredit
		if avail >= int64(n) || (partial && avail > 0) {
			got := min(int64(n), avail)
			c.sendCredit -= got
			s.mu.Unlock()
			return int(got), nil
		}
		sig := c.creditSig
		s.mu.Unlock()
		select {
		case <-sig:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
}

// Receive returns the next message. After a normal close it returns io.EOF
// once queued messages are read; after any other close, a *CloseError.
func (c *Channel) Receive(ctx context.Context) (Message, error) {
	s := c.s
	for {
		s.mu.Lock()
		if len(c.inbox) > 0 {
			m := c.inbox[0]
			c.inbox[0] = Message{}
			c.inbox = c.inbox[1:]
			if !c.manualCredit {
				c.consumedLocked(int64(len(m.Payload)))
			}
			s.mu.Unlock()
			return m, nil
		}
		if c.state == chanClosed {
			info := c.closeInfo
			s.mu.Unlock()
			if info == nil || info.Code == CodeNormal {
				return Message{}, io.EOF
			}
			return Message{}, info
		}
		sig := c.inboxSig
		s.mu.Unlock()
		select {
		case <-sig:
		case <-ctx.Done():
			return Message{}, ctx.Err()
		}
	}
}

// Close closes the channel (two-step, core §5.4) and waits until it is
// fully closed. In-progress sends get up to CloseTimeout to finish first.
func (c *Channel) Close(ctx context.Context, code int, reason string) error {
	s := c.s
	flush := time.NewTimer(s.opts.CloseTimeout)
	defer flush.Stop()
	select {
	case c.sendSem <- struct{}{}:
		defer func() { <-c.sendSem }()
	case <-flush.C:
	case <-c.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
	s.mu.Lock()
	if c.state == chanOpening {
		s.mu.Unlock()
		return errors.New("xumux: cannot close a channel that is still opening")
	}
	s.sendCloseChannelLocked(c, code, reason)
	s.mu.Unlock()
	select {
	case <-c.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Channel) deliverLocked(m Message) {
	c.inbox = append(c.inbox, m)
	c.signalInboxLocked()
}

// consumedLocked records consumed bytes; the writer returns them in the
// next WINDOW_UPDATE, batching whatever accumulates meanwhile (core §6).
func (c *Channel) consumedLocked(n int64) {
	if n <= 0 || !c.props.Reliable || c.state != chanOpen || c.sentClose {
		return
	}
	c.unreturned += n
	c.s.windowDirty = true
	c.s.signalWriter()
}

func (c *Channel) signalCreditLocked() {
	close(c.creditSig)
	c.creditSig = make(chan struct{})
}

func (c *Channel) signalInboxLocked() {
	close(c.inboxSig)
	c.inboxSig = make(chan struct{})
}
