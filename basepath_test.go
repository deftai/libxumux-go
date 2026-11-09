package xumux

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Tests of the base-path seam (core §11.1): detaching a session from a lost
// transport and attaching a new one under it, through pathHooks.

// keepHooks keeps the session alive on transport loss when keep is set.
type keepHooks struct {
	keep   bool
	retain bool
	lost   chan error
}

func newKeepHooks(keep bool) *keepHooks { return &keepHooks{keep: keep, lost: make(chan error, 16)} }

func (h *keepHooks) pathLost(err error) bool {
	select {
	case h.lost <- err:
	default:
	}
	return h.keep
}

func (h *keepHooks) writing(*outItem) bool { return h.retain }

func setHooks(s *Session, h pathHooks) {
	s.mu.Lock()
	s.hooks = h
	s.mu.Unlock()
}

func isDetached(s *Session) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.path == nil && s.state == stateOpen
}

func waitDetached(t *testing.T, s *Session) {
	t.Helper()
	require.Eventually(t, func() bool { return isDetached(s) }, 3*time.Second, time.Millisecond)
}

func attach(s *Session, conn FrameConn) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.attachLocked(conn, nil)
}

// seamPair connects a client and server over net.Pipe and returns the raw
// client end, so a test can cut the transport.
func seamPair(t *testing.T, specs ...ChannelSpec) (c, s *Session, res *ConnectResult, raw net.Conn, inbound chan *Channel) {
	t.Helper()
	a, b := net.Pipe()
	inbound = make(chan *Channel, 8)
	sopts := acceptAll(func(ch *Channel) { inbound <- ch })
	type acc struct {
		s   *Session
		err error
	}
	got := make(chan acc, 1)
	go func() {
		s, _, err := Accept(context.Background(), NewStreamConn(b), ServerConfig{}, sopts)
		got <- acc{s, err}
	}()
	c, res, err := Connect(context.Background(), NewStreamConn(a), ClientConfig{Channels: specs}, testOpts)
	require.NoError(t, err)
	sv := <-got
	require.NoError(t, sv.err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = c.Close(ctx, CodeNormal, "")
		_ = sv.s.Close(ctx, CodeNormal, "")
	})
	return c, sv.s, res, a, inbound
}

// reattach gives both sessions a new transport.
func reattach(t *testing.T, c, s *Session) {
	t.Helper()
	a, b := net.Pipe()
	require.NoError(t, attach(s, NewStreamConn(b)))
	require.NoError(t, attach(c, NewStreamConn(a)))
}

func receiveString(t *testing.T, ch *Channel) string {
	t.Helper()
	m, err := ch.Receive(ctx(t))
	require.NoError(t, err)
	return string(m.Payload)
}

func TestSeamDetachAndAttach(t *testing.T) {
	c, s, res, raw, inbound := seamPair(t, ChannelSpec{Name: "a", Reliable: true, Ordered: true})
	setHooks(c, newKeepHooks(true))
	setHooks(s, newKeepHooks(true))
	cch := res.Channels[0]
	sch := <-inbound
	require.NoError(t, cch.Send(ctx(t), 1, []byte("before")))
	assert.Equal(t, "before", receiveString(t, sch))

	raw.Close()
	waitDetached(t, c)
	waitDetached(t, s)
	select {
	case <-c.Done():
		t.Fatal("client session ended on transport loss")
	case <-s.Done():
		t.Fatal("server session ended on transport loss")
	default:
	}

	// Sends while detached wait for the next path.
	sent := make(chan error, 1)
	go func() { sent <- cch.Send(context.Background(), 1, []byte("while detached")) }()
	select {
	case err := <-sent:
		t.Fatalf("send completed while detached: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	reattach(t, c, s)
	require.NoError(t, <-sent)
	assert.Equal(t, "while detached", receiveString(t, sch))
	require.NoError(t, sch.Send(ctx(t), 1, []byte("back")))
	assert.Equal(t, "back", receiveString(t, cch))
	_, err := c.Ping(ctx(t))
	require.NoError(t, err)
}

func TestSeamWithoutKeepEndsSession(t *testing.T) {
	c, s, _, raw, _ := seamPair(t)
	h := newKeepHooks(false)
	setHooks(c, h)
	raw.Close()
	for _, x := range []*Session{c, s} {
		select {
		case <-x.Done():
		case <-time.After(3 * time.Second):
			t.Fatal("session did not end")
		}
		assert.True(t, x.CloseInfo().Lost)
	}
	select {
	case <-h.lost:
	default:
		t.Fatal("pathLost was not consulted")
	}
}

// cutConn fails every write once cut.
type cutConn struct {
	FrameConn
	cut atomic.Bool
}

var errCut = errors.New("transport cut")

func (c *cutConn) WriteFrame(f Frame) error {
	if c.cut.Load() {
		c.FrameConn.Close()
		return errCut
	}
	return c.FrameConn.WriteFrame(f)
}

func TestSeamFailedWriteIsRequeued(t *testing.T) {
	for _, retain := range []bool{false, true} {
		name := "requeued"
		if retain {
			name = "retained"
		}
		t.Run(name, func(t *testing.T) {
			a, b := net.Pipe()
			inbound := make(chan *Channel, 1)
			type acc struct {
				s   *Session
				err error
			}
			got := make(chan acc, 1)
			go func() {
				s, _, err := Accept(context.Background(), NewStreamConn(b), ServerConfig{}, acceptAll(func(ch *Channel) { inbound <- ch }))
				got <- acc{s, err}
			}()
			cc := &cutConn{FrameConn: NewStreamConn(a)}
			c, res, err := Connect(context.Background(), cc, ClientConfig{Channels: []ChannelSpec{{Name: "a", Reliable: true, Ordered: true}}}, testOpts)
			require.NoError(t, err)
			sv := <-got
			require.NoError(t, sv.err)
			s := sv.s
			defer c.Close(context.Background(), CodeNormal, "")
			defer s.Close(context.Background(), CodeNormal, "")
			h := newKeepHooks(true)
			h.retain = retain
			setHooks(c, h)
			setHooks(s, newKeepHooks(true))
			sch := <-inbound

			cc.cut.Store(true)
			first := make(chan error, 1)
			go func() { first <- res.Channels[0].Send(ctx(t), 1, []byte("first")) }()
			waitDetached(t, c)
			waitDetached(t, s)
			reattach(t, c, s)
			// A requeued frame completes once the next path writes it; a
			// retained one counts as sent at once.
			require.NoError(t, <-first)
			require.NoError(t, res.Channels[0].Send(ctx(t), 1, []byte("second")))
			if !retain {
				// The frame whose write failed goes out again, first.
				assert.Equal(t, "first", receiveString(t, sch))
			}
			// A retained frame is the extension's to send again: the core
			// does not requeue it.
			assert.Equal(t, "second", receiveString(t, sch))
		})
	}
}

func TestSeamKeepaliveTimeoutDetaches(t *testing.T) {
	lib, peer := rawPair(t, "stream")
	go peer.awaitHelloSendWelcome(map[string]any{"pingInterval": 0.05, "pingTimeout": 0.05})
	c, _, err := Connect(ctx(t), lib, ClientConfig{}, Options{CloseTimeout: 100 * time.Millisecond})
	require.NoError(t, err)
	defer c.Close(context.Background(), CodeNormal, "")
	h := newKeepHooks(true)
	setHooks(c, h)
	// The peer answers nothing: keepalive times out and detaches the session.
	select {
	case err := <-h.lost:
		assert.ErrorIs(t, err, errKeepaliveTimeout)
	case <-time.After(3 * time.Second):
		t.Fatal("keepalive did not time out")
	}
	waitDetached(t, c)
	peer.waitEnded()
	for {
		select {
		case f := <-peer.frames:
			assert.False(t, f.IsControl() && f.Type == TypeClose, "CLOSE sent on keepalive timeout of a kept session")
			continue
		default:
		}
		break
	}
}

func TestSeamTakeover(t *testing.T) {
	c, s, res, raw, inbound := seamPair(t, ChannelSpec{Name: "a", Reliable: true, Ordered: true})
	setHooks(c, newKeepHooks(true))
	setHooks(s, newKeepHooks(true))
	sch := <-inbound
	// The server attaches a new transport while the old one still works:
	// the old one is closed without CLOSE, so the client sees it fail.
	a, b := net.Pipe()
	require.NoError(t, attach(s, NewStreamConn(b)))
	waitDetached(t, c)
	_ = raw
	require.NoError(t, attach(c, NewStreamConn(a)))
	require.NoError(t, res.Channels[0].Send(ctx(t), 1, []byte("after takeover")))
	assert.Equal(t, "after takeover", receiveString(t, sch))
}

func TestSeamCloseWhileDetached(t *testing.T) {
	c, _, _, raw, _ := seamPair(t)
	setHooks(c, newKeepHooks(true))
	raw.Close()
	waitDetached(t, c)
	start := time.Now()
	require.NoError(t, c.Close(ctx(t), 4600, "bye"))
	assert.Less(t, time.Since(start), time.Second)
	assert.Equal(t, 4600, c.CloseInfo().Code)
	assert.ErrorIs(t, attach(c, NewStreamConn(&nopRWC{})), ErrClosed)
}

func TestSeamPingAbandonedOnDetach(t *testing.T) {
	lib, peer := rawPair(t, "stream")
	go peer.awaitHelloSendWelcome(nil)
	c, _, err := Connect(ctx(t), lib, ClientConfig{}, testOpts)
	require.NoError(t, err)
	defer c.Close(context.Background(), CodeNormal, "")
	setHooks(c, newKeepHooks(true))
	pinged := make(chan error, 1)
	go func() {
		_, err := c.Ping(context.Background())
		pinged <- err
	}()
	peer.next(func(f Frame) bool { return f.IsProbe() && f.Type == TypePing })
	peer.fc.Close()
	select {
	case err := <-pinged:
		assert.ErrorIs(t, err, errPathLost)
	case <-time.After(3 * time.Second):
		t.Fatal("Ping did not return")
	}
	assert.Zero(t, c.Stats().ProbesLost)
}

type nopRWC struct{}

func (*nopRWC) Read([]byte) (int, error)    { return 0, net.ErrClosed }
func (*nopRWC) Write(b []byte) (int, error) { return len(b), nil }
func (*nopRWC) Close() error                { return nil }

// gateConn holds a write, once armed, until release, then fails it.
type gateConn struct {
	FrameConn
	armed   atomic.Bool
	entered chan struct{}
	release chan struct{}
}

func newGateConn(fc FrameConn) *gateConn {
	return &gateConn{FrameConn: fc, entered: make(chan struct{}), release: make(chan struct{})}
}

func (g *gateConn) WriteFrame(f Frame) error {
	if g.armed.CompareAndSwap(true, false) {
		close(g.entered)
		<-g.release
		return errCut
	}
	return g.FrameConn.WriteFrame(f)
}

// gatePair is seamPair with the client's transport wrapped in a gateConn.
func gatePair(t *testing.T) (c, s *Session, cch, sch *Channel, g *gateConn) {
	t.Helper()
	a, b := net.Pipe()
	inbound := make(chan *Channel, 1)
	got := make(chan *Session, 1)
	go func() {
		s, _, err := Accept(context.Background(), NewStreamConn(b), ServerConfig{}, acceptAll(func(ch *Channel) { inbound <- ch }))
		assert.NoError(t, err)
		got <- s
	}()
	g = newGateConn(NewStreamConn(a))
	c, res, err := Connect(context.Background(), g, ClientConfig{Channels: []ChannelSpec{{Name: "a", Reliable: true, Ordered: true}}}, testOpts)
	require.NoError(t, err)
	s = <-got
	t.Cleanup(func() {
		c.Close(context.Background(), CodeNormal, "")
		s.Close(context.Background(), CodeNormal, "")
	})
	setHooks(c, newKeepHooks(true))
	setHooks(s, newKeepHooks(true))
	return c, s, res.Channels[0], <-inbound, g
}

// TestSeamReplacedDuringWrite: the path is replaced while its writer is
// inside WriteFrame; the frame that then fails goes out on the new path.
func TestSeamReplacedDuringWrite(t *testing.T) {
	c, s, cch, sch, g := gatePair(t)
	g.armed.Store(true)
	sent := make(chan error, 1)
	go func() { sent <- cch.Send(context.Background(), 1, []byte("x")) }()
	<-g.entered
	a, b := net.Pipe()
	require.NoError(t, attach(s, NewStreamConn(b)))
	require.NoError(t, attach(c, NewStreamConn(a)))
	time.Sleep(20 * time.Millisecond) // let the new writer go idle
	close(g.release)
	require.NoError(t, <-sent)
	assert.Equal(t, "x", receiveString(t, sch))
}

// TestSeamNoRequeueAfterChannelClose: a frame whose write fails after its
// channel closed is not sent again.
func TestSeamNoRequeueAfterChannelClose(t *testing.T) {
	c, s, cch, sch, g := gatePair(t)
	g.armed.Store(true)
	sent := make(chan error, 1)
	go func() { sent <- cch.Send(context.Background(), 1, []byte("stale")) }()
	<-g.entered
	closed := make(chan error, 1)
	go func() { closed <- sch.Close(context.Background(), CodeNormal, "") }()
	<-cch.Done() // the client got CLOSE_CHANNEL and replied (queued)
	close(g.release)
	assert.ErrorIs(t, <-sent, ErrClosed)
	waitDetached(t, c)
	waitDetached(t, s)
	reattach(t, c, s)
	require.NoError(t, <-closed)
	_, err := c.Ping(ctx(t))
	require.NoError(t, err)
}

func TestSeamCloseWithBlockedSenders(t *testing.T) {
	c, _, res, raw, _ := seamPair(t, ChannelSpec{Name: "a", Reliable: true, Ordered: true}, ChannelSpec{Name: "b", Reliable: true, Ordered: true})
	setHooks(c, newKeepHooks(true))
	raw.Close()
	waitDetached(t, c)
	for _, ch := range res.Channels {
		go ch.Send(context.Background(), 1, []byte("blocked"))
	}
	time.Sleep(20 * time.Millisecond)
	start := time.Now()
	require.NoError(t, c.Close(context.Background(), CodeNormal, ""))
	assert.Less(t, time.Since(start), 200*time.Millisecond)
}
