package xumux

import (
	"bytes"
	"context"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// heldWriter passes writes through until held, then blocks them until
// released, as a transport does when the peer stops reading.
type heldWriter struct {
	held    atomic.Bool
	release chan struct{}
	buf     bytes.Buffer
}

func (w *heldWriter) Write(b []byte) (int, error) {
	if w.held.Load() {
		<-w.release
		return 0, io.ErrClosedPipe
	}
	return w.buf.Write(b)
}

// serverWithHeldWriter returns a server session past its handshake whose
// writer is stuck, so queued frames are never drained.
func serverWithHeldWriter(t *testing.T, opts Options) *Session {
	t.Helper()
	pr, pw := io.Pipe()
	w := &heldWriter{release: make(chan struct{})}
	t.Cleanup(func() { close(w.release); pw.Close() })
	conn := NewStreamConn(struct {
		io.Reader
		io.Writer
		io.Closer
	}{pr, w, pr})
	hello := controlFrame(TypeHello, Hello{Version: Version, Limits: Limits{MaxFramePayload: DefaultMaxFramePayload, MaxMessageSize: DefaultMaxMessageSize, InitialWindow: DefaultInitialWindow}})
	go pw.Write(append([]byte("XMUX"), hello.Encode()...))
	opts.PingInterval = -1
	s, _, err := Accept(context.Background(), conn, ServerConfig{}, opts)
	require.NoError(t, err)
	w.held.Store(true)
	return s
}

func queueErrors(s *Session, n int) (queued int, bytesQueued int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for ; queued < n; queued++ {
		if _, err := s.enqueueLocked(controlFrame(TypeError, ErrorMessage{Code: CodeUnsupported, Reason: "unsupported"}), nil, false); err != nil {
			break
		}
	}
	return queued, s.urgentBytes
}

func TestControlQueueIsCapped(t *testing.T) {
	const limit = 64 << 10
	s := serverWithHeldWriter(t, Options{MaxControlQueue: limit, CloseTimeout: 100 * time.Millisecond})
	queued, inQueue := queueErrors(s, 1_000_000)
	assert.Less(t, queued, 1_000_000, "enqueueing stopped at the cap")
	assert.LessOrEqual(t, inQueue, int64(limit))
	select {
	case <-s.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("session did not close after the control queue filled")
	}
	assert.Equal(t, CodeRateLimited, s.CloseInfo().Code)
	assert.Contains(t, s.CloseInfo().Reason, "not reading")
}

func TestControlQueueCapDefaultsAndCanBeDisabled(t *testing.T) {
	assert.EqualValues(t, DefaultMaxControlQueue, Options{}.withDefaults().MaxControlQueue)
	s := serverWithHeldWriter(t, Options{MaxControlQueue: -1})
	queued, inQueue := queueErrors(s, 100_000)
	assert.Equal(t, 100_000, queued, "no cap")
	assert.Greater(t, inQueue, int64(DefaultMaxControlQueue))
}

// The default cap leaves room for the largest legitimate burst: a peer
// opening every channel it may at once.
func TestControlQueueDefaultFitsAFullChannelBurst(t *testing.T) {
	sopts := acceptAll(func(c *Channel) { _, _ = c.Receive(context.Background()) })
	sopts.PingInterval = -1
	client, server := tcpPair(t, Options{PingInterval: -1}, sopts)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	errs := make(chan error, DefaultMaxChannels)
	for i := 0; i < DefaultMaxChannels; i++ {
		go func() {
			_, err := client.OpenChannel(ctx, ChannelSpec{Name: "x", Reliable: true, Ordered: true})
			errs <- err
		}()
	}
	for i := 0; i < DefaultMaxChannels; i++ {
		require.NoError(t, <-errs)
	}
	select {
	case <-server.Done():
		t.Fatalf("server closed: %v", server.CloseInfo())
	default:
	}
}

// Sending a frame releases its bytes: the cap bounds what is queued at
// once, not the total ever sent.
func TestControlQueueReleasesSentFrames(t *testing.T) {
	const limit = 64 << 10
	s := serverWithHeldWriter(t, Options{MaxControlQueue: limit, CloseTimeout: 100 * time.Millisecond})
	frame := controlFrame(TypeError, ErrorMessage{Code: CodeUnsupported, Reason: "unsupported"})
	perBatch := (limit / (HeaderSize + len(frame.Payload))) * 3 / 4 // fills three quarters of the cap
	for batch := 0; batch < 10; batch++ {
		s.mu.Lock()
		for i := 0; i < perBatch; i++ {
			_, err := s.enqueueLocked(frame, nil, false)
			require.NoError(t, err, "batch %d frame %d", batch, i)
		}
		// Play the writer: take every queued frame off as if sent.
		for len(s.urgent) > 0 {
			s.nextLocked()
		}
		assert.Zero(t, s.urgentBytes, "batch %d: sent frames still counted", batch)
		s.mu.Unlock()
	}
	assert.Equal(t, stateOpen, s.state)
}

// HELLO channels count toward MaxChannels; the rest are refused with 4002,
// without reasons so a long refusal list still fits in WELCOME.
func TestHelloChannelsRespectMaxChannels(t *testing.T) {
	sopts := acceptAll(func(c *Channel) { _, _ = c.Receive(context.Background()) })
	sopts.PingInterval, sopts.MaxChannels = -1, 2
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()
	go func() {
		c, err := l.Accept()
		if err == nil {
			_, _, _ = Accept(context.Background(), NewStreamConn(c), ServerConfig{}, sopts)
		}
	}()
	raw, err := net.Dial("tcp", l.Addr().String())
	require.NoError(t, err)
	defer raw.Close()
	specs := []ChannelSpec{{Name: "a", Reliable: true, Ordered: true}, {Name: "b", Reliable: true, Ordered: true}, {Name: "c", Reliable: true, Ordered: true}}
	_, res, err := Connect(context.Background(), NewStreamConn(raw), ClientConfig{Channels: specs}, Options{PingInterval: -1})
	require.NoError(t, err)
	assert.Len(t, res.Channels, 2)
	require.Len(t, res.Rejected, 1)
	assert.Equal(t, CodeChannelLimit, res.Rejected[0].Code)
	assert.Empty(t, res.Rejected[0].Reason)
}
