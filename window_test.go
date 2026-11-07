package xumux

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// delayConn delivers each written frame after a fixed one-way delay, in
// order, without limiting bandwidth: a long-RTT link.
type delayConn struct {
	FrameConn
	delay time.Duration
	mu    sync.Mutex
	cond  *sync.Cond
	q     []delayed
	ended bool
	err   error
}

type delayed struct {
	due time.Time
	f   Frame
}

func newDelayConn(c FrameConn, d time.Duration) *delayConn {
	dc := &delayConn{FrameConn: c, delay: d}
	dc.cond = sync.NewCond(&dc.mu)
	go dc.run()
	return dc
}

func (d *delayConn) WriteFrame(f Frame) error {
	f.Payload = append([]byte(nil), f.Payload...)
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.err != nil {
		return d.err
	}
	d.q = append(d.q, delayed{time.Now().Add(d.delay), f})
	d.cond.Signal()
	return nil
}

func (d *delayConn) run() {
	for {
		d.mu.Lock()
		for len(d.q) == 0 && !d.ended {
			d.cond.Wait()
		}
		if len(d.q) == 0 {
			d.mu.Unlock()
			_ = d.FrameConn.CloseWrite()
			return
		}
		next := d.q[0]
		d.q = d.q[1:]
		d.mu.Unlock()
		time.Sleep(time.Until(next.due))
		if err := d.FrameConn.WriteFrame(next.f); err != nil {
			d.mu.Lock()
			d.err = err
			d.mu.Unlock()
			return
		}
	}
}

func (d *delayConn) CloseWrite() error {
	d.mu.Lock()
	d.ended = true
	d.cond.Signal()
	d.mu.Unlock()
	return nil
}

func (d *delayConn) Close() error {
	_ = d.CloseWrite()
	return d.FrameConn.Close()
}

// delayedPair connects a client and server over TCP with oneWay delay each way.
func delayedPair(t *testing.T, oneWay time.Duration, copts, sopts Options, specs ...ChannelSpec) (*Session, *Session, *ConnectResult) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()
	srv := make(chan *Session, 1)
	go func() {
		c, err := l.Accept()
		if err != nil {
			srv <- nil
			return
		}
		s, _, err := Accept(context.Background(), newDelayConn(NewStreamConn(c), oneWay), ServerConfig{}, sopts)
		if err != nil {
			srv <- nil
			return
		}
		srv <- s
	}()
	c, err := net.Dial("tcp", l.Addr().String())
	require.NoError(t, err)
	client, res, err := Connect(context.Background(), newDelayConn(NewStreamConn(c), oneWay), ClientConfig{Channels: specs}, copts)
	require.NoError(t, err)
	server := <-srv
	require.NotNil(t, server)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = client.Close(ctx, CodeNormal, "")
		<-server.Done()
	})
	return client, server, res
}

var reliable = ChannelSpec{Name: "bulk", Reliable: true, Ordered: true}

// blast sends n bytes on ch and returns how long the peer took to receive them.
func blast(t *testing.T, ch, peer *Channel, n int) time.Duration {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	start := time.Now()
	go func() {
		chunk := make([]byte, 64<<10)
		for sent := 0; sent < n; sent += len(chunk) {
			if err := ch.Send(ctx, 1, chunk); err != nil {
				return
			}
		}
	}()
	for got := 0; got < n; {
		m, err := peer.Receive(ctx)
		require.NoError(t, err)
		got += len(m.Payload)
	}
	return time.Since(start)
}

// serverChannel returns the server's side of the client's first channel.
func serverChannel(t *testing.T, s *Session) *Channel {
	t.Helper()
	require.Eventually(t, func() bool { return len(s.Channels()) == 1 }, time.Second, time.Millisecond)
	return s.Channels()[0]
}

func TestWindowGrowsOnLongRTT(t *testing.T) {
	const oneWay = 20 * time.Millisecond // 40 ms RTT
	opts := Options{PingInterval: -1}
	sopts := acceptAll(func(*Channel) {})
	sopts.PingInterval = -1
	_, server, res := delayedPair(t, oneWay, opts, sopts, reliable)
	ch, peer := res.Channels[0], serverChannel(t, server)

	const n = 32 << 20
	dt := blast(t, ch, peer, n)
	rate := float64(n) / dt.Seconds()
	limit := float64(DefaultInitialWindow) / (2 * oneWay).Seconds() // what a fixed window allows
	t.Logf("32 MiB at 40 ms RTT in %v: %.1f MB/s (fixed window: %.1f MB/s), window %d KiB",
		dt.Round(time.Millisecond), rate/1e6, limit/1e6, peer.ReceiveWindow()>>10)
	assert.Greater(t, peer.ReceiveWindow(), int64(DefaultInitialWindow), "the window grew")
	assert.LessOrEqual(t, peer.ReceiveWindow(), int64(DefaultMaxChannelWindow))
	assert.Greater(t, rate, 4*limit, "well beyond what the initial window allows")
}

func TestWindowAutotuneCanBeDisabled(t *testing.T) {
	const oneWay = 10 * time.Millisecond
	sopts := acceptAll(func(*Channel) {})
	sopts.PingInterval, sopts.MaxChannelWindow = -1, -1
	_, server, res := delayedPair(t, oneWay, Options{PingInterval: -1}, sopts, reliable)
	peer := serverChannel(t, server)
	blast(t, res.Channels[0], peer, 8<<20)
	assert.Equal(t, int64(DefaultInitialWindow), peer.ReceiveWindow())
}

func TestConnectionWindowBudget(t *testing.T) {
	const oneWay = 10 * time.Millisecond
	const budget = 1 << 20
	sopts := acceptAll(func(*Channel) {})
	sopts.PingInterval, sopts.ConnectionWindow = -1, budget
	_, server, res := delayedPair(t, oneWay, Options{PingInterval: -1}, sopts, reliable, reliable)
	require.Eventually(t, func() bool { return len(server.Channels()) == 2 }, time.Second, time.Millisecond)
	var wg sync.WaitGroup
	for i, peer := range server.Channels() {
		var ch *Channel
		for _, c := range res.Channels {
			if c.ID() == peer.ID() {
				ch = c
			}
		}
		wg.Add(1)
		go func(i int, ch, peer *Channel) {
			defer wg.Done()
			blast(t, ch, peer, 8<<20)
		}(i, ch, peer)
	}
	wg.Wait()
	var total int64
	for _, c := range server.Channels() {
		total += c.ReceiveWindow()
	}
	t.Logf("windows after two concurrent 8 MiB transfers: total %d KiB", total>>10)
	assert.LessOrEqual(t, total, int64(budget), "the sum of windows stays within ConnectionWindow")
	assert.Greater(t, total, int64(2*DefaultInitialWindow), "but they did grow into the budget")

	// Closing channels releases their windows.
	for _, c := range res.Channels {
		require.NoError(t, c.Close(ctx(t), CodeNormal, ""))
	}
	require.Eventually(t, func() bool {
		server.mu.Lock()
		defer server.mu.Unlock()
		return server.windowTotal == 0
	}, 2*time.Second, 5*time.Millisecond, "budget released")
}

func TestWindowDoesNotGrowOnFastLink(t *testing.T) {
	// No added delay: a window is used much more slowly than two (tiny) round
	// trips, so the sender is not window-limited and nothing grows.
	sopts := acceptAll(func(*Channel) {})
	sopts.PingInterval = -1
	_, server, res := pair(t, Options{PingInterval: -1}, sopts, reliable)
	peer := serverChannel(t, server)
	_, err := server.Ping(ctx(t))
	require.NoError(t, err)
	blast(t, res.Channels[0], peer, 4<<20)
	assert.LessOrEqual(t, peer.ReceiveWindow(), int64(4*DefaultInitialWindow))
}

func TestFirstWindowNeedsATimedSample(t *testing.T) {
	// SRTT is known and tiny; the peer then sends one message as large as the
	// whole window, long after the channel opened. Consuming it took far longer
	// than two round trips, so the window must not grow.
	sopts := acceptAll(func(*Channel) {})
	sopts.PingInterval = -1
	_, server, res := pair(t, Options{PingInterval: -1}, sopts, reliable)
	peer := serverChannel(t, server)
	_, err := server.Ping(ctx(t))
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)
	require.NoError(t, res.Channels[0].Send(ctx(t), 1, make([]byte, DefaultInitialWindow)))
	m, err := peer.Receive(ctx(t))
	require.NoError(t, err)
	require.Len(t, m.Payload, DefaultInitialWindow)
	time.Sleep(20 * time.Millisecond) // let the WINDOW_UPDATE go out
	assert.Equal(t, int64(DefaultInitialWindow), peer.ReceiveWindow())
}

func TestStaleRTTIsRefreshed(t *testing.T) {
	// The only RTT sample says 1 µs, but the link is 40 ms. Every growth check
	// fails against the stale sample until a fresh probe replaces it.
	const oneWay = 20 * time.Millisecond
	sopts := acceptAll(func(*Channel) {})
	sopts.PingInterval = -1
	_, server, res := delayedPair(t, oneWay, Options{PingInterval: -1}, sopts, reliable)
	peer := serverChannel(t, server)
	server.mu.Lock()
	server.stats.sample(time.Microsecond)
	server.rttProbedAt = time.Now().Add(-time.Hour)
	server.mu.Unlock()
	blast(t, res.Channels[0], peer, 32<<20)
	assert.Greater(t, peer.ReceiveWindow(), int64(DefaultInitialWindow), "growth resumed after the RTT was re-measured")
	assert.Greater(t, server.Stats().SRTT, time.Millisecond, "SRTT now reflects the real link")
}
