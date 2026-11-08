package xumux

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math/rand/v2"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// resumeNet is a client's view of a server that can be dialled again:
// every dial makes a new in-memory transport and runs Accept on the other
// end with the current ServerConfig.
type resumeNet struct {
	t       *testing.T
	kind    string // "stream" or "message"
	mu      sync.Mutex
	scfg    ServerConfig
	sopts   Options
	cut     []func()      // closes each transport, newest last
	servers chan *Session // new sessions (HELLO), not resumes
	handled atomic.Int32  // transports taken by RESUME
	refused chan error    // other Accept errors
	noDial  atomic.Bool
}

func newResumeNet(t *testing.T, kind string, rs *ResumeServer, sopts Options) *resumeNet {
	return &resumeNet{t: t, kind: kind, scfg: ServerConfig{Resume: rs}, sopts: sopts,
		servers: make(chan *Session, 16), refused: make(chan error, 16)}
}

func (n *resumeNet) dial(context.Context) (FrameConn, error) {
	if n.noDial.Load() {
		return nil, errors.New("unreachable")
	}
	var cl, sv FrameConn
	var cut func()
	if n.kind == "message" {
		a, b := memMessagePair()
		cl, sv, cut = NewMessageConn(a), NewMessageConn(b), func() { a.Close() }
	} else {
		a, b := net.Pipe()
		cl, sv, cut = NewStreamConn(a), NewStreamConn(b), func() { a.Close() }
	}
	n.mu.Lock()
	n.cut = append(n.cut, cut)
	cfg, opts := n.scfg, n.sopts
	n.mu.Unlock()
	go func() {
		s, _, err := Accept(context.Background(), sv, cfg, opts)
		switch {
		case err == nil:
			n.servers <- s
		case errors.Is(err, ErrFirstFrameHandled):
			n.handled.Add(1)
		default:
			n.refused <- err
		}
	}()
	return cl, nil
}

// drop cuts the newest transport.
func (n *resumeNet) drop() {
	n.mu.Lock()
	cut := n.cut[len(n.cut)-1]
	n.mu.Unlock()
	cut()
}

func (n *resumeNet) setServer(cfg ServerConfig) {
	n.mu.Lock()
	n.scfg = cfg
	n.mu.Unlock()
}

func (n *resumeNet) connect(cfg ClientConfig, opts Options) (*Session, *ConnectResult, *Session) {
	n.t.Helper()
	if cfg.Resume == nil {
		cfg.Resume = &ResumeClient{}
	}
	if cfg.Resume.Redial == nil {
		cfg.Resume.Redial = n.dial
	}
	if cfg.Resume.RetryDelay == 0 {
		cfg.Resume.RetryDelay = 5 * time.Millisecond
	}
	conn, err := n.dial(context.Background())
	require.NoError(n.t, err)
	c, res, err := Connect(ctx(n.t), conn, cfg, opts)
	require.NoError(n.t, err)
	var s *Session
	select {
	case s = <-n.servers:
	case <-time.After(3 * time.Second):
		n.t.Fatal("no server session")
	}
	n.t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = c.Close(ctx, CodeNormal, "")
		_ = s.Close(ctx, CodeNormal, "")
	})
	if slices.Contains(c.Extensions(), ExtensionSessionResume) {
		// A transport lost before RESUME_TOKEN arrives cannot be resumed.
		waitFor(n.t, "RESUME_TOKEN", func() bool {
			c.mu.Lock()
			defer c.mu.Unlock()
			return c.resume.token != ""
		})
	}
	return c, res, s
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	require.Eventually(t, cond, 5*time.Second, time.Millisecond, what)
}

// stream sends total random bytes in messages of random sizes and returns
// their hash.
func stream(ctx context.Context, ch *Channel, seed uint64, total int) ([32]byte, error) {
	rng := rand.New(rand.NewPCG(seed, seed))
	h := sha256.New()
	for sent := 0; sent < total; {
		n := min(1+rng.IntN(70000), total-sent)
		if rng.IntN(10) == 0 {
			n = min(rng.IntN(64), total-sent) // small, sometimes empty
		}
		b := make([]byte, n)
		for i := range b {
			b[i] = byte(rng.Uint32())
		}
		h.Write(b)
		if err := ch.Send(ctx, 7, b); err != nil {
			return [32]byte{}, err
		}
		sent += n
	}
	if err := ch.Send(ctx, 8, nil); err != nil { // end marker
		return [32]byte{}, err
	}
	return [32]byte(h.Sum(nil)), nil
}

func drain(ctx context.Context, ch *Channel) ([32]byte, int, error) {
	h := sha256.New()
	n := 0
	for {
		m, err := ch.Receive(ctx)
		if err != nil {
			return [32]byte{}, n, err
		}
		if m.Type == 8 {
			return [32]byte(h.Sum(nil)), n, nil
		}
		if m.Type != 7 {
			return [32]byte{}, n, errors.New("unexpected message type")
		}
		h.Write(m.Payload)
		n += len(m.Payload)
	}
}

// TestResumeEndToEnd drops the transport repeatedly in the middle of a
// transfer in both directions and checks that every byte arrives once, in
// order, on the same session.
func TestResumeEndToEnd(t *testing.T) {
	for _, kind := range []string{"stream", "message"} {
		t.Run(kind, func(t *testing.T) {
			rs := NewResumeServer(ResumeServerOptions{Timeout: 5 * time.Second})
			inbound := make(chan *Channel, 4)
			n := newResumeNet(t, kind, rs, acceptAll(func(ch *Channel) { inbound <- ch }))
			var resumed atomic.Int32
			c, res, s := n.connect(ClientConfig{
				Channels: []ChannelSpec{{Name: "bulk", Reliable: true, Ordered: true}},
				Resume:   &ResumeClient{OnResume: func() { resumed.Add(1) }},
			}, testOpts)
			require.Equal(t, []string{ExtensionSessionResume}, c.Extensions())
			cch := res.Channels[0]
			sch := <-inbound
			const total = 6 << 20
			tctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			type result struct {
				sum [32]byte
				n   int
				err error
			}
			sentC, sentS := make(chan result, 1), make(chan result, 1)
			gotC, gotS := make(chan result, 1), make(chan result, 1)
			go func() { h, err := stream(tctx, cch, 1, total); sentC <- result{h, total, err} }()
			go func() { h, err := stream(tctx, sch, 2, total); sentS <- result{h, total, err} }()
			go func() { h, k, err := drain(tctx, sch); gotS <- result{h, k, err} }()
			go func() { h, k, err := drain(tctx, cch); gotC <- result{h, k, err} }()

			const drops = 6
			for i := 0; i < drops; i++ {
				time.Sleep(time.Duration(20+rand.IntN(40)) * time.Millisecond)
				n.drop()
				waitFor(t, "resumed", func() bool { return resumed.Load() == int32(i+1) })
			}
			for _, pair := range [][2]chan result{{sentC, gotS}, {sentS, gotC}} {
				sent, got := <-pair[0], <-pair[1]
				require.NoError(t, sent.err)
				require.NoError(t, got.err)
				assert.Equal(t, total, got.n)
				assert.Equal(t, sent.sum, got.sum, "bytes differ after resume")
			}
			assert.Equal(t, int32(drops), n.handled.Load())
			select {
			case <-c.Done():
				t.Fatal("client session ended")
			case <-s.Done():
				t.Fatal("server session ended")
			default:
			}
			// Retention is bounded once everything is acknowledged.
			waitFor(t, "retention drained", func() bool {
				for _, x := range []*Session{c, s} {
					x.mu.Lock()
					r := x.resume
					busy := len(r.retained0) > 0
					for _, ch := range x.chans {
						busy = busy || len(ch.retained) > 0
					}
					x.mu.Unlock()
					if busy {
						return false
					}
				}
				return true
			})
		})
	}
}

// TestResumeServerKeepsSessionWithoutClient: the client is gone for a while
// (dials fail), then resumes within resumeTimeout.
func TestResumeAfterOutage(t *testing.T) {
	rs := NewResumeServer(ResumeServerOptions{Timeout: 5 * time.Second})
	inbound := make(chan *Channel, 1)
	n := newResumeNet(t, "stream", rs, acceptAll(func(ch *Channel) { inbound <- ch }))
	detached := make(chan error, 1)
	c, res, s := n.connect(ClientConfig{
		Channels: []ChannelSpec{{Name: "a", Reliable: true, Ordered: true}},
		Resume:   &ResumeClient{OnDetach: func(err error) { detached <- err }},
	}, testOpts)
	sch := <-inbound
	n.noDial.Store(true)
	n.drop()
	<-detached
	waitFor(t, "both detached", func() bool { return c.Detached() && s.Detached() })
	assert.Equal(t, 1, rs.Detached())
	// Sends wait while detached, then go through.
	sent := make(chan error, 2)
	go func() { sent <- res.Channels[0].Send(context.Background(), 1, []byte("from client")) }()
	go func() { sent <- sch.Send(context.Background(), 1, []byte("from server")) }()
	time.Sleep(100 * time.Millisecond)
	n.noDial.Store(false)
	require.NoError(t, <-sent)
	require.NoError(t, <-sent)
	assert.Equal(t, "from client", receiveString(t, sch))
	assert.Equal(t, "from server", receiveString(t, res.Channels[0]))
	assert.False(t, c.Detached())
	assert.Equal(t, 0, rs.Detached())
}

func TestResumeClientGivesUpAfterTimeout(t *testing.T) {
	rs := NewResumeServer(ResumeServerOptions{Timeout: time.Second})
	n := newResumeNet(t, "stream", rs, testOpts)
	c, _, s := n.connect(ClientConfig{}, testOpts)
	n.noDial.Store(true)
	n.drop()
	for _, x := range []*Session{c, s} {
		select {
		case <-x.Done():
		case <-time.After(5 * time.Second):
			t.Fatal("session kept past resumeTimeout")
		}
		assert.True(t, x.CloseInfo().Lost)
	}
}

func TestResumeNotNegotiated(t *testing.T) {
	// The server does not offer it: the session ends with the transport.
	a, b := net.Pipe()
	go Accept(context.Background(), NewStreamConn(b), ServerConfig{}, testOpts)
	c, _, err := Connect(ctx(t), NewStreamConn(a), ClientConfig{Resume: &ResumeClient{Redial: func(context.Context) (FrameConn, error) {
		return nil, errors.New("no")
	}}}, testOpts)
	require.NoError(t, err)
	assert.Empty(t, c.Extensions())
	a.Close()
	<-c.Done()
	assert.True(t, c.CloseInfo().Lost)

	// Resume needs Redial.
	a, _ = net.Pipe()
	_, _, err = Connect(ctx(t), NewStreamConn(a), ClientConfig{Resume: &ResumeClient{}}, testOpts)
	assert.Error(t, err)
}

// TestResumeFallbackToHello: after the server forgot the session (4100) or
// no longer implements the extension (1002), the client connects anew.
func TestResumeFallbackToHello(t *testing.T) {
	for _, tc := range []struct {
		name string
		code int
		cfg  func() ServerConfig
	}{
		{"not found", CodeResumeNotFound, func() ServerConfig { return ServerConfig{Resume: NewResumeServer(ResumeServerOptions{})} }},
		{"no extension", CodeProtocolError, func() ServerConfig { return ServerConfig{} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			n := newResumeNet(t, "stream", NewResumeServer(ResumeServerOptions{}), testOpts)
			type fb struct {
				s   *Session
				err error
			}
			fell := make(chan fb, 1)
			c, _, _ := n.connect(ClientConfig{Application: "app/1", Resume: &ResumeClient{
				OnFallback: func(s *Session, _ *ConnectResult, err error) { fell <- fb{s, err} },
			}}, testOpts)
			n.setServer(tc.cfg())
			n.drop()
			select {
			case <-c.Done():
			case <-time.After(5 * time.Second):
				t.Fatal("old session did not end")
			}
			assert.Equal(t, tc.code, c.CloseInfo().Code)
			got := <-fell
			require.NoError(t, got.err)
			defer got.s.Close(context.Background(), CodeNormal, "")
			ns := <-n.servers
			defer ns.Close(context.Background(), CodeNormal, "")
			_, err := got.s.Ping(ctx(t))
			require.NoError(t, err)
		})
	}
}

func TestResumeServerDiscardsBeyondMaxDetached(t *testing.T) {
	rs := NewResumeServer(ResumeServerOptions{MaxDetached: 1})
	n1 := newResumeNet(t, "stream", rs, testOpts)
	n2 := newResumeNet(t, "stream", rs, testOpts)
	_, _, s1 := n1.connect(ClientConfig{}, testOpts)
	_, _, s2 := n2.connect(ClientConfig{}, testOpts)
	n1.noDial.Store(true)
	n2.noDial.Store(true)
	n1.drop()
	waitFor(t, "first detached", s1.Detached)
	n2.drop()
	select {
	case <-s1.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("oldest detached session was not discarded")
	}
	waitFor(t, "second detached", s2.Detached)
	assert.Equal(t, 1, rs.Detached())
}

func TestResumeTokenRotation(t *testing.T) {
	rs := NewResumeServer(ResumeServerOptions{RotateEvery: 20 * time.Millisecond})
	inbound := make(chan *Channel, 1)
	n := newResumeNet(t, "stream", rs, acceptAll(func(ch *Channel) { inbound <- ch }))
	var resumed atomic.Int32
	c, res, _ := n.connect(ClientConfig{
		Channels: []ChannelSpec{{Name: "a", Reliable: true, Ordered: true}},
		Resume:   &ResumeClient{OnResume: func() { resumed.Add(1) }},
	}, testOpts)
	sch := <-inbound
	c.mu.Lock()
	first := c.resume.token
	c.mu.Unlock()
	waitFor(t, "token rotated", func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.resume.token != first
	})
	for i := 1; i <= 3; i++ {
		n.drop()
		waitFor(t, "resumed", func() bool { return resumed.Load() == int32(i) })
		require.NoError(t, res.Channels[0].Send(ctx(t), 1, []byte("x")))
		assert.Equal(t, "x", receiveString(t, sch))
		time.Sleep(30 * time.Millisecond)
	}
}

// ─── Raw peers ──────────────────────────────────────────────────────────────

// resumeServer starts Accepts on fresh raw transports.
type rawResume struct {
	t   *testing.T
	cfg ServerConfig
	opt Options
}

// open makes a new transport to the server and returns the raw client end.
func (rr rawResume) open() (*rawPeer, chan error) {
	lib, peer := rawPair(rr.t, "stream")
	errc := make(chan error, 1)
	go func() {
		s, _, err := Accept(context.Background(), lib, rr.cfg, rr.opt)
		if s != nil {
			rr.t.Cleanup(func() { s.Close(context.Background(), CodeNormal, "") })
		}
		errc <- err
	}()
	return peer, errc
}

func tokenOf(t *testing.T, f Frame) string {
	t.Helper()
	m := jsonOf(t, f)
	tok, _ := m["token"].(string)
	require.NotEmpty(t, tok)
	return tok
}

// rawHandshake does HELLO with session-resume/1 and returns the token.
func (rr rawResume) rawHandshake(extra map[string]any) (*rawPeer, string) {
	peer, _ := rr.open()
	h := map[string]any{"extensions": []string{ExtensionSessionResume}}
	for k, v := range extra {
		h[k] = v
	}
	w := jsonOf(rr.t, peer.helloAndWelcome(h))
	require.Equal(rr.t, []any{ExtensionSessionResume}, w["extensions"])
	f := peer.next(nil)
	require.True(rr.t, f.IsControl() && f.Type == TypeResumeToken, "first frame after WELCOME is RESUME_TOKEN")
	m := jsonOf(rr.t, f)
	assert.EqualValues(rr.t, rr.cfg.Resume.opts.Timeout/time.Second, m["resumeTimeout"])
	return peer, tokenOf(rr.t, f)
}

func resumeFrame(tok string, received [][2]uint32, extra map[string]any) Frame {
	m := map[string]any{"version": Version, "token": tok, "received": received}
	for k, v := range extra {
		m[k] = v
	}
	return ctrl(TypeResume, m)
}

func receipt(entries ...[2]uint32) Frame {
	var es []windowEntry
	for _, e := range entries {
		es = append(es, windowEntry{uint16(e[0]), e[1]})
	}
	f := windowUpdateFrame(es)
	f.Type = TypeReceipt
	return f
}

func newRawResume(t *testing.T) rawResume {
	return rawResume{t: t, cfg: ServerConfig{Resume: NewResumeServer(ResumeServerOptions{})}, opt: testOpts}
}

func TestResumeRefusals(t *testing.T) {
	rr := newRawResume(t)
	first, tok := rr.rawHandshake(nil)
	first.fc.Close()

	for _, tc := range []struct {
		name string
		f    Frame
		code int
	}{
		{"unknown token", resumeFrame("nope", [][2]uint32{{0, 0}}, nil), CodeResumeNotFound},
		{"version", ctrl(TypeResume, map[string]any{"version": []int{0, 2, 0}, "token": tok, "received": [][2]uint32{{0, 0}}}), CodeVersionMismatch},
		{"no channel 0", resumeFrame(tok, [][2]uint32{{1, 0}}, nil), CodeInvalidMessage},
		{"malformed", ctrl(TypeResume, map[string]any{"version": Version, "token": tok, "received": [][]int{{0}}}), CodeInvalidMessage},
		{"missing token", ctrl(TypeResume, map[string]any{"version": Version, "received": [][2]uint32{{0, 0}}}), CodeInvalidMessage},
	} {
		t.Run(tc.name, func(t *testing.T) {
			peer, errc := rr.open()
			peer.send(tc.f)
			assert.Equal(t, tc.code, peer.closeCode())
			var ce *ProtocolError
			assert.ErrorAs(t, <-errc, &ce)
		})
	}
	// None of those ended the session: the token still works.
	peer, errc := rr.open()
	peer.send(resumeFrame(tok, [][2]uint32{{0, 0}}, nil))
	peer.control(TypeResumed)
	assert.ErrorIs(t, <-errc, ErrFirstFrameHandled)
}

func TestResumeStateMismatchEndsSession(t *testing.T) {
	for _, tc := range []struct {
		name string
		rec  [][2]uint32
		code int
	}{
		{"channel 0", [][2]uint32{{0, 99}}, CodeResumeStateMismatch},
		{"unknown channel", [][2]uint32{{0, 0}, {5, 3}}, CodeResumeStateMismatch},
		{"channel", [][2]uint32{{0, 0}, {1, 3}}, CodeResumeStateMismatch},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rr := newRawResume(t)
			rr.opt = acceptAll(nil)
			first, tok := rr.rawHandshake(map[string]any{"channels": []map[string]any{{"id": 1, "name": "a", "reliable": true, "ordered": true}}})
			first.fc.Close()
			peer, _ := rr.open()
			peer.send(resumeFrame(tok, tc.rec, nil))
			assert.Equal(t, tc.code, peer.closeCode())
			// 4103 ends the session: the token is gone.
			peer, _ = rr.open()
			peer.send(resumeFrame(tok, [][2]uint32{{0, 0}}, nil))
			assert.Equal(t, CodeResumeNotFound, peer.closeCode())
		})
	}
}

func TestResumeDataLost(t *testing.T) {
	rr := newRawResume(t)
	first, tok := rr.rawHandshake(nil)
	// Acknowledge the RESUME_TOKEN, then claim, on resume, not to have it.
	first.send(receipt([2]uint32{0, 1}))
	time.Sleep(50 * time.Millisecond)
	first.fc.Close()
	peer, _ := rr.open()
	peer.send(resumeFrame(tok, [][2]uint32{{0, 0}}, nil))
	assert.Equal(t, CodeResumeDataLost, peer.closeCode())
}

func TestResumeExpired(t *testing.T) {
	rr := newRawResume(t)
	rr.cfg.Resume = NewResumeServer(ResumeServerOptions{Timeout: time.Second})
	first, tok := rr.rawHandshake(nil)
	first.fc.Close()
	time.Sleep(1300 * time.Millisecond)
	peer, _ := rr.open()
	peer.send(resumeFrame(tok, [][2]uint32{{0, 0}}, nil))
	assert.Equal(t, CodeResumeExpired, peer.closeCode())
}

func TestResumeAuthorization(t *testing.T) {
	t.Run("AuthorizeResume", func(t *testing.T) {
		rr := newRawResume(t)
		var orig *Session
		rr.cfg.AuthorizeResume = func(_ context.Context, req *ResumeRequest, s *Session) error {
			orig = s
			if req.Auth["user"] != "alice" {
				return errors.New("different identity")
			}
			return nil
		}
		first, tok := rr.rawHandshake(nil)
		first.fc.Close()
		peer, _ := rr.open()
		peer.send(resumeFrame(tok, [][2]uint32{{0, 0}}, map[string]any{"auth": map[string]any{"user": "mallory"}}))
		assert.Equal(t, CodeAuthFailed, peer.closeCode())
		require.NotNil(t, orig)
		peer, _ = rr.open()
		peer.send(resumeFrame(tok, [][2]uint32{{0, 0}}, map[string]any{"auth": map[string]any{"user": "alice"}}))
		peer.control(TypeResumed)
	})
	t.Run("Authorize without AuthorizeResume", func(t *testing.T) {
		rr := newRawResume(t)
		rr.cfg.Authorize = func(context.Context, *Hello) error { return nil }
		first, tok := rr.rawHandshake(nil)
		first.fc.Close()
		peer, _ := rr.open()
		peer.send(resumeFrame(tok, [][2]uint32{{0, 0}}, nil))
		assert.Equal(t, CodeAuthFailed, peer.closeCode())
	})
	t.Run("Refusal code", func(t *testing.T) {
		rr := newRawResume(t)
		rr.cfg.AuthorizeResume = func(context.Context, *ResumeRequest, *Session) error {
			return &Refusal{Code: CodeBusy, Reason: "later"}
		}
		first, tok := rr.rawHandshake(nil)
		first.fc.Close()
		peer, _ := rr.open()
		peer.send(resumeFrame(tok, [][2]uint32{{0, 0}}, nil))
		assert.Equal(t, CodeBusy, peer.closeCode())
	})
}

// TestResumeTokenConfirmation follows §3.3: until the client sends a frame
// on the transport that carried RESUMED, the old token and the new one both
// work; after that only the new one, and each resume issues a new token.
func TestResumeTokenConfirmation(t *testing.T) {
	rr := newRawResume(t)
	first, t1 := rr.rawHandshake(nil)
	first.fc.Close()

	p, _ := rr.open()
	p.send(resumeFrame(t1, [][2]uint32{{0, 1}}, nil))
	t2 := tokenOf(t, p.control(TypeResumed))
	assert.NotEqual(t, t1, t2)
	p.fc.Close() // lost before the client confirmed: T1 still works

	p, _ = rr.open()
	p.send(resumeFrame(t1, [][2]uint32{{0, 1}}, nil))
	f := p.next(func(f Frame) bool { return f.IsControl() })
	t.Logf("%x %s", f.Type, f.Payload)
	t3 := tokenOf(t, f)
	p.send(receipt([2]uint32{0, 1})) // confirm
	time.Sleep(50 * time.Millisecond)
	p.fc.Close()

	for _, old := range []string{t1, t2} {
		p, _ = rr.open()
		p.send(resumeFrame(old, [][2]uint32{{0, 1}}, nil))
		assert.Equal(t, CodeResumeNotFound, p.closeCode())
	}
	p, _ = rr.open()
	p.send(resumeFrame(t3, [][2]uint32{{0, 1}}, nil))
	p.control(TypeResumed)
}

func TestResumeTokenRotationRetiresOld(t *testing.T) {
	rr := newRawResume(t)
	rr.cfg.Resume = NewResumeServer(ResumeServerOptions{RotateEvery: 30 * time.Millisecond})
	first, t1 := rr.rawHandshake(nil)
	f := first.control(TypeResumeToken)
	t2 := tokenOf(t, f)
	assert.NotEqual(t, t1, t2)
	// Before the replacement is acknowledged, both work; acknowledge it.
	first.send(receipt([2]uint32{0, 2}))
	time.Sleep(10 * time.Millisecond)
	first.fc.Close()
	p, _ := rr.open()
	p.send(resumeFrame(t1, [][2]uint32{{0, 2}}, nil))
	assert.Equal(t, CodeResumeNotFound, p.closeCode())
}

// TestResumeTakeover: a RESUME for a session that still appears attached
// takes it over; the old transport is closed without CLOSE (§6).
func TestResumeTakeover(t *testing.T) {
	rr := newRawResume(t)
	first, tok := rr.rawHandshake(nil)
	p, errc := rr.open()
	p.send(resumeFrame(tok, [][2]uint32{{0, 1}}, nil))
	p.control(TypeResumed)
	assert.ErrorIs(t, <-errc, ErrFirstFrameHandled)
	first.waitEnded()
	for {
		select {
		case f := <-first.frames:
			assert.False(t, f.IsControl() && f.Type == TypeClose, "CLOSE on the taken-over transport")
			continue
		default:
		}
		break
	}
	// The new transport works.
	p.send(probeFrame(0, TypePing, 9))
	p.next(func(f Frame) bool { return f.IsProbe() && f.Type == TypePong })
}

// TestResumeReplaysUnacknowledged: what the server sent and the client did
// not acknowledge is sent again after RESUMED, from the client's count, in
// the original order.
func TestResumeReplaysUnacknowledged(t *testing.T) {
	rr := newRawResume(t)
	handled := make(chan *Channel, 1)
	rr.opt = acceptAll(func(ch *Channel) {
		for i := 0; i < 5; i++ {
			ch.Send(context.Background(), byte(10+i), []byte{byte(i)})
		}
		handled <- ch
	})
	first, tok := rr.rawHandshake(map[string]any{"channels": []map[string]any{{"id": 1, "name": "a", "reliable": true, "ordered": true}}})
	for i := 0; i < 5; i++ {
		first.next(func(f Frame) bool { return f.Channel == 1 })
	}
	<-handled
	first.fc.Close()
	p, _ := rr.open()
	// The client got channel 0's RESUME_TOKEN and 2 of the 5 messages.
	p.send(resumeFrame(tok, [][2]uint32{{0, 1}, {1, 2}}, nil))
	m := jsonOf(t, p.control(TypeResumed))
	assert.Equal(t, []any{[]any{0.0, 0.0}, []any{1.0, 0.0}}, m["received"])
	for i := 2; i < 5; i++ {
		f := p.next(func(f Frame) bool { return f.Channel == 1 })
		assert.Equal(t, byte(10+i), f.Type)
	}
}

func TestResumeReceiptRules(t *testing.T) {
	t.Run("length", func(t *testing.T) {
		rr := newRawResume(t)
		p, _ := rr.rawHandshake(nil)
		p.send(Frame{Channel: 0, Type: TypeReceipt, Payload: []byte{0, 0, 0}})
		assert.Equal(t, CodeInvalidMessage, p.closeCode())
	})
	t.Run("too high", func(t *testing.T) {
		rr := newRawResume(t)
		p, _ := rr.rawHandshake(nil)
		p.send(receipt([2]uint32{0, 5}))
		assert.Equal(t, CodeResumeStateMismatch, p.closeCode())
	})
	t.Run("ignored", func(t *testing.T) {
		rr := newRawResume(t)
		p, _ := rr.rawHandshake(nil)
		// Unknown channel, then stale: ignored.
		p.send(receipt([2]uint32{9, 5}, [2]uint32{0, 1}))
		p.send(receipt([2]uint32{0, 0}))
		p.send(probeFrame(0, TypePing, 1))
		p.next(func(f Frame) bool { return f.IsProbe() && f.Type == TypePong })
	})
	t.Run("RESUME mid-connection", func(t *testing.T) {
		rr := newRawResume(t)
		p, tok := rr.rawHandshake(nil)
		p.send(resumeFrame(tok, [][2]uint32{{0, 0}}, nil))
		assert.Equal(t, CodeProtocolError, p.closeCode())
	})
	t.Run("RESUME_TOKEN from the client", func(t *testing.T) {
		rr := newRawResume(t)
		p, _ := rr.rawHandshake(nil)
		p.send(ctrl(TypeResumeToken, map[string]any{"token": "x", "resumeTimeout": 5}))
		assert.Equal(t, CodeProtocolError, p.closeCode())
	})
}

// TestResumeFrameLimit: a sender stops at its unacknowledged-frame limit
// and goes on when a RECEIPT arrives (§5).
func TestResumeFrameLimit(t *testing.T) {
	rr := newRawResume(t)
	rr.cfg.Resume = NewResumeServer(ResumeServerOptions{FrameLimit: 1})
	rr.opt = acceptAll(func(ch *Channel) {
		for i := 0; i < 200; i++ {
			if ch.Send(context.Background(), 1, nil) != nil {
				return
			}
		}
	})
	p, _ := rr.rawHandshake(map[string]any{"channels": []map[string]any{{"id": 1, "name": "a", "reliable": true, "ordered": true}}})
	count := func(d time.Duration) int {
		n := 0
		deadline := time.After(d)
		for {
			select {
			case f := <-p.frames:
				if f.Channel == 1 {
					n++
				}
			case <-deadline:
				return n
			}
		}
	}
	assert.Equal(t, MinResumeFrameLimit, count(200*time.Millisecond))
	p.send(receipt([2]uint32{1, 100}))
	assert.Equal(t, 72, count(200*time.Millisecond))
}

// TestResumeReceiptBeforeCredit: a client returns credit only after a
// RECEIPT covering the frames (§5).
func TestResumeReceiptBeforeCredit(t *testing.T) {
	lib, peer := rawPair(t, "stream")
	go func() {
		peer.awaitHelloSendWelcome(map[string]any{"extensions": []string{ExtensionSessionResume}})
		peer.send(ctrl(TypeResumeToken, map[string]any{"token": "tok-0123456789abcdef", "resumeTimeout": 30}))
	}()
	c, res, err := Connect(ctx(t), lib, ClientConfig{
		Channels: []ChannelSpec{{Name: "a", Reliable: true, Ordered: true}},
		Resume:   &ResumeClient{Redial: func(context.Context) (FrameConn, error) { return nil, errors.New("no") }},
	}, testOpts)
	require.NoError(t, err)
	defer c.Close(context.Background(), CodeNormal, "")
	peer.send(Frame{Channel: 1, Type: 1, Payload: []byte("hello")})
	_, err = res.Channels[0].Receive(ctx(t))
	require.NoError(t, err)
	var sawReceipt bool
	for {
		f := peer.next(func(f Frame) bool { return f.IsControl() })
		if f.Type == TypeReceipt {
			for i := 0; i < len(f.Payload); i += 6 {
				if binary.BigEndian.Uint16(f.Payload[i:]) == 1 && binary.BigEndian.Uint32(f.Payload[i+2:]) == 1 {
					sawReceipt = true
				}
			}
		}
		if f.Type == TypeWindowUpdate {
			require.True(t, sawReceipt, "WINDOW_UPDATE before the RECEIPT covering the frame")
			return
		}
	}
}

func TestResumeClientRequiresTokenFirst(t *testing.T) {
	lib, peer := rawPair(t, "stream")
	go func() {
		peer.awaitHelloSendWelcome(map[string]any{"extensions": []string{ExtensionSessionResume}})
		peer.send(probeFrame(0, TypePing, 1))
	}()
	c, _, err := Connect(ctx(t), lib, ClientConfig{Resume: &ResumeClient{Redial: func(context.Context) (FrameConn, error) {
		return nil, errors.New("no")
	}}}, testOpts)
	require.NoError(t, err)
	assert.Equal(t, CodeProtocolError, peer.closeCode())
	<-c.Done()
}

func TestResumeClientRejectsInvalidToken(t *testing.T) {
	for _, m := range []map[string]any{
		{"token": "", "resumeTimeout": 5},
		{"token": "x", "resumeTimeout": 0},
		{"token": "x", "resumeTimeout": 3601},
		{"token": string(bytes.Repeat([]byte("x"), 513)), "resumeTimeout": 5},
		{"resumeTimeout": 5},
	} {
		lib, peer := rawPair(t, "stream")
		go func() {
			peer.awaitHelloSendWelcome(map[string]any{"extensions": []string{ExtensionSessionResume}})
			peer.send(ctrl(TypeResumeToken, m))
		}()
		c, _, err := Connect(ctx(t), lib, ClientConfig{Resume: &ResumeClient{Redial: func(context.Context) (FrameConn, error) {
			return nil, errors.New("no")
		}}}, testOpts)
		require.NoError(t, err)
		assert.Equal(t, CodeInvalidMessage, peer.closeCode(), "%v", m)
		<-c.Done()
	}
}

// rawResumeServer is a scripted server for a library client: it answers
// HELLO with session-resume/1, then answers RESUMEs with answer.
func rawClientSetup(t *testing.T, answer func(p *rawPeer, resume map[string]any)) (*Session, chan *rawPeer) {
	lib, peer := rawPair(t, "stream")
	go func() {
		peer.awaitHelloSendWelcome(map[string]any{"extensions": []string{ExtensionSessionResume}})
		peer.send(ctrl(TypeResumeToken, map[string]any{"token": "tok-0123456789abcdef", "resumeTimeout": 2}))
	}()
	peers := make(chan *rawPeer, 8)
	peers <- peer
	c, _, err := Connect(ctx(t), lib, ClientConfig{Resume: &ResumeClient{
		RetryDelay: 5 * time.Millisecond,
		Redial: func(context.Context) (FrameConn, error) {
			lib, p := rawPair(t, "stream")
			peers <- p
			go func() {
				select {
				case f := <-p.frames:
					if f.IsControl() && f.Type == TypeResume {
						answer(p, jsonOf(t, f))
					}
				case <-p.ended:
				}
			}()
			return lib, nil
		},
	}}, testOpts)
	require.NoError(t, err)
	t.Cleanup(func() { c.Close(context.Background(), CodeNormal, "") })
	waitFor(t, "RESUME_TOKEN", func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.resume.token != ""
	})
	return c, peers
}

func TestResumeClientChecksResumed(t *testing.T) {
	for _, tc := range []struct {
		name     string
		received [][2]uint32
		code     int
	}{
		{"too many", [][2]uint32{{0, 50}}, CodeResumeStateMismatch},
		{"no channel 0", [][2]uint32{{3, 0}}, CodeInvalidMessage},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := make(chan int, 1)
			c, peers := rawClientSetup(t, func(p *rawPeer, resume map[string]any) {
				assert.Equal(t, "tok-0123456789abcdef", resume["token"])
				assert.Equal(t, []any{[]any{0.0, 1.0}}, resume["received"])
				p.send(ctrl(TypeResumed, map[string]any{"token": "tok-2", "resumeTimeout": 2, "received": tc.received}))
				got <- p.closeCode()
			})
			first := <-peers
			first.control(TypeReceipt) // the client acknowledges RESUME_TOKEN
			first.fc.Close()
			assert.Equal(t, tc.code, <-got)
			<-c.Done()
			assert.Equal(t, tc.code, c.CloseInfo().Code)
		})
	}
}

func TestResumeClientRetriesAfterBusy(t *testing.T) {
	var tries atomic.Int32
	confirmed := make(chan struct{})
	c, peers := rawClientSetup(t, func(p *rawPeer, _ map[string]any) {
		if tries.Add(1) == 1 {
			p.send(ctrl(TypeClose, map[string]any{"code": CodeBusy}))
			return
		}
		p.send(ctrl(TypeResumed, map[string]any{"token": "tok-2", "resumeTimeout": 2, "received": [][2]uint32{{0, 0}}}))
		// The confirming RECEIPT for channel 0 comes first.
		f := p.next(nil)
		assert.True(t, f.IsControl() && f.Type == TypeReceipt)
		close(confirmed)
	})
	(<-peers).fc.Close()
	<-confirmed
	waitFor(t, "resumed", func() bool { return tries.Load() == 2 && !c.Detached() })
	c.mu.Lock()
	assert.Equal(t, "tok-2", c.resume.token)
	c.mu.Unlock()
}

func TestResumeClientUnexpectedAnswer(t *testing.T) {
	got := make(chan int, 1)
	c, peers := rawClientSetup(t, func(p *rawPeer, _ map[string]any) {
		p.send(ctrl(TypeWelcome, map[string]any{"version": Version}))
		got <- p.closeCode()
	})
	(<-peers).fc.Close()
	assert.Equal(t, CodeProtocolError, <-got)
	<-c.Done()
}
