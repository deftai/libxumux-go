package xumux

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	mrand "math/rand/v2"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// session-resume/1 interop with libxumux over TCP, in both directions. A TCP
// proxy between the two loses the transport on demand: on both sides, or on
// the client's only (the server still sees the old transport, so the resume
// is a takeover). It can also hold back one direction, so that the frames in
// flight are lost with the transport, and refuse new connections.
// interop/libxumux-resume-peer.mjs runs libxumux, driven over its stdio with
// one JSON command per line.

// ─── The libxumux peer ──────────────────────────────────────────────────────

type jsPeer struct {
	t      *testing.T
	stdin  io.WriteCloser
	mu     sync.Mutex
	nextID int
	reply  map[int]chan map[string]any
	events []map[string]any
	notify chan struct{}
}

func spawnResumePeer(t *testing.T) *jsPeer {
	dir := os.Getenv("LIBXUMUX_DIR")
	if dir == "" {
		t.Skip("set LIBXUMUX_DIR to a built libxumux checkout to run interop tests")
	}
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node not found")
	}
	cmd := exec.Command(node, filepath.Join("interop", "libxumux-resume-peer.mjs"))
	cmd.Env = append(os.Environ(), "LIBXUMUX_DIR="+dir)
	cmd.Stderr = testWriter{t}
	stdin, err := cmd.StdinPipe()
	require.NoError(t, err)
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	require.NoError(t, cmd.Start())
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	p := &jsPeer{t: t, stdin: stdin, reply: map[int]chan map[string]any{}, notify: make(chan struct{}, 1)}
	go func() {
		sc := bufio.NewScanner(stdout)
		sc.Buffer(nil, 1<<20)
		for sc.Scan() {
			var m map[string]any
			if json.Unmarshal(sc.Bytes(), &m) != nil {
				continue
			}
			p.mu.Lock()
			if id, ok := m["id"].(float64); ok {
				if ch := p.reply[int(id)]; ch != nil {
					ch <- m
					delete(p.reply, int(id))
				}
			} else {
				p.events = append(p.events, m)
				select {
				case p.notify <- struct{}{}:
				default:
				}
			}
			p.mu.Unlock()
		}
	}()
	return p
}

// start sends a command and returns a channel for its answer.
func (p *jsPeer) start(op string, args map[string]any) <-chan map[string]any {
	p.mu.Lock()
	p.nextID++
	id := p.nextID
	ch := make(chan map[string]any, 1)
	p.reply[id] = ch
	p.mu.Unlock()
	m := map[string]any{"id": id, "op": op}
	for k, v := range args {
		m[k] = v
	}
	b, _ := json.Marshal(m)
	_, err := p.stdin.Write(append(b, '\n'))
	require.NoError(p.t, err)
	return ch
}

func await(t *testing.T, ch <-chan map[string]any, timeout time.Duration) map[string]any {
	t.Helper()
	select {
	case m := <-ch:
		return m
	case <-time.After(timeout):
		t.Fatal("libxumux peer did not answer")
		return nil
	}
}

// call runs a command and requires it to succeed.
func (p *jsPeer) call(op string, args map[string]any) map[string]any {
	p.t.Helper()
	m := await(p.t, p.start(op, args), 20*time.Second)
	require.Equal(p.t, true, m["ok"], "%s: %v", op, m)
	return m
}

// event waits for (and consumes) the first event that matches.
func (p *jsPeer) event(timeout time.Duration, match func(map[string]any) bool) map[string]any {
	p.t.Helper()
	deadline := time.After(timeout)
	for {
		p.mu.Lock()
		for i, e := range p.events {
			if match(e) {
				p.events = append(p.events[:i], p.events[i+1:]...)
				p.mu.Unlock()
				return e
			}
		}
		p.mu.Unlock()
		select {
		case <-p.notify:
		case <-time.After(10 * time.Millisecond):
		case <-deadline:
			p.t.Fatalf("libxumux peer: no matching event; have %v", p.events)
			return nil
		}
	}
}

// clear forgets the events received so far.
func (p *jsPeer) clear() {
	p.mu.Lock()
	p.events = nil
	p.mu.Unlock()
}

func isEvent(name string, kv ...any) func(map[string]any) bool {
	return func(e map[string]any) bool {
		if e["event"] != name {
			return false
		}
		for i := 0; i < len(kv); i += 2 {
			if e[kv[i].(string)] != kv[i+1] {
				return false
			}
		}
		return true
	}
}

// ─── A TCP proxy that loses connections ─────────────────────────────────────

// Directions of the proxy.
const (
	down = 0 // server → client
	up   = 1 // client → server
)

type lossyProxy struct {
	ln      net.Listener
	mu      sync.Mutex
	cond    *sync.Cond
	pairs   []*proxyPair
	orphans []*proxyPair
	held    [2]bool // stop forwarding in a direction
	refuse  bool
	// onDrop, if set, is called with the client port of each connection the
	// proxy drops, so the client can learn of it. Through the proxy, the
	// client's kernel may not see the reset: macOS can leave a socket with
	// unsent data ESTABLISHED after its loopback peer is gone, and only a
	// keepalive would then notice.
	onDrop func(port int)
}

// proxyPair is one client connection and the proxy's connection to the
// server for it. An orphaned pair has lost its client side, but the server
// side stays open and its bytes are read and discarded, so the server does
// not notice (until a resume takes the session over).
type proxyPair struct {
	conn   [2]net.Conn // [down] the client side, [up] the server side
	dead   bool
	orphan bool
}

func newLossyProxy(t *testing.T, target string) *lossyProxy {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	p := &lossyProxy{ln: ln}
	p.cond = sync.NewCond(&p.mu)
	t.Cleanup(func() { ln.Close(); p.drop() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			p.mu.Lock()
			refuse := p.refuse
			p.mu.Unlock()
			if refuse {
				c.Close()
				continue
			}
			s, err := net.Dial("tcp", target)
			if err != nil {
				c.Close()
				continue
			}
			pp := &proxyPair{conn: [2]net.Conn{c, s}}
			p.mu.Lock()
			p.pairs = append(p.pairs, pp)
			p.mu.Unlock()
			go p.forward(pp, up)
			go p.forward(pp, down)
		}
	}()
	return p
}

func (p *lossyProxy) addr() string { return p.ln.Addr().String() }

func (p *lossyProxy) port() int { return p.ln.Addr().(*net.TCPAddr).Port }

// forward copies one direction of pp, waiting while that direction is held.
// Bytes read while held are lost if the pair is dropped before release.
func (p *lossyProxy) forward(pp *proxyPair, dir int) {
	src, dst := pp.conn[1-dir], pp.conn[dir]
	buf := make([]byte, 32<<10)
	for {
		n, err := src.Read(buf)
		p.mu.Lock()
		for n > 0 && p.held[dir] && !pp.dead && !pp.orphan {
			p.cond.Wait()
		}
		dead, orphan := pp.dead, pp.orphan
		p.mu.Unlock()
		if err != nil || dead {
			p.kill(pp)
			return
		}
		if orphan {
			continue // discard
		}
		if _, err := dst.Write(buf[:n]); err != nil {
			p.kill(pp)
			return
		}
	}
}

func (p *lossyProxy) kill(pp *proxyPair) {
	p.mu.Lock()
	if pp.orphan && !pp.dead {
		p.mu.Unlock()
		return // the server side stays open
	}
	pp.dead = true
	p.cond.Broadcast()
	p.mu.Unlock()
	pp.conn[0].Close()
	pp.conn[1].Close()
}

// drop closes every connection through the proxy, on both sides (and the
// server side of orphaned ones).
func (p *lossyProxy) drop() {
	p.mu.Lock()
	pairs := append(p.pairs, p.orphans...)
	p.pairs, p.orphans = nil, nil
	for _, pp := range pairs {
		pp.dead = true
	}
	p.cond.Broadcast()
	p.mu.Unlock()
	for _, pp := range pairs {
		abort(pp.conn[0])
		abort(pp.conn[1])
	}
	p.notify(pairs)
}

func (p *lossyProxy) notify(pairs []*proxyPair) {
	if p.onDrop == nil {
		return
	}
	for _, pp := range pairs {
		if !pp.orphan || !pp.dead {
			p.onDrop(pp.conn[down].RemoteAddr().(*net.TCPAddr).Port)
		}
	}
}

// abort closes c with a reset, so the peer learns of it at once even if it
// is only writing.
func abort(c net.Conn) {
	_ = c.(*net.TCPConn).SetLinger(0)
	c.Close()
}

// dropClient closes the client side of every connection. The server side
// stays open, so the server still sees its session attached when the
// client resumes: a takeover (§6).
func (p *lossyProxy) dropClient() {
	p.mu.Lock()
	pairs := p.pairs
	p.pairs = nil
	for _, pp := range pairs {
		pp.orphan = true
	}
	p.orphans = append(p.orphans, pairs...)
	p.cond.Broadcast()
	p.mu.Unlock()
	for _, pp := range pairs {
		abort(pp.conn[down])
	}
	p.notify(pairs)
}

// lose loses the transport: on both sides for even i, and on the client side
// only (a takeover) for odd i.
func (p *lossyProxy) lose(i int) {
	if i%2 == 0 {
		p.drop()
	} else {
		p.dropClient()
	}
}

// hold stops (or resumes) forwarding in direction dir.
func (p *lossyProxy) hold(dir int, on bool) {
	p.mu.Lock()
	p.held[dir] = on
	p.cond.Broadcast()
	p.mu.Unlock()
}

func (p *lossyProxy) setRefuse(on bool) {
	p.mu.Lock()
	p.refuse = on
	p.mu.Unlock()
}

// ─── Token sniffing ─────────────────────────────────────────────────────────

// tokenLog records, in order, the tokens in RESUME_TOKEN and RESUMED frames a
// FrameConn reads or writes.
type tokenLog struct {
	mu   sync.Mutex
	seen []sniffed
}

type sniffed struct {
	typ byte
	tok string
}

func (l *tokenLog) note(f Frame) {
	if !f.IsControl() || (f.Type != TypeResumeToken && f.Type != TypeResumed) {
		return
	}
	var m struct{ Token string }
	if json.Unmarshal(f.Payload, &m) == nil {
		l.mu.Lock()
		l.seen = append(l.seen, sniffed{f.Type, m.Token})
		l.mu.Unlock()
	}
}

func (l *tokenLog) all() []sniffed {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]sniffed(nil), l.seen...)
}

type sniffConn struct {
	FrameConn
	log *tokenLog
}

func (c sniffConn) ReadFrame(max uint32) (Frame, error) {
	f, err := c.FrameConn.ReadFrame(max)
	if err == nil {
		c.log.note(f)
	}
	return f, err
}

func (c sniffConn) WriteFrame(f Frame) error {
	c.log.note(f)
	return c.FrameConn.WriteFrame(f)
}

// ─── The bulk exchange (see libxumux-resume-peer.mjs) ───────────────────────

func bulkExchange(ctx context.Context, ch *Channel) error { return bulkProgress(ctx, ch, nil) }

// bulkProgress is bulkExchange that adds the bytes it receives to prog.
func bulkProgress(ctx context.Context, ch *Channel, prog *atomic.Int64) error {
	total, err := strconv.Atoi(strings.TrimPrefix(ch.Name(), "bulk:"))
	if err != nil {
		return err
	}
	sent := make(chan string, 1)
	sendErr := make(chan error, 1)
	go func() {
		rng := mrand.New(mrand.NewPCG(mrand.Uint64(), 0))
		h := sha256.New()
		for n := 0; n < total; {
			k := min(1+rng.IntN(70000), total-n)
			if rng.IntN(10) == 0 {
				k = min(rng.IntN(64), total-n)
			}
			b := make([]byte, k)
			_, _ = rand.Read(b)
			h.Write(b)
			if err := ch.Send(ctx, 7, b); err != nil {
				sendErr <- err
				return
			}
			n += k
		}
		if err := ch.Send(ctx, 8, nil); err != nil {
			sendErr <- err
			return
		}
		sent <- hex.EncodeToString(h.Sum(nil))
	}()
	rh := sha256.New()
	received := 0
	var mine, theirs string
	for mine == "" || theirs == "" {
		m, err := ch.Receive(ctx)
		if err != nil {
			return fmt.Errorf("%s: %w", ch.Name(), err)
		}
		switch m.Type {
		case 7:
			rh.Write(m.Payload)
			received += len(m.Payload)
			if prog != nil {
				prog.Add(int64(len(m.Payload)))
			}
		case 8:
			if err := ch.Send(ctx, 9, []byte(hex.EncodeToString(rh.Sum(nil)))); err != nil {
				return err
			}
		case 9:
			select {
			case want := <-sent:
				mine = "mismatch"
				if string(m.Payload) == want {
					mine = "ok"
				}
			case err := <-sendErr:
				return err
			}
			if err := ch.Send(ctx, 10, []byte(mine)); err != nil {
				return err
			}
		case 10:
			theirs = string(m.Payload)
		default:
			return fmt.Errorf("%s: unexpected type %d", ch.Name(), m.Type)
		}
	}
	if mine != "ok" || theirs != "ok" || received != total {
		return fmt.Errorf("%s: mine=%s theirs=%s received=%d/%d", ch.Name(), mine, theirs, received, total)
	}
	return nil
}

// bulkOpts accepts every channel and runs the bulk exchange on bulk:* ones,
// reporting each result.
func bulkOpts(results chan<- error) Options { return bulkOptsProgress(results, nil) }

func bulkOptsProgress(results chan<- error, prog *atomic.Int64) Options {
	o := testOpts
	o.Acceptor = func(context.Context, ChannelRequest) (Handler, error) {
		return func(ch *Channel) {
			if strings.HasPrefix(ch.Name(), "bulk:") {
				c, cancel := context.WithTimeout(context.Background(), 60*time.Second)
				defer cancel()
				results <- bulkProgress(c, ch, prog)
			}
		}, nil
	}
	return o
}

// resumeWithToken sends RESUME with tok on a new connection to addr and returns the
// code of the CLOSE it is answered with. Only for tokens that must be refused.
func resumeWithToken(t *testing.T, addr, tok string) int {
	t.Helper()
	nc, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer nc.Close()
	_ = nc.SetDeadline(time.Now().Add(5 * time.Second))
	conn := NewStreamConn(nc)
	f, err := encodeControl(TypeResume, resumeMsg{Version: Version, Token: tok, Received: [][2]uint32{{0, 0}}})
	require.NoError(t, err)
	require.NoError(t, conn.WriteFrame(f))
	ans, err := conn.ReadFrame(DefaultMaxFramePayload)
	require.NoError(t, err)
	require.True(t, ans.IsControl() && ans.Type == TypeClose, "answer to RESUME: type %#x", ans.Type)
	m, err := decodeClose(ans.Payload)
	require.NoError(t, err)
	return m.Code
}

func bogusToken() string {
	var b [33]byte
	_, _ = rand.Read(b[:])
	return base64.RawURLEncoding.EncodeToString(b[:])
}

// ─── Go client ↔ libxumux server ───────────────────────────────────────────

type goResumeClient struct {
	s        *Session
	hello    []*Channel // HELLO channels
	log      *tokenLog
	resumed  atomic.Int32
	fallback chan *Session
}

func dialGoClient(t *testing.T, proxy *lossyProxy, results chan<- error, hello ...ChannelSpec) *goResumeClient {
	t.Helper()
	gc := &goResumeClient{log: &tokenLog{}, fallback: make(chan *Session, 1)}
	var mu sync.Mutex
	conns := map[int]net.Conn{} // by local port
	proxy.onDrop = func(port int) {
		mu.Lock()
		defer mu.Unlock()
		if c := conns[port]; c != nil {
			c.Close()
			delete(conns, port)
		}
	}
	dial := func(ctx context.Context) (FrameConn, error) {
		var d net.Dialer
		nc, err := d.DialContext(ctx, "tcp", proxy.addr())
		if err != nil {
			return nil, err
		}
		mu.Lock()
		conns[nc.LocalAddr().(*net.TCPAddr).Port] = nc
		mu.Unlock()
		return sniffConn{NewStreamConn(nc), gc.log}, nil
	}
	conn, err := dial(context.Background())
	require.NoError(t, err)
	s, res, err := Connect(ctx(t), conn, ClientConfig{Channels: hello, Resume: &ResumeClient{
		Redial:     dial,
		RetryDelay: 20 * time.Millisecond,
		OnResume:   func() { gc.resumed.Add(1) },
		OnFallback: func(ns *Session, _ *ConnectResult, err error) {
			if err != nil {
				t.Errorf("fallback HELLO: %v", err)
			}
			gc.fallback <- ns
		},
	}}, bulkOpts(results))
	require.NoError(t, err)
	require.Equal(t, []string{ExtensionSessionResume}, s.Extensions())
	gc.s, gc.hello = s, res.Channels
	waitFor(t, "RESUME_TOKEN", func() bool { return gc.token() != "" })
	t.Cleanup(func() {
		c, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = s.Close(c, CodeNormal, "")
	})
	return gc
}

func (gc *goResumeClient) token() string {
	gc.s.mu.Lock()
	defer gc.s.mu.Unlock()
	return gc.s.resume.token
}

func TestInteropResumeGoClient(t *testing.T) {
	peer := spawnResumePeer(t)
	ports := peer.call("listen", map[string]any{"timeouts": []int{30, 2}})["ports"].([]any)
	addr := fmt.Sprintf("127.0.0.1:%d", int(ports[0].(float64)))
	shortAddr := fmt.Sprintf("127.0.0.1:%d", int(ports[1].(float64)))

	t.Run("transfer", func(t *testing.T) {
		peer.clear()
		proxy := newLossyProxy(t, addr)
		results := make(chan error, 8)
		gc := newClientWithSession(t, peer, proxy, results, ChannelSpec{Name: "bulk:8000000", Reliable: true, Ordered: true})
		// Requested properties do not matter on TCP: every channel is on the
		// base path, so it is reliable and ordered, and replayed (§4).
		specs := []ChannelSpec{
			{Name: "bulk:16000000", Reliable: true, Ordered: true},
			{Name: "bulk:12000000", Reliable: true, Ordered: true},
			{Name: "bulk:8000000", Reliable: false, Ordered: false},
			{Name: "bulk:8000000", Reliable: true, Ordered: false},
		}
		const total = 52_000_000
		var prog atomic.Int64
		require.Len(t, gc.hello, 1)
		go func() {
			c, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			results <- bulkProgress(c, gc.hello[0], &prog)
		}()
		for _, sp := range specs {
			ch, err := gc.s.OpenChannel(ctx(t), sp)
			require.NoError(t, err)
			assert.Equal(t, Properties{Reliable: true, Ordered: true}, ch.Properties(), sp.Name)
			go func() {
				c, cancel := context.WithTimeout(context.Background(), 60*time.Second)
				defer cancel()
				results <- bulkProgress(c, ch, &prog)
			}()
		}
		const drops = 5
		for i := 0; i < drops; i++ {
			waitProgress(t, &prog, int64(i+1)*total/(drops+1))
			requireMidTransfer(t, &prog, total)
			proxy.lose(i)
			waitFor(t, "resumed", func() bool { return gc.resumed.Load() == int32(i+1) })
			peer.event(5*time.Second, isEvent("resumed"))
		}
		for range len(specs) + 1 {
			select {
			case err := <-results:
				require.NoError(t, err)
			case <-time.After(60 * time.Second):
				t.Fatal("bulk exchange did not finish")
			}
		}
		for range len(specs) + 1 {
			e := peer.event(10*time.Second, isEvent("bulk"))
			assert.Nil(t, e["error"], "libxumux: %v", e)
		}
		assert.Equal(t, int32(drops), gc.resumed.Load())
		assert.False(t, gc.s.Detached())
	})

	t.Run("token rotation across a resume", func(t *testing.T) {
		peer.clear()
		proxy := newLossyProxy(t, addr)
		results := make(chan error, 8)
		gc := newClientWithSession(t, peer, proxy, results)
		t1 := gc.token()
		// The server rotates while its bytes are held back: RESUME_TOKEN T2 is
		// lost with the transport, so the client resumes with T1.
		proxy.hold(down, true)
		peer.call("rotate", nil)
		time.Sleep(200 * time.Millisecond)
		proxy.drop()
		proxy.hold(down, false)
		waitFor(t, "resumed", func() bool { return gc.resumed.Load() == 1 })
		peer.event(5*time.Second, isEvent("resumed"))
		// After RESUMED (T3) the server retransmits RESUME_TOKEN T2, the most
		// recent token the client receives, so the client holds T2 (§3.1).
		var t2, t3 string
		waitFor(t, "RESUMED then RESUME_TOKEN", func() bool {
			seen := gc.log.all()
			for i, x := range seen {
				if x.typ == TypeResumed && i+1 < len(seen) && seen[i+1].typ == TypeResumeToken {
					t3, t2 = x.tok, seen[i+1].tok
					return true
				}
			}
			return false
		})
		assert.NotEqual(t, t1, t2)
		assert.NotEqual(t, t2, t3)
		assert.Equal(t, t2, gc.token())
		// Once T2 is acknowledged, T1 and T3 no longer work.
		requireBulk(t, gc.s, results, 100_000)
		peer.event(10*time.Second, isEvent("bulk"))
		assert.Equal(t, CodeResumeNotFound, resumeWithToken(t, addr, t1), "T1")
		assert.Equal(t, CodeResumeNotFound, resumeWithToken(t, addr, t3), "T3")
		// T2 still does.
		proxy.drop()
		waitFor(t, "resumed with T2", func() bool { return gc.resumed.Load() == 2 })
		peer.event(5*time.Second, isEvent("resumed"))
		requireBulk(t, gc.s, results, 100_000)
		peer.event(10*time.Second, isEvent("bulk"))
	})

	t.Run("channels opening across a resume", func(t *testing.T) {
		peer.clear()
		proxy := newLossyProxy(t, addr)
		results := make(chan error, 8)
		gc := newClientWithSession(t, peer, proxy, results)
		// The Go client opens a channel; the CHANNEL_ACK and the server's first
		// data are lost. The client lists the channel it has in Opening state
		// with count 0 (§7), and the server retransmits them.
		proxy.hold(down, true)
		opened := make(chan error, 1)
		go func() {
			ch, err := gc.s.OpenChannel(context.Background(), ChannelSpec{Name: "bulk:2000000", Reliable: true, Ordered: true})
			if err == nil {
				c, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				defer cancel()
				err = bulkExchange(c, ch)
			}
			opened <- err
		}()
		time.Sleep(200 * time.Millisecond)
		proxy.drop()
		proxy.hold(down, false)
		waitFor(t, "resumed", func() bool { return gc.resumed.Load() == 1 })
		require.NoError(t, <-opened)
		e := peer.event(10*time.Second, isEvent("bulk"))
		assert.Nil(t, e["error"], "libxumux: %v", e)

		// The libxumux server opens one; the Go client's CHANNEL_ACK and first
		// data are lost. The server lists its Opening channel in RESUMED.
		proxy.hold(up, true)
		reply := peer.start("open", map[string]any{"bytes": 2_000_000})
		time.Sleep(200 * time.Millisecond)
		proxy.drop()
		proxy.hold(up, false)
		waitFor(t, "resumed", func() bool { return gc.resumed.Load() == 2 })
		assert.Equal(t, true, await(t, reply, 20*time.Second)["ok"])
		require.NoError(t, <-results)

		// The Go client opens one and the OPEN_CHANNEL itself is lost: the
		// client lists a channel the server has never heard of, with count 0.
		proxy.hold(up, true)
		go func() {
			ch, err := gc.s.OpenChannel(context.Background(), ChannelSpec{Name: "bulk:2000000", Reliable: true, Ordered: true})
			if err == nil {
				c, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				defer cancel()
				err = bulkExchange(c, ch)
			}
			opened <- err
		}()
		time.Sleep(200 * time.Millisecond)
		proxy.drop()
		proxy.hold(up, false)
		waitFor(t, "resumed", func() bool { return gc.resumed.Load() == 3 })
		require.NoError(t, <-opened)
		e = peer.event(10*time.Second, isEvent("bulk"))
		assert.Nil(t, e["error"], "libxumux: %v", e)
	})

	t.Run("channel close across a resume", func(t *testing.T) {
		peer.clear()
		proxy := newLossyProxy(t, addr)
		results := make(chan error, 8)
		gc := newClientWithSession(t, peer, proxy, results)
		resumes := int32(0)
		lose := func(dir int, during func()) {
			t.Helper()
			proxy.hold(dir, true)
			during()
			time.Sleep(200 * time.Millisecond)
			proxy.drop()
			proxy.hold(dir, false)
			resumes++
			waitFor(t, "resumed", func() bool { return gc.resumed.Load() == resumes })
		}
		for _, dir := range []int{down, up} {
			// The Go client closes; the CLOSE_CHANNEL (up) or the answer
			// (down) is lost with the transport.
			ch, err := gc.s.OpenChannel(ctx(t), ChannelSpec{Name: "bulk:100000", Reliable: true, Ordered: true})
			require.NoError(t, err)
			require.NoError(t, bulkExchange(ctx(t), ch))
			peer.event(10*time.Second, isEvent("bulk"))
			closed := make(chan error, 1)
			lose(dir, func() { go func() { closed <- ch.Close(context.Background(), CodeNormal, "bye") }() })
			require.NoError(t, <-closed)
			assert.Equal(t, CodeNormal, ch.CloseInfo().Code)

			// The libxumux server closes.
			m := peer.call("open", map[string]any{"bytes": 100_000, "keep": true})
			require.NoError(t, <-results)
			var reply <-chan map[string]any
			lose(dir, func() { reply = peer.start("closech", map[string]any{"handle": m["handle"]}) })
			r := await(t, reply, 10*time.Second)
			assert.Equal(t, true, r["ok"], "%v", r)
			assert.Equal(t, "closed", r["state"])
		}
		// IDs are reused, and the session carries on.
		requireBulk(t, gc.s, results, 100_000)
		peer.event(10*time.Second, isEvent("bulk"))
		assert.Equal(t, int32(4), gc.resumed.Load())
	})

	t.Run("refused resume falls back to HELLO", func(t *testing.T) {
		peer.clear()
		proxy := newLossyProxy(t, addr)
		results := make(chan error, 8)
		gc := newClientWithSession(t, peer, proxy, results)
		gc.s.mu.Lock()
		gc.s.resume.token = bogusToken()
		gc.s.mu.Unlock()
		proxy.drop()
		var ns *Session
		select {
		case ns = <-gc.fallback:
		case <-time.After(10 * time.Second):
			t.Fatal("no fallback")
		}
		require.NotNil(t, ns)
		assert.Equal(t, CloseError{Code: CodeResumeNotFound, Reason: "unknown session", Remote: true}, gc.s.CloseInfo())
		peer.event(5*time.Second, isEvent("session"))
		requireBulk(t, ns, results, 100_000)
		peer.event(10*time.Second, isEvent("bulk"))
		c, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = ns.Close(c, CodeNormal, "")
	})

	t.Run("expiry", func(t *testing.T) {
		peer.clear()
		proxy := newLossyProxy(t, shortAddr)
		results := make(chan error, 8)
		gc := dialGoClient(t, proxy, results)
		peer.event(5*time.Second, isEvent("session", "timeout", 2.0))
		tok := gc.token()
		proxy.setRefuse(true)
		proxy.drop()
		// The client stops trying after resumeTimeout (§6), and the server
		// discards the session.
		select {
		case <-gc.s.Done():
		case <-time.After(6 * time.Second):
			t.Fatal("client did not give up")
		}
		assert.True(t, gc.s.CloseInfo().Lost)
		peer.event(6*time.Second, isEvent("closed", "timeout", 2.0))
		// A RESUME after that is told the session expired.
		assert.Equal(t, CodeResumeExpired, resumeWithToken(t, shortAddr, tok))
	})
}

// newClientWithSession connects a Go client through proxy and waits for the
// libxumux server to report the session.
func newClientWithSession(t *testing.T, peer *jsPeer, proxy *lossyProxy, results chan<- error, hello ...ChannelSpec) *goResumeClient {
	t.Helper()
	gc := dialGoClient(t, proxy, results, hello...)
	e := peer.event(5*time.Second, isEvent("session"))
	assert.Equal(t, []any{ExtensionSessionResume}, e["extensions"])
	return gc
}

// waitProgress waits until prog reaches n bytes.
func waitProgress(t *testing.T, prog *atomic.Int64, n int64) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for prog.Load() < n {
		require.True(t, time.Now().Before(deadline), "transfer stalled at %d bytes", prog.Load())
		time.Sleep(50 * time.Microsecond)
	}
}

// requireMidTransfer requires that a transfer of total bytes is under way, so
// that a drop now interrupts it.
func requireMidTransfer(t *testing.T, prog *atomic.Int64, total int64) {
	t.Helper()
	n := prog.Load()
	require.True(t, n > 0 && n < total, "drop at %d of %d bytes received: not mid-transfer", n, total)
	t.Logf("drop at %d of %d bytes received", n, total)
}

// requireBulk runs a bulk exchange of n bytes on a new channel of s.
func requireBulk(t *testing.T, s *Session, results <-chan error, n int) {
	t.Helper()
	ch, err := s.OpenChannel(ctx(t), ChannelSpec{Name: fmt.Sprintf("bulk:%d", n), Reliable: true, Ordered: true})
	require.NoError(t, err)
	c, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	require.NoError(t, bulkExchange(c, ch))
}

// ─── libxumux client ↔ Go server ───────────────────────────────────────────

type goResumeServer struct {
	addr     string
	rs       *ResumeServer
	log      *tokenLog
	sessions chan *Session
	handled  atomic.Int32
	prog     atomic.Int64 // bytes received on bulk channels the client opened
}

func serveGo(t *testing.T, timeout time.Duration, results chan error) *goResumeServer {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	gs := &goResumeServer{
		addr: ln.Addr().String(), rs: NewResumeServer(ResumeServerOptions{Timeout: timeout}),
		log: &tokenLog{}, sessions: make(chan *Session, 16),
	}
	var live []*Session
	var mu sync.Mutex
	t.Cleanup(func() {
		ln.Close()
		c, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		mu.Lock()
		defer mu.Unlock()
		for _, s := range live {
			_ = s.Close(c, CodeNormal, "")
		}
	})
	go func() {
		for {
			nc, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				s, _, err := Accept(context.Background(), sniffConn{NewStreamConn(nc), gs.log}, ServerConfig{Resume: gs.rs}, bulkOptsProgress(results, &gs.prog))
				switch {
				case err == nil:
					mu.Lock()
					live = append(live, s)
					mu.Unlock()
					gs.sessions <- s
				case errors.Is(err, ErrFirstFrameHandled):
					gs.handled.Add(1)
				}
			}()
		}
	}()
	return gs
}

func (gs *goResumeServer) session(t *testing.T) *Session {
	t.Helper()
	select {
	case s := <-gs.sessions:
		return s
	case <-time.After(5 * time.Second):
		t.Fatal("no server session")
		return nil
	}
}

func TestInteropResumeGoServer(t *testing.T) {
	peer := spawnResumePeer(t)
	results := make(chan error, 16)
	gs := serveGo(t, 30*time.Second, results)

	connect := func(t *testing.T, proxy *lossyProxy, gs *goResumeServer, hello ...map[string]any) *Session {
		t.Helper()
		proxy.onDrop = func(port int) { peer.start("drop", map[string]any{"port": port}) }
		m := peer.call("connect", map[string]any{"port": proxy.port(), "channels": hello})
		assert.Equal(t, []any{ExtensionSessionResume}, m["extensions"])
		return gs.session(t)
	}
	waitResumed := func(t *testing.T) {
		t.Helper()
		peer.event(10*time.Second, isEvent("resumed"))
	}
	bulkOK := func(t *testing.T, m map[string]any) {
		t.Helper()
		assert.Equal(t, true, m["ok"], "libxumux bulk: %v", m)
	}

	t.Run("transfer", func(t *testing.T) {
		peer.clear()
		proxy := newLossyProxy(t, gs.addr)
		gs.prog.Store(0)
		s := connect(t, proxy, gs, map[string]any{"name": "bulk:8000000", "reliable": true, "ordered": true})
		prog := &gs.prog
		var pending []<-chan map[string]any
		// Requested properties do not matter on TCP (§4): all are replayed.
		for _, x := range []map[string]any{
			{"bytes": 16_000_000},
			{"bytes": 12_000_000, "reliable": false, "ordered": false},
			{"bytes": 8_000_000, "reliable": true, "ordered": false},
		} {
			pending = append(pending, peer.start("bulk", x))
		}
		// And one opened by the Go server.
		sch, err := s.OpenChannel(ctx(t), ChannelSpec{Name: "bulk:8000000", Reliable: false, Ordered: false})
		require.NoError(t, err)
		assert.Equal(t, Properties{Reliable: true, Ordered: true}, sch.Properties())
		goDone := make(chan error, 1)
		go func() {
			c, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			goDone <- bulkProgress(c, sch, prog)
		}()
		const drops, total = 5, 52_000_000
		for i := 0; i < drops; i++ {
			waitProgress(t, prog, int64(i+1)*total/(drops+1))
			requireMidTransfer(t, prog, total)
			proxy.lose(i)
			waitResumed(t)
		}
		for _, p := range pending {
			bulkOK(t, await(t, p, 60*time.Second))
		}
		for range len(pending) + 1 {
			select {
			case err := <-results:
				require.NoError(t, err)
			case <-time.After(30 * time.Second):
				t.Fatal("Go side of a bulk exchange did not finish")
			}
		}
		require.NoError(t, <-goDone)
		for range 2 { // the HELLO channel and the one the Go server opened
			e := peer.event(10*time.Second, isEvent("bulk"))
			assert.Nil(t, e["error"], "libxumux: %v", e)
		}
		assert.Equal(t, int32(drops), gs.handled.Load())
		assert.False(t, s.Detached())
	})

	t.Run("token rotation across a resume", func(t *testing.T) {
		peer.clear()
		proxy := newLossyProxy(t, gs.addr)
		s := connect(t, proxy, gs)
		t1 := peer.call("token", nil)["token"].(string)
		// Rotate while the server's bytes are held back: RESUME_TOKEN T2 is lost
		// with the transport.
		proxy.hold(down, true)
		s.mu.Lock()
		s.resume.sendTokenLocked()
		t2 := s.resume.tokens[len(s.resume.tokens)-1].tok
		s.mu.Unlock()
		time.Sleep(200 * time.Millisecond)
		before := len(gs.log.all())
		proxy.drop()
		proxy.hold(down, false)
		waitResumed(t)
		var t3 string
		for _, x := range gs.log.all()[before:] {
			if x.typ == TypeResumed {
				t3 = x.tok
			}
		}
		require.NotEmpty(t, t3)
		// The client receives RESUMED (T3), then the retransmitted T2.
		waitFor(t, "client holds T2", func() bool { return peer.call("token", nil)["token"] == t2 })
		bulkOK(t, peer.call("bulk", map[string]any{"bytes": 100_000}))
		require.NoError(t, <-results)
		waitFor(t, "server holds only T2", func() bool {
			s.mu.Lock()
			defer s.mu.Unlock()
			return len(s.resume.tokens) == 1 && s.resume.tokens[0].tok == t2
		})
		assert.Equal(t, CodeResumeNotFound, resumeWithToken(t, gs.addr, t1), "T1")
		assert.Equal(t, CodeResumeNotFound, resumeWithToken(t, gs.addr, t3), "T3")
		proxy.drop()
		waitResumed(t)
		bulkOK(t, peer.call("bulk", map[string]any{"bytes": 100_000}))
		require.NoError(t, <-results)
	})

	t.Run("channels opening across a resume", func(t *testing.T) {
		peer.clear()
		proxy := newLossyProxy(t, gs.addr)
		s := connect(t, proxy, gs)
		// The libxumux client opens a channel; the CHANNEL_ACK and the Go
		// server's first data are lost.
		proxy.hold(down, true)
		reply := peer.start("bulk", map[string]any{"bytes": 2_000_000})
		time.Sleep(200 * time.Millisecond)
		proxy.drop()
		proxy.hold(down, false)
		waitResumed(t)
		bulkOK(t, await(t, reply, 20*time.Second))
		require.NoError(t, <-results)

		// The Go server opens one; the libxumux client's CHANNEL_ACK and first
		// data are lost.
		proxy.hold(up, true)
		opened := make(chan error, 1)
		go func() {
			ch, err := s.OpenChannel(context.Background(), ChannelSpec{Name: "bulk:2000000", Reliable: true, Ordered: true})
			if err == nil {
				c, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				defer cancel()
				err = bulkExchange(c, ch)
			}
			opened <- err
		}()
		time.Sleep(200 * time.Millisecond)
		proxy.drop()
		proxy.hold(up, false)
		waitResumed(t)
		require.NoError(t, <-opened)
		e := peer.event(10*time.Second, isEvent("bulk"))
		assert.Nil(t, e["error"], "libxumux: %v", e)

		// The libxumux client opens one and the OPEN_CHANNEL itself is lost:
		// it lists a channel the Go server has never heard of, with count 0.
		proxy.hold(up, true)
		reply = peer.start("bulk", map[string]any{"bytes": 2_000_000})
		time.Sleep(200 * time.Millisecond)
		proxy.drop()
		proxy.hold(up, false)
		waitResumed(t)
		bulkOK(t, await(t, reply, 20*time.Second))
		require.NoError(t, <-results)
	})

	t.Run("channel close across a resume", func(t *testing.T) {
		peer.clear()
		proxy := newLossyProxy(t, gs.addr)
		s := connect(t, proxy, gs)
		lose := func(dir int, during func()) {
			t.Helper()
			proxy.hold(dir, true)
			during()
			time.Sleep(200 * time.Millisecond)
			proxy.drop()
			proxy.hold(dir, false)
			waitResumed(t)
		}
		for _, dir := range []int{down, up} {
			// The Go server closes; its CLOSE_CHANNEL (down) or the answer (up)
			// is lost with the transport.
			ch, err := s.OpenChannel(ctx(t), ChannelSpec{Name: "bulk:100000", Reliable: true, Ordered: true})
			require.NoError(t, err)
			require.NoError(t, bulkExchange(ctx(t), ch))
			peer.event(10*time.Second, isEvent("bulk"))
			closed := make(chan error, 1)
			lose(dir, func() { go func() { closed <- ch.Close(context.Background(), CodeNormal, "bye") }() })
			require.NoError(t, <-closed)
			assert.Equal(t, CodeNormal, ch.CloseInfo().Code)

			// The libxumux client closes.
			m := peer.call("bulk", map[string]any{"bytes": 100_000, "keep": true})
			require.NoError(t, <-results)
			var reply <-chan map[string]any
			lose(dir, func() { reply = peer.start("closech", map[string]any{"handle": m["handle"]}) })
			r := await(t, reply, 10*time.Second)
			assert.Equal(t, true, r["ok"], "%v", r)
			assert.Equal(t, "closed", r["state"])
		}
		bulkOK(t, peer.call("bulk", map[string]any{"bytes": 100_000}))
		require.NoError(t, <-results)
	})

	t.Run("refused resume falls back to HELLO", func(t *testing.T) {
		peer.clear()
		proxy := newLossyProxy(t, gs.addr)
		connect(t, proxy, gs)
		peer.call("settoken", map[string]any{"token": bogusToken()})
		proxy.drop()
		e := peer.event(10*time.Second, isEvent("lost"))
		assert.Equal(t, float64(CodeResumeNotFound), e["code"])
		e = peer.event(10*time.Second, isEvent("fallback"))
		assert.Equal(t, []any{ExtensionSessionResume}, e["extensions"], "%v", e)
		gs.session(t)
		bulkOK(t, peer.call("bulk", map[string]any{"bytes": 100_000}))
		require.NoError(t, <-results)
	})

	t.Run("expiry", func(t *testing.T) {
		peer.clear()
		short := serveGo(t, 2*time.Second, results)
		proxy := newLossyProxy(t, short.addr)
		s := connect(t, proxy, short)
		tok := peer.call("token", nil)["token"].(string)
		peer.call("autoresume", map[string]any{"on": false})
		defer peer.call("autoresume", map[string]any{"on": true})
		proxy.drop()
		peer.event(5*time.Second, isEvent("detached"))
		// Both sides give up after resumeTimeout.
		select {
		case <-s.Done():
		case <-time.After(6 * time.Second):
			t.Fatal("server kept the session")
		}
		assert.True(t, s.CloseInfo().Lost)
		peer.event(6*time.Second, isEvent("closed"))
		assert.Equal(t, CodeResumeExpired, resumeWithToken(t, short.addr, tok))
		assert.Equal(t, 0, short.rs.Detached())
	})
}
