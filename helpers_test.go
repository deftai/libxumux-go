package xumux

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// memTransport is one end of an in-memory message transport (WebSocket-like).
type memTransport struct {
	in     chan []byte
	out    chan []byte
	closed chan struct{}
	peer   *memTransport
	once   sync.Once
}

func memMessagePair() (*memTransport, *memTransport) {
	a2b, b2a := make(chan []byte, 1024), make(chan []byte, 1024)
	a := &memTransport{in: b2a, out: a2b, closed: make(chan struct{})}
	b := &memTransport{in: a2b, out: b2a, closed: make(chan struct{})}
	a.peer, b.peer = b, a
	return a, b
}

func (t *memTransport) ReadMessage() ([]byte, error) {
	select {
	case m := <-t.in:
		if m == nil {
			return nil, ErrTextMessage
		}
		return m, nil
	case <-t.closed:
		return nil, io.EOF
	case <-t.peer.closed:
		select {
		case m := <-t.in:
			return m, nil
		default:
			return nil, io.EOF
		}
	}
}

func (t *memTransport) WriteMessage(b []byte) error {
	select {
	case <-t.closed:
		return net.ErrClosed
	case <-t.peer.closed:
		return net.ErrClosed
	case t.out <- append([]byte(nil), b...):
		return nil
	}
}

func (t *memTransport) Close() error {
	t.once.Do(func() { close(t.closed) })
	return nil
}

// rawPeer drives a session through exact, possibly invalid, byte sequences.
type rawPeer struct {
	t      *testing.T
	conn   net.Conn      // stream kind
	msg    *memTransport // message kind
	fc     FrameConn
	frames chan Frame
	ended  chan struct{}
}

// rawPair returns a FrameConn for the library and a raw peer on the other end.
func rawPair(t *testing.T, kind string) (FrameConn, *rawPeer) {
	t.Helper()
	r := &rawPeer{t: t, frames: make(chan Frame, 1024), ended: make(chan struct{})}
	var lib FrameConn
	if kind == "message" {
		a, b := memMessagePair()
		lib, r.msg, r.fc = NewMessageConn(a), b, NewMessageConn(b)
	} else {
		a, b := net.Pipe()
		lib, r.conn, r.fc = NewStreamConn(a), b, NewStreamConn(b)
	}
	go func() {
		defer close(r.ended)
		for {
			f, err := r.fc.ReadFrame(MaxWindow)
			if err != nil {
				return
			}
			r.frames <- f
		}
	}()
	t.Cleanup(func() { r.fc.Close() })
	return lib, r
}

func (r *rawPeer) send(f Frame) {
	r.t.Helper()
	require.NoError(r.t, r.fc.WriteFrame(f))
}

// sendBytes writes raw bytes. On streams magic is sent first unless noMagic.
func (r *rawPeer) sendBytes(b []byte, noMagic bool) {
	r.t.Helper()
	if r.msg != nil {
		require.NoError(r.t, r.msg.WriteMessage(b))
		return
	}
	if !noMagic {
		if sc := r.fc.(*streamConn); !sc.sentMagic {
			sc.sentMagic = true
			b = append(Magic[:], b...)
		}
	}
	go r.conn.Write(b) // net.Pipe is synchronous
}

func (r *rawPeer) next(pred func(Frame) bool) Frame {
	r.t.Helper()
	timeout := time.After(3 * time.Second)
	for {
		select {
		case f := <-r.frames:
			if pred == nil || pred(f) {
				return f
			}
		case <-r.ended:
			select {
			case f := <-r.frames:
				if pred == nil || pred(f) {
					return f
				}
				continue
			default:
			}
			r.t.Fatal("transport ended before a matching frame arrived")
		case <-timeout:
			r.t.Fatal("timed out waiting for a frame")
		}
	}
}

func (r *rawPeer) control(typ byte) Frame {
	r.t.Helper()
	return r.next(func(f Frame) bool { return f.IsControl() && f.Type == typ })
}

func (r *rawPeer) closeCode() int {
	r.t.Helper()
	f := r.control(TypeClose)
	var m closeMsg
	require.NoError(r.t, json.Unmarshal(f.Payload, &m))
	return m.Code
}

func (r *rawPeer) waitEnded() {
	r.t.Helper()
	select {
	case <-r.ended:
	case <-time.After(3 * time.Second):
		r.t.Fatal("transport did not end")
	}
}

func ctrl(typ byte, v any) Frame { return controlFrame(typ, v) }

func jsonOf(t *testing.T, f Frame) map[string]any {
	t.Helper()
	var m map[string]any
	require.NoError(t, json.Unmarshal(f.Payload, &m))
	return m
}

// helloAndWelcome acts as a client: send HELLO, return WELCOME.
func (r *rawPeer) helloAndWelcome(extra map[string]any) Frame {
	h := map[string]any{"version": []int{0, 2, 0}}
	for k, v := range extra {
		h[k] = v
	}
	r.send(ctrl(TypeHello, h))
	return r.control(TypeWelcome)
}

// awaitHelloSendWelcome acts as a server and accepts every HELLO channel.
func (r *rawPeer) awaitHelloSendWelcome(extra map[string]any) map[string]any {
	hello := jsonOf(r.t, r.control(TypeHello))
	var chans []map[string]any
	if cs, ok := hello["channels"].([]any); ok {
		for _, c := range cs {
			chans = append(chans, map[string]any{"id": c.(map[string]any)["id"], "reliable": true, "ordered": true})
		}
	}
	w := map[string]any{"version": []int{0, 2, 0}, "channels": chans}
	for k, v := range extra {
		w[k] = v
	}
	r.send(ctrl(TypeWelcome, w))
	return hello
}

func fromHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(strings.ReplaceAll(s, " ", ""))
	require.NoError(t, err)
	return b
}

var testOpts = Options{PingInterval: -1, CloseTimeout: 300 * time.Millisecond}

func acceptAll(handler Handler) Options {
	o := testOpts
	o.Acceptor = func(context.Context, ChannelRequest) (Handler, error) {
		if handler == nil {
			return func(*Channel) {}, nil
		}
		return handler, nil
	}
	return o
}

// pair connects a client and server over net.Pipe.
func pair(t *testing.T, copts, sopts Options, specs ...ChannelSpec) (*Session, *Session, *ConnectResult) {
	t.Helper()
	a, b := net.Pipe()
	type acc struct {
		s   *Session
		err error
	}
	ch := make(chan acc, 1)
	go func() {
		s, _, err := Accept(context.Background(), NewStreamConn(b), ServerConfig{}, sopts)
		ch <- acc{s, err}
	}()
	c, res, err := Connect(context.Background(), NewStreamConn(a), ClientConfig{Channels: specs}, copts)
	require.NoError(t, err)
	sv := <-ch
	require.NoError(t, sv.err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = c.Close(ctx, CodeNormal, "")
		_ = sv.s.Close(ctx, CodeNormal, "")
	})
	return c, sv.s, res
}

func ctx(t *testing.T) context.Context {
	c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return c
}

func isClose(err error, code int) bool {
	var ce *CloseError
	return errors.As(err, &ce) && ce.Code == code
}
