package pipe_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/nettest"

	xumux "github.com/deftai/libxumux-go"
	"github.com/deftai/libxumux-go/pipe"
)

var opts = xumux.Options{PingInterval: -1, CloseTimeout: 300 * time.Millisecond}

// sessions connects a client to a server that sends peer-opened channels
// to l.
func sessions(t testing.TB, l *pipe.Listener) (client, server *xumux.Session) {
	t.Helper()
	a, b := net.Pipe()
	sopts := opts
	sopts.Acceptor = l.Acceptor()
	type res struct {
		s   *xumux.Session
		err error
	}
	done := make(chan res, 1)
	go func() {
		s, _, err := xumux.Accept(context.Background(), xumux.NewStreamConn(b), xumux.ServerConfig{}, sopts)
		done <- res{s, err}
	}()
	c, _, err := xumux.Connect(context.Background(), xumux.NewStreamConn(a), xumux.ClientConfig{}, opts)
	require.NoError(t, err)
	r := <-done
	require.NoError(t, r.err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = c.Close(ctx, xumux.CodeNormal, "")
		_ = r.s.Close(ctx, xumux.CodeNormal, "")
	})
	return c, r.s
}

func ctx(t testing.TB) context.Context {
	c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return c
}

// connPair returns both ends of one pipe.
func connPair(t testing.TB) (dialed, accepted *pipe.Conn) {
	t.Helper()
	l := pipe.NewListener()
	t.Cleanup(func() { l.Close() })
	client, _ := sessions(t, l)
	c1, err := pipe.Dial(ctx(t), client, "test", "")
	require.NoError(t, err)
	c2, err := l.AcceptPipe(ctx(t))
	require.NoError(t, err)
	return c1, c2
}

// The standard library's conformance suite for net.Conn implementations:
// I/O, concurrent use, deadlines (past, present, future, changed while
// blocked) and Close unblocking pending calls.
func TestNetConnConformance(t *testing.T) {
	nettest.TestConn(t, func() (c1, c2 net.Conn, stop func(), err error) {
		a, b := connPair(t)
		return a, b, func() { a.Close(); b.Close() }, nil
	})
}

var _ interface {
	net.Conn
	CloseWrite() error
} = (*pipe.Conn)(nil)

var _ net.Listener = (*pipe.Listener)(nil)

func TestHalfClose(t *testing.T) {
	a, b := connPair(t)
	_, err := io.WriteString(a, "ping")
	require.NoError(t, err)
	require.NoError(t, a.CloseWrite())
	_, err = a.Write([]byte("x"))
	assert.ErrorIs(t, err, pipe.ErrWriteClosed)

	got, err := io.ReadAll(b)
	require.NoError(t, err)
	assert.Equal(t, "ping", string(got))

	// b can still answer after a's EOF.
	_, err = io.WriteString(b, "pong")
	require.NoError(t, err)
	require.NoError(t, b.CloseWrite())
	got, err = io.ReadAll(a)
	require.NoError(t, err)
	assert.Equal(t, "pong", string(got))

	// Both EOFs exchanged: the channel closes itself with 1000 (pipe/1).
	for _, c := range []*pipe.Conn{a, b} {
		select {
		case <-c.Channel().Done():
			assert.Equal(t, xumux.CodeNormal, c.Channel().CloseInfo().Code)
		case <-time.After(2 * time.Second):
			t.Fatal("channel did not close after both EOFs")
		}
	}
}

// Credit is returned only as Read hands bytes over, so a reader that stops
// reading stops the writer at the window.
func TestBackpressure(t *testing.T) {
	a, b := connPair(t)
	require.NoError(t, a.SetWriteDeadline(time.Now().Add(200*time.Millisecond)))
	n, err := a.Write(make([]byte, 4<<20))
	var ne net.Error
	require.True(t, errors.As(err, &ne) && ne.Timeout(), "want a timeout, got %v", err)
	assert.ErrorIs(t, err, os.ErrDeadlineExceeded)
	assert.Equal(t, xumux.DefaultInitialWindow, n, "exactly the peer's window")

	// Reading frees credit, and the writer can go on.
	require.NoError(t, a.SetWriteDeadline(time.Now().Add(5*time.Second)))
	go func() { _, _ = io.Copy(io.Discard, b) }()
	_, err = a.Write(make([]byte, 1<<20))
	require.NoError(t, err)
}

func TestReset(t *testing.T) {
	a, b := connPair(t)
	_, err := io.WriteString(a, "partial")
	require.NoError(t, err)
	require.NoError(t, a.Reset(xumux.CodeGoingAway, "upstream died"))

	got, err := io.ReadAll(b)
	assert.Equal(t, "partial", string(got), "data before the RESET is still delivered")
	var re *pipe.ResetError
	require.ErrorAs(t, err, &re)
	assert.Equal(t, &pipe.ResetError{Code: xumux.CodeGoingAway, Reason: "upstream died"}, re)
	_, err = a.Write([]byte("x"))
	assert.ErrorIs(t, err, net.ErrClosed)
}

// A message type pipe/1 doesn't define closes the channel with 1002.
func TestUnknownMessageType(t *testing.T) {
	l := pipe.NewListener()
	defer l.Close()
	client, _ := sessions(t, l)
	ch, err := client.OpenChannel(ctx(t), pipe.Spec("raw", ""))
	require.NoError(t, err)
	b, err := l.AcceptPipe(ctx(t))
	require.NoError(t, err)
	require.NoError(t, ch.Send(ctx(t), 0x09, []byte("?")))
	_, err = b.Read(make([]byte, 8))
	assert.ErrorContains(t, err, "unknown pipe/1 message type 0x9")
	<-ch.Done()
	assert.Equal(t, xumux.CodeProtocolError, ch.CloseInfo().Code)
}

func TestCloseSendsEOFAndClosesTheChannel(t *testing.T) {
	a, b := connPair(t)
	_, err := io.WriteString(a, "bye")
	require.NoError(t, err)
	require.NoError(t, a.Close())
	got, err := io.ReadAll(b)
	require.NoError(t, err, "the peer sees a clean EOF")
	assert.Equal(t, "bye", string(got))
	assert.Equal(t, xumux.CodeNormal, a.Channel().CloseInfo().Code)
	_, err = a.Read(make([]byte, 1))
	assert.ErrorIs(t, err, net.ErrClosed)
	assert.NoError(t, a.Close(), "a second Close is harmless")
}

func TestListener(t *testing.T) {
	l := pipe.NewListener()
	l.Check = func(_ context.Context, req xumux.ChannelRequest) error {
		if pipe.Target(req) != "tcp:allowed:1" {
			return &xumux.Refusal{Code: xumux.CodeChannelRefused, Reason: "target not allowed"}
		}
		return nil
	}
	client, _ := sessions(t, l)

	_, err := pipe.Dial(ctx(t), client, "x", "tcp:elsewhere:1")
	var ce *xumux.CloseError
	require.ErrorAs(t, err, &ce)
	assert.Equal(t, xumux.CodeChannelRefused, ce.Code)

	_, err = client.OpenChannel(ctx(t), xumux.ChannelSpec{Name: "x", Reliable: true, Ordered: true, Metadata: map[string]any{"profile": "other/1"}})
	require.ErrorAs(t, err, &ce)
	assert.Contains(t, ce.Reason, "unsupported profile")

	c, err := pipe.Dial(ctx(t), client, "x", "tcp:allowed:1")
	require.NoError(t, err)
	s, err := l.Accept()
	require.NoError(t, err)
	assert.Equal(t, "tcp:allowed:1", s.(*pipe.Conn).Target())
	assert.Equal(t, "xumux", s.RemoteAddr().Network())
	c.Close()

	require.NoError(t, l.Close())
	_, err = l.Accept()
	assert.ErrorIs(t, err, net.ErrClosed)
}

// net/http runs unchanged over pipes: the server on a Listener, the client
// dialing a pipe per connection.
func TestHTTPOverPipes(t *testing.T) {
	l := pipe.NewListener()
	client, _ := sessions(t, l)
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "hello %s", strings.TrimPrefix(r.URL.Path, "/"))
	})}
	go func() { _ = srv.Serve(l) }()
	defer srv.Close()

	hc := &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return pipe.Dial(ctx, client, "http", "")
		},
	}}
	var wg sync.WaitGroup
	for i := range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := hc.Get(fmt.Sprintf("http://xumux/%d", i))
			if !assert.NoError(t, err) {
				return
			}
			defer resp.Body.Close()
			body, _ := io.ReadAll(resp.Body)
			assert.Equal(t, fmt.Sprintf("hello %d", i), string(body))
		}()
	}
	wg.Wait()
}

// A pipe that failed keeps its close code when the application closes it at
// once, as io.Copy-style code does: no EOF, and 1001 after a RESET or 1002
// after a protocol error, never 1000.
func TestCloseAfterFailureKeepsTheCode(t *testing.T) {
	for _, tc := range []struct {
		name  string
		typ   byte
		body  string
		code  int
		reset bool // the application answers with Reset rather than Close
	}{
		{"reset then Close", pipe.TypeReset, `{"code":1001}`, xumux.CodeGoingAway, false},
		{"unknown type then Close", 0x09, "?", xumux.CodeProtocolError, false},
		{"unknown type then Reset", 0x09, "?", xumux.CodeProtocolError, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			l := pipe.NewListener()
			defer l.Close()
			client, _ := sessions(t, l)
			ch, err := client.OpenChannel(ctx(t), pipe.Spec("raw", ""))
			require.NoError(t, err)
			b, err := l.AcceptPipe(ctx(t))
			require.NoError(t, err)
			require.NoError(t, ch.Send(ctx(t), tc.typ, []byte(tc.body)))
			_, err = b.Read(make([]byte, 8))
			require.Error(t, err)
			if tc.reset {
				_ = b.Reset(xumux.CodeGoingAway, "local failure")
			} else {
				_ = b.Close()
			}
			for {
				m, err := ch.Receive(ctx(t))
				if err != nil {
					break
				}
				assert.NotEqual(t, pipe.TypeEOF, m.Type, "no EOF from a failed pipe")
				assert.NotEqual(t, pipe.TypeReset, m.Type, "no RESET from a pipe that already failed")
			}
			<-ch.Done()
			assert.Equal(t, tc.code, ch.CloseInfo().Code)
		})
	}
}
