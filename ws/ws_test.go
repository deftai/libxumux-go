package ws

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	xumux "github.com/deftai/libxumux-go"
)

func testCtx(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func wsURL(srv *httptest.Server) string { return "ws" + strings.TrimPrefix(srv.URL, "http") }

func echo(ctx context.Context) xumux.Acceptor {
	return func(context.Context, xumux.ChannelRequest) (xumux.Handler, error) {
		return func(ch *xumux.Channel) {
			for {
				m, err := ch.Receive(ctx)
				if err != nil {
					return
				}
				_ = ch.Send(ctx, m.Type, m.Payload)
			}
		}, nil
	}
}

// echoServer serves xumux echo sessions through Handler and reports how
// each server session ended.
func echoServer(t *testing.T, opts *AcceptOptions) (*httptest.Server, <-chan xumux.CloseError) {
	ended := make(chan xumux.CloseError, 4)
	srv := httptest.NewServer(Handler(opts, func(fc xumux.FrameConn, r *http.Request) {
		s, _, err := xumux.Accept(r.Context(), fc, xumux.ServerConfig{}, xumux.Options{Acceptor: echo(context.Background())})
		if err != nil {
			ended <- xumux.CloseError{Reason: err.Error()}
			return
		}
		<-s.Done()
		ended <- s.CloseInfo()
	}))
	t.Cleanup(srv.Close)
	return srv, ended
}

func TestSessionOverWebSocket(t *testing.T) {
	ctx := testCtx(t)
	srv, ended := echoServer(t, nil)
	fc, resp, err := Dial(ctx, wsURL(srv)+"/xumux", nil)
	require.NoError(t, err)
	assert.Equal(t, Subprotocol, resp.Header.Get("Sec-WebSocket-Protocol"))
	s, _, err := xumux.Connect(ctx, fc, xumux.ClientConfig{Application: "ws-test/1"}, xumux.Options{})
	require.NoError(t, err)

	ch, err := s.OpenChannel(ctx, xumux.ChannelSpec{Name: "echo", Reliable: true, Ordered: true})
	require.NoError(t, err)
	// Large enough to fragment into several frames, and so several messages.
	big := []byte(strings.Repeat("xumux ", 30_000))
	for _, p := range [][]byte{[]byte("hello"), big} {
		require.NoError(t, ch.Send(ctx, 0x01, p))
		m, err := ch.Receive(ctx)
		require.NoError(t, err)
		assert.Equal(t, p, m.Payload)
	}
	_, err = s.Ping(ctx)
	require.NoError(t, err)

	require.NoError(t, s.Close(ctx, xumux.CodeGoingAway, "bye"))
	select {
	case ce := <-ended:
		assert.Equal(t, xumux.CodeGoingAway, ce.Code, "the xumux CLOSE code reaches the server")
	case <-ctx.Done():
		t.Fatal("server session did not end")
	}
}

// rawServer is a WebSocket server that speaks the subprotocol (when
// subprotocols is non-nil) but not xumux, for checking what Dial's
// FrameConn sends and does.
func rawServer(t *testing.T, subprotocols []string, serve func(c *websocket.Conn)) *httptest.Server {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: subprotocols})
		if err != nil {
			return
		}
		defer c.CloseNow()
		serve(c)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// closeStatus reads until the connection ends and returns its close code.
// It gives up after 5 seconds (-1).
func closeStatus(c *websocket.Conn) websocket.StatusCode {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for {
		if _, _, err := c.Read(ctx); err != nil {
			return websocket.CloseStatus(err)
		}
	}
}

func closeFrame(code int) xumux.Frame {
	b, _ := json.Marshal(map[string]int{"code": code})
	return xumux.Frame{Channel: xumux.ControlChannel, Type: xumux.TypeClose, Payload: b}
}

func TestCloseCodeSent(t *testing.T) {
	for _, tc := range []struct {
		name string
		sent []xumux.Frame
		want websocket.StatusCode
	}{
		{"no CLOSE", nil, websocket.StatusNormalClosure},
		{"CLOSE 1000", []xumux.Frame{closeFrame(xumux.CodeNormal)}, websocket.StatusNormalClosure},
		{"CLOSE 4008", []xumux.Frame{closeFrame(xumux.CodeKeepaliveTimeout)}, websocket.StatusNormalClosure},
		{"CLOSE 1001", []xumux.Frame{closeFrame(xumux.CodeGoingAway)}, websocket.StatusGoingAway},
		{"probe is not CLOSE", []xumux.Frame{{Channel: 0, Type: xumux.TypeClose, Flags: xumux.FlagProbe, Payload: []byte(`{"code":1001}`)}}, websocket.StatusNormalClosure},
		{"data channel is not CLOSE", []xumux.Frame{{Channel: 1, Type: xumux.TypeClose, Payload: []byte(`{"code":1001}`)}}, websocket.StatusNormalClosure},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := make(chan websocket.StatusCode, 1)
			srv := rawServer(t, []string{Subprotocol}, func(c *websocket.Conn) {
				for range tc.sent {
					typ, _, err := c.Read(context.Background())
					if err != nil || typ != websocket.MessageBinary {
						got <- -2
						return
					}
				}
				got <- closeStatus(c)
			})
			fc, _, err := Dial(testCtx(t), wsURL(srv), nil)
			require.NoError(t, err)
			for _, f := range tc.sent {
				require.NoError(t, fc.WriteFrame(f))
			}
			require.NoError(t, fc.Close())
			assert.Equal(t, tc.want, <-got)
		})
	}
}

func TestCloseCodeReceived(t *testing.T) {
	for _, tc := range []struct {
		code  websocket.StatusCode
		clean bool
	}{
		{websocket.StatusNormalClosure, true},
		{websocket.StatusGoingAway, true},
		{websocket.StatusInternalError, false},
		{websocket.StatusPolicyViolation, false},
	} {
		t.Run(tc.code.String(), func(t *testing.T) {
			srv := rawServer(t, []string{Subprotocol}, func(c *websocket.Conn) { c.Close(tc.code, "") })
			fc, _, err := Dial(testCtx(t), wsURL(srv), nil)
			require.NoError(t, err)
			defer fc.Close()
			_, err = fc.ReadFrame(xumux.DefaultMaxFramePayload)
			if tc.clean {
				assert.ErrorIs(t, err, io.EOF)
			} else {
				require.Error(t, err)
				assert.NotErrorIs(t, err, io.EOF)
				assert.Equal(t, tc.code, websocket.CloseStatus(err), "the close code is kept: %v", err)
			}
		})
	}
}

func TestTextMessageRejectedByClient(t *testing.T) {
	got := make(chan websocket.StatusCode, 1)
	srv := rawServer(t, []string{Subprotocol}, func(c *websocket.Conn) {
		c.Write(context.Background(), websocket.MessageText, []byte(`{"hello":1}`))
		got <- closeStatus(c)
	})
	fc, _, err := Dial(testCtx(t), wsURL(srv), nil)
	require.NoError(t, err)
	_, err = fc.ReadFrame(xumux.DefaultMaxFramePayload)
	var pe *xumux.ProtocolError
	require.True(t, errors.As(err, &pe), "%v", err)
	assert.Equal(t, xumux.CodeProtocolError, pe.Code)
	require.NoError(t, fc.Close())
	assert.Equal(t, websocket.StatusProtocolError, <-got)
}

// A text message to a server session gets xumux CLOSE 1002, then a
// WebSocket close with 1002.
func TestTextMessageRejectedByServer(t *testing.T) {
	ctx := testCtx(t)
	srv, ended := echoServer(t, nil)
	c, _, err := websocket.Dial(ctx, wsURL(srv), &websocket.DialOptions{Subprotocols: []string{Subprotocol}})
	require.NoError(t, err)
	defer c.CloseNow()
	require.NoError(t, c.Write(ctx, websocket.MessageText, []byte("HELLO")))

	typ, b, err := c.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, websocket.MessageBinary, typ)
	f, err := xumux.DecodeFrame(b, xumux.DefaultMaxFramePayload)
	require.NoError(t, err)
	require.True(t, f.IsControl() && f.Type == xumux.TypeClose, "%+v", f)
	var m struct{ Code int }
	require.NoError(t, json.Unmarshal(f.Payload, &m))
	assert.Equal(t, xumux.CodeProtocolError, m.Code)

	// Answer the CLOSE, as a peer should; the WebSocket then closes with 1002.
	require.NoError(t, c.Write(ctx, websocket.MessageBinary, closeFrame(xumux.CodeProtocolError).Encode()))
	assert.Equal(t, websocket.StatusProtocolError, closeStatus(c))
	<-ended
}

func TestBadFrames(t *testing.T) {
	short := closeFrame(1000).Encode()
	short = short[:len(short)-1] // header length says one byte more
	long := append(closeFrame(1000).Encode(), 0)
	for name, msg := range map[string][]byte{
		"truncated": short, "trailing bytes": long, "short header": {0, 0, 0x20},
	} {
		t.Run(name, func(t *testing.T) {
			srv := rawServer(t, []string{Subprotocol}, func(c *websocket.Conn) {
				c.Write(context.Background(), websocket.MessageBinary, msg)
				closeStatus(c)
			})
			fc, _, err := Dial(testCtx(t), wsURL(srv), nil)
			require.NoError(t, err)
			defer fc.Close()
			_, err = fc.ReadFrame(xumux.DefaultMaxFramePayload)
			var pe *xumux.ProtocolError
			assert.True(t, errors.As(err, &pe), "message length must equal 8 + payload length: %v", err)
		})
	}
}

func TestReadLimit(t *testing.T) {
	payload := make([]byte, 2000)
	srv := rawServer(t, []string{Subprotocol}, func(c *websocket.Conn) {
		f := xumux.Frame{Channel: 1, Type: 0x01, Payload: payload}
		c.Write(context.Background(), websocket.MessageBinary, f.Encode())
		closeStatus(c)
	})
	fc, _, err := Dial(testCtx(t), wsURL(srv), &DialOptions{MaxFramePayload: 1024})
	require.NoError(t, err)
	defer fc.Close()
	// The WebSocket read limit allows channel 0's 64 KiB, so this frame is
	// read whole and refused by the frame check; an oversized message is
	// refused by the WebSocket library before it is buffered.
	_, err = fc.ReadFrame(1024)
	require.Error(t, err)

	// Channel 0 may always carry 64 KiB, whatever MaxFramePayload says, so
	// the read limit must allow it (the library's own default is 32 KiB).
	ctl := rawServer(t, []string{Subprotocol}, func(c *websocket.Conn) {
		f := xumux.Frame{Channel: xumux.ControlChannel, Type: 0x7f, Payload: make([]byte, xumux.ControlMaxPayload)}
		c.Write(context.Background(), websocket.MessageBinary, f.Encode())
		closeStatus(c)
	})
	fc1, _, err := Dial(testCtx(t), wsURL(ctl), &DialOptions{MaxFramePayload: 1024})
	require.NoError(t, err)
	defer fc1.Close()
	f, err := fc1.ReadFrame(1024)
	require.NoError(t, err)
	assert.Len(t, f.Payload, xumux.ControlMaxPayload)

	big := rawServer(t, []string{Subprotocol}, func(c *websocket.Conn) {
		c.Write(context.Background(), websocket.MessageBinary, make([]byte, xumux.ControlMaxPayload+xumux.HeaderSize+1))
		closeStatus(c)
	})
	fc2, _, err := Dial(testCtx(t), wsURL(big), &DialOptions{MaxFramePayload: 1024})
	require.NoError(t, err)
	defer fc2.Close()
	_, err = fc2.ReadFrame(1024)
	assert.ErrorContains(t, err, "message too big")
}

func TestServerRequiresSubprotocol(t *testing.T) {
	ctx := testCtx(t)
	called := make(chan struct{}, 1)
	srv := httptest.NewServer(Handler(nil, func(xumux.FrameConn, *http.Request) { called <- struct{}{} }))
	defer srv.Close()
	c, _, err := websocket.Dial(ctx, wsURL(srv), nil)
	require.NoError(t, err)
	defer c.CloseNow()
	_, _, err = c.Read(ctx)
	assert.Equal(t, websocket.StatusPolicyViolation, websocket.CloseStatus(err))
	select {
	case <-called:
		t.Fatal("handler ran without the xumux subprotocol")
	default:
	}

	// Offering it alongside others is fine.
	c, _, err = websocket.Dial(ctx, wsURL(srv), &websocket.DialOptions{Subprotocols: []string{"chat", Subprotocol}})
	require.NoError(t, err)
	defer c.CloseNow()
	assert.Equal(t, Subprotocol, c.Subprotocol())
	select {
	case <-called:
	case <-ctx.Done():
		t.Fatal("handler did not run")
	}
}

func TestClientRequiresSubprotocol(t *testing.T) {
	got := make(chan websocket.StatusCode, 1)
	srv := rawServer(t, nil, func(c *websocket.Conn) { got <- closeStatus(c) })
	fc, _, err := Dial(testCtx(t), wsURL(srv), nil)
	assert.ErrorIs(t, err, ErrSubprotocol)
	assert.Nil(t, fc)
	assert.Equal(t, websocket.StatusProtocolError, <-got)
}

func TestAcceptRefusesForeignOrigin(t *testing.T) {
	srv, _ := echoServer(t, nil)
	h := http.Header{"Origin": {"https://evil.example"}}
	_, resp, err := Dial(testCtx(t), wsURL(srv), &DialOptions{HTTPHeader: h})
	require.Error(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, http.StatusForbidden, resp.StatusCode)

	srv2, _ := echoServer(t, &AcceptOptions{OriginPatterns: []string{"evil.example"}})
	fc, _, err := Dial(testCtx(t), wsURL(srv2), &DialOptions{HTTPHeader: h})
	require.NoError(t, err)
	fc.Close()
}

func TestHandlerClosesWhenHandleReturns(t *testing.T) {
	ctx := testCtx(t)
	srv := httptest.NewServer(Handler(nil, func(xumux.FrameConn, *http.Request) {}))
	defer srv.Close()
	c, _, err := websocket.Dial(ctx, wsURL(srv), &websocket.DialOptions{Subprotocols: []string{Subprotocol}})
	require.NoError(t, err)
	defer c.CloseNow()
	assert.Equal(t, websocket.StatusNormalClosure, closeStatus(c))
}
