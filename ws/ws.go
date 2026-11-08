// Package ws carries xumux over WebSocket, per the xumux WebSocket binding
// (spec/bindings/websocket.md):
//
//   - The subprotocol is "xumux". Dial fails, and Accept closes the
//     connection with 1008, if the other side does not agree to it.
//   - Each binary message is exactly one xumux frame, with no magic bytes.
//   - A text message is a protocol error: the session sends xumux CLOSE 1002
//     and the WebSocket is closed with 1002.
//   - After the xumux CLOSE exchange the WebSocket is closed with 1000, or
//     1001 if the xumux CLOSE sent was 1001 (going away). A peer's 1000 or
//     1001 close reads as a clean end of the transport (io.EOF).
//
// Dial and Accept return an [xumux.FrameConn] for [xumux.Connect] and
// [xumux.Accept]:
//
//	fc, _, err := ws.Dial(ctx, "wss://example.com/xumux", nil)
//	s, res, err := xumux.Connect(ctx, fc, cfg, xumux.Options{})
//
//	http.Handle("/xumux", ws.Handler(nil, func(fc xumux.FrameConn, r *http.Request) {
//		s, hello, err := xumux.Accept(r.Context(), fc, cfg, xumux.Options{})
//		...
//	}))
//
// This package is a separate module, so that the core module stays free of
// dependencies. It uses github.com/coder/websocket.
package ws

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/coder/websocket"

	xumux "github.com/deftai/libxumux-go"
)

// Subprotocol is the WebSocket subprotocol for xumux.
const Subprotocol = "xumux"

// ErrSubprotocol is returned by Dial when the server does not select the
// xumux subprotocol, and by Accept when the client does not offer it.
var ErrSubprotocol = errors.New("ws: the peer did not agree to the xumux WebSocket subprotocol")

// DialOptions configure Dial. The zero value is usable.
type DialOptions struct {
	// HTTPClient makes the upgrade request, for example with a custom
	// TLS configuration. Its Timeout must be zero. Default:
	// http.DefaultClient.
	HTTPClient *http.Client
	// HTTPHeader is added to the upgrade request (cookies, Authorization).
	HTTPHeader http.Header
	// MaxFramePayload is the largest frame payload this side accepts. Set it
	// to the xumux.Options Limits.MaxFramePayload passed to the session.
	// Default: xumux.DefaultMaxFramePayload.
	MaxFramePayload uint32
}

// AcceptOptions configure Accept and Handler. The zero value is usable.
type AcceptOptions struct {
	// OriginPatterns lists the hosts, besides the request's own, whose
	// browser Origin is allowed (see websocket.AcceptOptions). By default
	// cross-origin requests are refused with 403.
	OriginPatterns []string
	// MaxFramePayload is as in DialOptions.
	MaxFramePayload uint32
}

// Dial opens a WebSocket to url (ws:// or wss://) with the xumux
// subprotocol. The response is the server's upgrade response, if one was
// received, so callers can inspect a refused upgrade (401, 403…).
func Dial(ctx context.Context, url string, opts *DialOptions) (xumux.FrameConn, *http.Response, error) {
	if opts == nil {
		opts = &DialOptions{}
	}
	c, resp, err := websocket.Dial(ctx, url, &websocket.DialOptions{
		HTTPClient:   opts.HTTPClient,
		HTTPHeader:   opts.HTTPHeader,
		Subprotocols: []string{Subprotocol},
	})
	if err != nil {
		return nil, resp, err
	}
	if c.Subprotocol() != Subprotocol {
		c.Close(websocket.StatusProtocolError, "subprotocol xumux required")
		return nil, resp, ErrSubprotocol
	}
	return newConn(c, opts.MaxFramePayload), resp, nil
}

// Accept upgrades an HTTP request to a WebSocket with the xumux
// subprotocol. If it fails, it has already written an HTTP error response
// or, when the client did not offer the subprotocol, closed the WebSocket
// with 1008 (so browsers see a close code and reason) and returned
// ErrSubprotocol.
//
// The returned FrameConn outlives the request's handler; close it when
// done. Handler does that for you.
func Accept(w http.ResponseWriter, r *http.Request, opts *AcceptOptions) (xumux.FrameConn, error) {
	if opts == nil {
		opts = &AcceptOptions{}
	}
	c, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		Subprotocols:   []string{Subprotocol},
		OriginPatterns: opts.OriginPatterns,
	})
	if err != nil {
		return nil, err
	}
	if c.Subprotocol() != Subprotocol {
		c.Close(websocket.StatusPolicyViolation, "subprotocol xumux required")
		return nil, ErrSubprotocol
	}
	return newConn(c, opts.MaxFramePayload), nil
}

// Handler returns an http.Handler that accepts xumux WebSocket connections
// and calls handle for each, then closes the FrameConn when handle returns.
// handle typically runs xumux.Accept and serves the session until it ends.
// Requests that cannot be upgraded never reach handle.
func Handler(opts *AcceptOptions, handle func(fc xumux.FrameConn, r *http.Request)) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fc, err := Accept(w, r, opts)
		if err != nil {
			return
		}
		defer fc.Close()
		handle(fc, r)
	})
}

func newConn(c *websocket.Conn, maxFramePayload uint32) xumux.FrameConn {
	if maxFramePayload == 0 {
		maxFramePayload = xumux.DefaultMaxFramePayload
	}
	// A message is one frame: header plus the largest payload either
	// channel 0 (always up to 64 KiB) or a data channel may carry.
	c.SetReadLimit(int64(max(maxFramePayload, xumux.ControlMaxPayload)) + xumux.HeaderSize)
	return xumux.NewMessageConn(&transport{c: c})
}

// transport is the xumux.MessageTransport over one WebSocket.
type transport struct {
	c *websocket.Conn
	// closeCode is the WebSocket close code Close will send.
	closeCode atomic.Int32
	once      sync.Once
	closeErr  error
}

func (t *transport) ReadMessage() ([]byte, error) {
	typ, b, err := t.c.Read(context.Background())
	if err != nil {
		switch websocket.CloseStatus(err) {
		case websocket.StatusNormalClosure, websocket.StatusGoingAway:
			return nil, io.EOF
		}
		return nil, err
	}
	if typ != websocket.MessageBinary {
		t.closeCode.CompareAndSwap(0, int32(websocket.StatusProtocolError))
		return nil, xumux.ErrTextMessage
	}
	return b, nil
}

func (t *transport) WriteMessage(b []byte) error {
	if code, ok := sentCloseCode(b); ok {
		t.closeCode.CompareAndSwap(0, int32(code))
	}
	return t.c.Write(context.Background(), websocket.MessageBinary, b)
}

func (t *transport) Close() error {
	t.once.Do(func() {
		code, reason := websocket.StatusNormalClosure, ""
		switch websocket.StatusCode(t.closeCode.Load()) {
		case websocket.StatusGoingAway:
			code = websocket.StatusGoingAway
		case websocket.StatusProtocolError:
			code, reason = websocket.StatusProtocolError, "binary messages only"
		}
		t.closeErr = t.c.Close(code, reason)
		var ce websocket.CloseError
		if errors.As(t.closeErr, &ce) && (ce.Code == websocket.StatusNormalClosure || ce.Code == websocket.StatusGoingAway) {
			t.closeErr = nil // the peer closed first, cleanly
		}
	})
	return t.closeErr
}

// sentCloseCode reports the code of an outgoing xumux CLOSE frame. Only
// 1001 (going away) changes the WebSocket close code.
func sentCloseCode(b []byte) (websocket.StatusCode, bool) {
	if len(b) < xumux.HeaderSize || !bytes.Equal(b[:2], []byte{0, 0}) || b[2] != xumux.TypeClose || b[3]&xumux.FlagProbe != 0 {
		return 0, false
	}
	var m struct {
		Code int `json:"code"`
	}
	if json.Unmarshal(b[xumux.HeaderSize:], &m) != nil || m.Code != xumux.CodeGoingAway {
		return 0, false
	}
	return websocket.StatusGoingAway, true
}
