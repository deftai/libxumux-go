package xumux

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ─── extensionParams (core §3.8) ────────────────────────────────────────────

func TestExtensionParamsNegotiated(t *testing.T) {
	a, b := net.Pipe()
	var seen []string
	type acc struct {
		s   *Session
		h   *Hello
		err error
	}
	accepted := make(chan acc, 1)
	go func() {
		s, h, err := Accept(context.Background(), NewStreamConn(b), ServerConfig{
			Extensions: []string{"a/1", "b/1", "refused/1"},
			NegotiateExtension: func(_ context.Context, name string, p json.RawMessage) (json.RawMessage, error) {
				seen = append(seen, name+"="+string(p))
				switch name {
				case "a/1":
					return json.RawMessage(`{"level":2}`), nil
				case "refused/1":
					return nil, errors.New("cannot accept these parameters")
				}
				return nil, nil
			},
		}, testOpts)
		accepted <- acc{s, h, err}
	}()
	var checked []string
	cs, res, err := Connect(context.Background(), NewStreamConn(a), ClientConfig{
		Extensions: []string{"a/1", "b/1", "refused/1", "unknown/1"},
		ExtensionParams: map[string]json.RawMessage{
			"a/1":        json.RawMessage(`{"level":3}`),
			"refused/1":  json.RawMessage(`{"x":1}`),
			"unknown/1":  json.RawMessage(`{"y":1}`),
			"unlisted/1": json.RawMessage(`true`),
		},
		CheckExtensionParams: func(name string, p json.RawMessage) error {
			checked = append(checked, name+"="+string(p))
			return nil
		},
	}, testOpts)
	require.NoError(t, err)
	r := <-accepted
	require.NoError(t, r.err)

	// The server is asked about the extensions it supports, with the client's params.
	assert.Equal(t, []string{`a/1={"level":3}`, "b/1=", `refused/1={"x":1}`}, seen)
	assert.Equal(t, []string{"a/1", "b/1"}, res.Welcome.Extensions)
	assert.Equal(t, map[string]json.RawMessage{"a/1": json.RawMessage(`{"level":2}`)}, res.Welcome.ExtensionParams)
	assert.Equal(t, []string{`a/1={"level":2}`, "b/1="}, checked)

	for _, s := range []*Session{cs, r.s} {
		assert.Equal(t, []string{"a/1", "b/1"}, s.Extensions())
		l, p := s.ExtensionParams("refused/1")
		assert.Nil(t, l)
		assert.Nil(t, p)
		l, p = s.ExtensionParams("unknown/1")
		assert.Nil(t, l)
		assert.Nil(t, p)
	}
	l, p := cs.ExtensionParams("a/1")
	assert.JSONEq(t, `{"level":3}`, string(l))
	assert.JSONEq(t, `{"level":2}`, string(p))
	l, p = r.s.ExtensionParams("a/1")
	assert.JSONEq(t, `{"level":2}`, string(l))
	assert.JSONEq(t, `{"level":3}`, string(p))
	l, p = r.s.ExtensionParams("b/1")
	assert.Nil(t, l)
	assert.Nil(t, p)

	require.NoError(t, cs.Close(context.Background(), CodeNormal, ""))
}

func TestServerSendsNoParamsForUnacceptedExtensions(t *testing.T) {
	lib, raw := rawPair(t, "stream")
	go Accept(context.Background(), lib, ServerConfig{
		Extensions: []string{"a/1"},
		NegotiateExtension: func(_ context.Context, name string, _ json.RawMessage) (json.RawMessage, error) {
			return json.RawMessage(`{"for":"` + name + `"}`), nil
		},
	}, testOpts)
	w := jsonOf(t, raw.helloAndWelcome(map[string]any{
		"extensions":      []string{"a/1", "b/1"},
		"extensionParams": map[string]any{"a/1": 1, "b/1": 2, "c/1": 3},
	}))
	assert.Equal(t, []any{"a/1"}, w["extensions"])
	assert.Equal(t, map[string]any{"a/1": map[string]any{"for": "a/1"}}, w["extensionParams"])
}

func TestClientSendsParamsOnlyForRequested(t *testing.T) {
	lib, raw := rawPair(t, "stream")
	go Connect(context.Background(), lib, ClientConfig{
		Extensions:      []string{"a/1"},
		ExtensionParams: map[string]json.RawMessage{"a/1": json.RawMessage(`1`), "secret/1": json.RawMessage(`2`)},
	}, testOpts)
	h := jsonOf(t, raw.control(TypeHello))
	assert.Equal(t, map[string]any{"a/1": float64(1)}, h["extensionParams"])
}

func TestServerWithoutNegotiateSendsNoParams(t *testing.T) {
	lib, raw := rawPair(t, "stream")
	go Accept(context.Background(), lib, ServerConfig{Extensions: []string{"a/1"}}, testOpts)
	w := jsonOf(t, raw.helloAndWelcome(map[string]any{
		"extensions":      []string{"a/1", "a/1"},
		"extensionParams": map[string]any{"a/1": map[string]any{"level": 3}},
	}))
	assert.Equal(t, []any{"a/1"}, w["extensions"], "a repeated request is accepted once")
	assert.NotContains(t, w, "extensionParams")
}

func TestExtensionParamsNotObject(t *testing.T) {
	for name, v := range map[string]any{"array": []string{"x"}, "string": "x", "number": 3} {
		t.Run(name, func(t *testing.T) {
			lib, raw := rawPair(t, "stream")
			errc := make(chan error, 1)
			go func() { _, _, err := Accept(context.Background(), lib, ServerConfig{}, testOpts); errc <- err }()
			raw.send(ctrl(TypeHello, map[string]any{"version": Version, "extensionParams": v}))
			assert.Equal(t, CodeInvalidMessage, raw.closeCode())
			assert.Error(t, <-errc)
		})
	}
}

func TestClientClosesOnInvalidWelcomeParams(t *testing.T) {
	lib, raw := rawPair(t, "stream")
	errc := make(chan error, 1)
	go func() {
		_, _, err := Connect(context.Background(), lib, ClientConfig{
			Extensions: []string{"a/1", "b/1"},
			CheckExtensionParams: func(name string, p json.RawMessage) error {
				if name == "a/1" && string(p) != `{"ok":true}` {
					return errors.New("bad a/1 params")
				}
				return nil
			},
		}, testOpts)
		errc <- err
	}()
	raw.awaitHelloSendWelcome(map[string]any{"extensions": []string{"a/1"}, "extensionParams": map[string]any{"a/1": 7}})
	assert.Equal(t, CodeInvalidMessage, raw.closeCode())
	err := <-errc
	var pe *ProtocolError
	require.ErrorAs(t, err, &pe)
	assert.Equal(t, CodeInvalidMessage, pe.Code)
}

func TestClientIgnoresWelcomeParamsForUnaccepted(t *testing.T) {
	var checked []string
	s, res, _ := connectRaw(t, testOpts, ClientConfig{
		Extensions: []string{"a/1", "b/1"},
		CheckExtensionParams: func(name string, _ json.RawMessage) error {
			checked = append(checked, name)
			return nil
		},
	}, map[string]any{"extensions": []string{"a/1"}, "extensionParams": map[string]any{"b/1": "bad", "z/1": 1}})
	assert.Equal(t, []string{"a/1"}, checked)
	assert.Equal(t, []string{"a/1"}, s.Extensions())
	_, p := s.ExtensionParams("b/1")
	assert.Nil(t, p)
	assert.Contains(t, res.Welcome.ExtensionParams, "b/1", "the raw WELCOME is reported as received")
}

func TestHelloTooLarge(t *testing.T) {
	lib, raw := rawPair(t, "stream")
	big := json.RawMessage(`"` + strings.Repeat("x", ControlMaxPayload) + `"`)
	_, _, err := Connect(context.Background(), lib, ClientConfig{
		Extensions: []string{"a/1"}, ExtensionParams: map[string]json.RawMessage{"a/1": big},
	}, testOpts)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "limit")
	raw.waitEnded()
	assert.Empty(t, raw.frames, "nothing is sent")
}

func TestHelloInvalidParamsJSON(t *testing.T) {
	lib, _ := rawPair(t, "stream")
	_, _, err := Connect(context.Background(), lib, ClientConfig{
		Extensions: []string{"a/1"}, ExtensionParams: map[string]json.RawMessage{"a/1": json.RawMessage(`{`)},
	}, testOpts)
	assert.Error(t, err)
}

func TestWelcomeTooLarge(t *testing.T) {
	lib, raw := rawPair(t, "stream")
	errc := make(chan error, 1)
	go func() {
		_, _, err := Accept(context.Background(), lib, ServerConfig{
			Extensions: []string{"a/1"},
			NegotiateExtension: func(context.Context, string, json.RawMessage) (json.RawMessage, error) {
				return json.RawMessage(`"` + strings.Repeat("x", ControlMaxPayload) + `"`), nil
			},
		}, acceptAll(nil))
		errc <- err
	}()
	raw.send(ctrl(TypeHello, map[string]any{"version": Version, "extensions": []string{"a/1"},
		"channels": []any{map[string]any{"id": 1, "name": "x", "reliable": true, "ordered": true}}}))
	assert.Equal(t, CodeGoingAway, raw.closeCode())
	raw.send(ctrl(TypeClose, closeMsg{Code: CodeGoingAway}))
	assert.Error(t, <-errc)
}

// ─── Extension-defined first frames (core §3.1) ─────────────────────────────

const typeTestResume byte = 0x30

func TestFirstFrameHandled(t *testing.T) {
	lib, raw := rawPair(t, "stream")
	type res struct {
		s   *Session
		err error
	}
	done := make(chan res, 1)
	go func() {
		s, _, err := Accept(context.Background(), lib, ServerConfig{FirstFrames: map[byte]FirstFrameHandler{
			typeTestResume: func(ctx context.Context, conn FrameConn, f Frame) error {
				if _, ok := ctx.Deadline(); !ok {
					return errors.New("no deadline")
				}
				// Answer in place of WELCOME, then keep the transport.
				return conn.WriteFrame(Frame{Channel: ControlChannel, Type: 0x31, Payload: f.Payload})
			},
		}}, testOpts)
		done <- res{s, err}
	}()
	raw.send(Frame{Channel: ControlChannel, Type: typeTestResume, Payload: []byte(`{"token":"t"}`)})
	answer := raw.next(nil)
	assert.Equal(t, byte(0x31), answer.Type)
	assert.Equal(t, `{"token":"t"}`, string(answer.Payload))
	r := <-done
	assert.Nil(t, r.s)
	assert.ErrorIs(t, r.err, ErrFirstFrameHandled)
}

func TestFirstFrameRefused(t *testing.T) {
	lib, raw := rawPair(t, "stream")
	errc := make(chan error, 1)
	go func() {
		_, _, err := Accept(context.Background(), lib, ServerConfig{FirstFrames: map[byte]FirstFrameHandler{
			typeTestResume: func(context.Context, FrameConn, Frame) error {
				return &Refusal{Code: 4100, Reason: "unknown session"}
			},
		}}, testOpts)
		errc <- err
	}()
	raw.send(Frame{Channel: ControlChannel, Type: typeTestResume, Payload: []byte(`{}`)})
	assert.Equal(t, 4100, raw.closeCode())
	raw.send(ctrl(TypeClose, closeMsg{Code: 4100}))
	var pe *ProtocolError
	require.ErrorAs(t, <-errc, &pe)
	assert.Equal(t, 4100, pe.Code)
}

func TestFirstFrameHandlerError(t *testing.T) {
	lib, raw := rawPair(t, "stream")
	boom := errors.New("boom")
	errc := make(chan error, 1)
	go func() {
		_, _, err := Accept(context.Background(), lib, ServerConfig{FirstFrames: map[byte]FirstFrameHandler{
			typeTestResume: func(context.Context, FrameConn, Frame) error { return boom },
		}}, testOpts)
		errc <- err
	}()
	raw.send(Frame{Channel: ControlChannel, Type: typeTestResume, Payload: []byte(`{}`)})
	assert.ErrorIs(t, <-errc, boom)
	raw.waitEnded()
	assert.Empty(t, raw.frames)
}

func TestFirstFrameUnclaimed(t *testing.T) {
	handlers := map[byte]FirstFrameHandler{
		typeTestResume: func(context.Context, FrameConn, Frame) error { return nil },
	}
	for name, f := range map[string]Frame{
		"no handler for the type": {Channel: ControlChannel, Type: 0x32, Payload: []byte(`{}`)},
		"probe of a claimed type": {Channel: ControlChannel, Type: typeTestResume, Flags: FlagProbe, Payload: []byte{0, 0, 0, 1}},
		"claimed type on channel": {Channel: 1, Type: typeTestResume, Payload: []byte(`{}`)},
	} {
		t.Run(name, func(t *testing.T) {
			lib, raw := rawPair(t, "stream")
			go Accept(context.Background(), lib, ServerConfig{FirstFrames: handlers}, testOpts)
			raw.send(f)
			assert.Equal(t, CodeProtocolError, raw.closeCode())
		})
	}
}

func TestFirstFramesTypeRange(t *testing.T) {
	for _, typ := range []byte{TypeHello, 0x2F, 0x80} {
		lib, raw := rawPair(t, "stream")
		_, _, err := Accept(context.Background(), lib, ServerConfig{FirstFrames: map[byte]FirstFrameHandler{
			typ: func(context.Context, FrameConn, Frame) error { return nil },
		}}, testOpts)
		assert.Error(t, err, "%#x", typ)
		raw.waitEnded()
	}
}

func TestFirstFrameHandlerDeadline(t *testing.T) {
	cases := map[string]FirstFrameHandler{
		// Honours ctx.
		"returns at ctx expiry": func(ctx context.Context, _ FrameConn, _ Frame) error {
			<-ctx.Done()
			return ctx.Err()
		},
		// Ignores ctx and blocks on the transport: closing it unblocks the handler.
		"blocked on the transport": func(_ context.Context, conn FrameConn, _ Frame) error {
			_, _ = conn.ReadFrame(MaxWindow)
			return nil
		},
		// Returns the transport's error once the deadline closes it.
		"returns the transport error": func(_ context.Context, conn FrameConn, _ Frame) error {
			_, err := conn.ReadFrame(MaxWindow)
			return err
		},
	}
	for name, h := range cases {
		t.Run(name, func(t *testing.T) {
			lib, raw := rawPair(t, "stream")
			o := testOpts
			o.HandshakeTimeout = 200 * time.Millisecond
			errc := make(chan error, 1)
			go func() {
				_, _, err := Accept(context.Background(), lib, ServerConfig{FirstFrames: map[byte]FirstFrameHandler{typeTestResume: h}}, o)
				errc <- err
			}()
			raw.send(Frame{Channel: ControlChannel, Type: typeTestResume, Payload: []byte(`{}`)})
			select {
			case err := <-errc:
				assert.ErrorIs(t, err, context.DeadlineExceeded)
				assert.NotErrorIs(t, err, ErrFirstFrameHandled)
			case <-time.After(3 * time.Second):
				t.Fatal("Accept outlived the handshake timeout")
			}
			raw.waitEnded()
		})
	}
}
