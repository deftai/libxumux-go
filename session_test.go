package xumux

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var data = ChannelSpec{Name: "data", Reliable: true, Ordered: true}

func connectRaw(t *testing.T, opts Options, cfg ClientConfig, welcome map[string]any) (*Session, *ConnectResult, *rawPeer) {
	t.Helper()
	lib, raw := rawPair(t, "stream")
	type res struct {
		s   *Session
		r   *ConnectResult
		err error
	}
	done := make(chan res, 1)
	go func() {
		s, r, err := Connect(context.Background(), lib, cfg, opts)
		done <- res{s, r, err}
	}()
	raw.awaitHelloSendWelcome(welcome)
	r := <-done
	require.NoError(t, r.err)
	return r.s, r.r, raw
}

func acceptRaw(t *testing.T, opts Options, hello map[string]any) (*Session, *rawPeer) {
	t.Helper()
	lib, raw := rawPair(t, "stream")
	done := make(chan *Session, 1)
	go func() {
		s, _, err := Accept(context.Background(), lib, ServerConfig{}, opts)
		assert.NoError(t, err)
		done <- s
	}()
	raw.helloAndWelcome(hello)
	return <-done, raw
}

// ─── Handshake ──────────────────────────────────────────────────────────────

func TestHandshakeOpensHelloChannels(t *testing.T) {
	refuseB := testOpts
	refuseB.Acceptor = func(_ context.Context, r ChannelRequest) (Handler, error) {
		if r.Name == "b" {
			return nil, &Refusal{Code: 4610, Reason: "no b"}
		}
		return func(*Channel) {}, nil
	}
	_, server, res := pair(t, testOpts, refuseB, ChannelSpec{Name: "a", Reliable: false}, ChannelSpec{Name: "b"}, ChannelSpec{Name: "c"})
	require.Len(t, res.Channels, 2)
	assert.Equal(t, []uint16{1, 5}, []uint16{res.Channels[0].ID(), res.Channels[1].ID()})
	assert.Equal(t, Properties{Reliable: true, Ordered: true}, res.Channels[0].Properties(), "single-stream bindings report reliable+ordered")
	assert.Equal(t, []ChannelRefusal{{ID: 3, Code: 4610, Reason: "no b"}}, res.Rejected)
	assert.Len(t, server.Channels(), 2)
}

func TestNoAcceptorRefusesWith1003(t *testing.T) {
	_, _, res := pair(t, testOpts, testOpts, data)
	assert.Equal(t, CodeUnsupported, res.Rejected[0].Code)
}

func TestAuthorizeRefusal(t *testing.T) {
	a, b := net.Pipe()
	go Accept(context.Background(), NewStreamConn(b), ServerConfig{Authorize: func(_ context.Context, h *Hello) error {
		if h.Auth["token"] == "good" {
			return nil
		}
		return errors.New("bad token")
	}}, testOpts)
	_, _, err := Connect(context.Background(), NewStreamConn(a), ClientConfig{Auth: map[string]any{"token": "bad"}}, testOpts)
	assert.True(t, isClose(err, CodeAuthFailed), "%v", err)
}

func TestVersionMismatch(t *testing.T) {
	lib, raw := rawPair(t, "stream")
	go Accept(context.Background(), lib, ServerConfig{}, testOpts)
	raw.send(ctrl(TypeHello, map[string]any{"version": []int{0, 1, 0}}))
	assert.Equal(t, CodeVersionMismatch, raw.closeCode())
}

func TestHelloTimeout(t *testing.T) {
	lib, raw := rawPair(t, "stream")
	o := testOpts
	o.HandshakeTimeout = 50 * time.Millisecond
	errc := make(chan error, 1)
	go func() { _, _, err := Accept(context.Background(), lib, ServerConfig{}, o); errc <- err }()
	assert.Equal(t, CodeHelloTimeout, raw.closeCode())
	assert.ErrorIs(t, <-errc, context.DeadlineExceeded)
}

func TestClientRejectsBadWelcome(t *testing.T) {
	cases := map[string]map[string]any{
		"unanswered channel":  {"version": []int{0, 2, 0}},
		"unrequested ext":     {"version": []int{0, 2, 0}, "extensions": []string{"x/1"}, "rejected": []any{map[string]any{"id": 1, "code": 4010}}},
		"incompatible":        {"version": []int{0, 3, 0}, "rejected": []any{map[string]any{"id": 1, "code": 4010}}},
		"duplicate answer":    {"version": []int{0, 2, 0}, "channels": []any{map[string]any{"id": 1, "reliable": true, "ordered": true}}, "rejected": []any{map[string]any{"id": 1, "code": 4010}}},
		"invalid limits 4001": {"version": []int{0, 2, 0}, "maxFramePayload": 100},
	}
	for name, w := range cases {
		t.Run(name, func(t *testing.T) {
			lib, raw := rawPair(t, "stream")
			errc := make(chan error, 1)
			go func() {
				_, _, err := Connect(context.Background(), lib, ClientConfig{Channels: []ChannelSpec{data}}, testOpts)
				errc <- err
			}()
			raw.control(TypeHello)
			raw.send(ctrl(TypeWelcome, w))
			code := raw.closeCode()
			if name == "invalid limits 4001" {
				assert.Equal(t, CodeInvalidMessage, code)
			} else {
				assert.Equal(t, CodeProtocolError, code)
			}
			assert.Error(t, <-errc)
		})
	}
}

func TestServerRefusalAsClose(t *testing.T) {
	lib, raw := rawPair(t, "stream")
	errc := make(chan error, 1)
	go func() { _, _, err := Connect(context.Background(), lib, ClientConfig{}, testOpts); errc <- err }()
	raw.control(TypeHello)
	raw.send(ctrl(TypeClose, map[string]any{"code": 4000, "reason": "nope"}))
	err := <-errc
	assert.True(t, isClose(err, 4000), "%v", err)
	assert.Equal(t, 4000, raw.closeCode(), "client replies with CLOSE")
}

// ─── Messages ───────────────────────────────────────────────────────────────

func TestMessagesAndFragmentation(t *testing.T) {
	got := make(chan Message, 3)
	_, _, res := pair(t, testOpts, acceptAll(func(ch *Channel) {
		for {
			m, err := ch.Receive(context.Background())
			if err != nil {
				return
			}
			got <- m
		}
	}), data)
	ch := res.Channels[0]
	big := bytes.Repeat([]byte{0xAB, 0xCD}, 100_000)
	require.NoError(t, ch.Send(ctx(t), 7, []byte("hello")))
	require.NoError(t, ch.Send(ctx(t), 8, big))
	require.NoError(t, ch.Send(ctx(t), 9, nil))
	assert.Equal(t, Message{Type: 7, Payload: []byte("hello")}, <-got)
	m := <-got
	assert.Equal(t, byte(8), m.Type)
	assert.Equal(t, big, m.Payload)
	assert.Equal(t, Message{Type: 9, Payload: []byte{}}, <-got)
}

func TestSendRejectsOversizedMessage(t *testing.T) {
	_, _, res := pair(t, testOpts, acceptAll(nil), data)
	err := res.Channels[0].Send(ctx(t), 1, make([]byte, DefaultMaxMessageSize+1))
	assert.ErrorContains(t, err, "maxMessageSize")
}

func TestRoundRobinBetweenChannels(t *testing.T) {
	var mu sync.Mutex
	var order []string
	handler := func(ch *Channel) {
		for {
			if _, err := ch.Receive(context.Background()); err != nil {
				return
			}
			mu.Lock()
			order = append(order, ch.Name())
			mu.Unlock()
		}
	}
	// A transport that takes 1ms per write, so the bulk transfer takes ~16ms.
	a, b := net.Pipe()
	go Accept(context.Background(), NewStreamConn(b), ServerConfig{}, acceptAll(handler))
	_, res, err := Connect(context.Background(), NewStreamConn(slowConn{a}), ClientConfig{Channels: []ChannelSpec{
		{Name: "bulk", Reliable: true, Ordered: true}, {Name: "small", Reliable: true, Ordered: true}}}, testOpts)
	require.NoError(t, err)
	bulk, small := res.Channels[0], res.Channels[1]
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		assert.NoError(t, bulk.Send(ctx(t), 1, make([]byte, 250_000)))
	}()
	time.Sleep(5 * time.Millisecond)
	require.NoError(t, small.Send(ctx(t), 1, []byte("x")))
	wg.Wait()
	require.Eventually(t, func() bool { mu.Lock(); defer mu.Unlock(); return len(order) == 2 }, time.Second, 5*time.Millisecond)
	assert.Equal(t, []string{"small", "bulk"}, order, "a small message is not stuck behind a bulk transfer")
}

type slowConn struct{ net.Conn }

func (c slowConn) Write(b []byte) (int, error) {
	time.Sleep(time.Millisecond)
	return c.Conn.Write(b)
}

// ─── Flow control ───────────────────────────────────────────────────────────

func TestFlowControlBlocksUntilRead(t *testing.T) {
	small := testOpts
	small.Limits = Limits{MaxFramePayload: 4096, MaxMessageSize: 4096, InitialWindow: 8192}
	accepted := make(chan *Channel, 1)
	small.Acceptor = func(context.Context, ChannelRequest) (Handler, error) {
		return func(ch *Channel) { accepted <- ch }, nil
	}
	_, _, res := pair(t, testOpts, small, data)
	ch := res.Channels[0]
	srv := <-accepted
	for i := 0; i < 2; i++ {
		require.NoError(t, ch.Send(ctx(t), 1, make([]byte, 4096)))
	}
	sent := make(chan error, 1)
	go func() { sent <- ch.Send(ctx(t), 1, make([]byte, 4096)) }()
	select {
	case <-sent:
		t.Fatal("send should wait for credit")
	case <-time.After(50 * time.Millisecond):
	}
	_, err := srv.Receive(ctx(t))
	require.NoError(t, err)
	require.NoError(t, <-sent)
}

func TestCreditViolationCloses4009(t *testing.T) {
	_, raw := acceptRaw(t, acceptAll(nil), map[string]any{"channels": []any{map[string]any{"id": 1, "name": "d", "reliable": true, "ordered": true}}})
	for i := 0; i < 5; i++ {
		raw.send(Frame{Channel: 1, Type: 1, Payload: make([]byte, 65536)})
	}
	assert.Equal(t, CodeFlowControlError, raw.closeCode())
}

func TestWindowUpdateAfterRead(t *testing.T) {
	client, res, raw := connectRaw(t, testOpts, ClientConfig{Channels: []ChannelSpec{data}}, nil)
	_ = client
	raw.send(Frame{Channel: 1, Type: 1, Payload: make([]byte, 1000)})
	_, err := res.Channels[0].Receive(ctx(t))
	require.NoError(t, err)
	wu := raw.control(TypeWindowUpdate)
	assert.Equal(t, []byte{0, 1, 0, 0, 0x03, 0xE8}, wu.Payload)
}

func TestManualCredit(t *testing.T) {
	_, res, raw := connectRaw(t, testOpts, ClientConfig{Channels: []ChannelSpec{data}}, nil)
	ch := res.Channels[0]
	ch.SetManualCredit(true)
	raw.send(Frame{Channel: 1, Type: 1, Payload: make([]byte, 10)})
	_, err := ch.Receive(ctx(t))
	require.NoError(t, err)
	ch.ReturnCredit(10)
	assert.Equal(t, []byte{0, 1, 0, 0, 0, 10}, raw.control(TypeWindowUpdate).Payload)
}

func TestWindowUpdateValidation(t *testing.T) {
	_, raw := acceptRaw(t, acceptAll(nil), map[string]any{"channels": []any{map[string]any{"id": 1, "name": "d", "reliable": true, "ordered": true}}})
	raw.send(windowUpdateFrame([]windowEntry{{1, MaxWindow}}))
	assert.Equal(t, CodeFlowControlError, raw.closeCode(), "credit above 2^31-1")
}

// ─── Probes ─────────────────────────────────────────────────────────────────

func TestPingOnConnectionAndChannel(t *testing.T) {
	client, _, res := pair(t, testOpts, acceptAll(nil), data)
	rtt, err := client.Ping(ctx(t))
	require.NoError(t, err)
	assert.Greater(t, rtt, time.Duration(0))
	_, err = res.Channels[0].Ping(ctx(t))
	require.NoError(t, err)
	st := res.Channels[0].Stats()
	assert.Equal(t, 1, st.ProbesSent)
	assert.NotZero(t, st.SRTT)
	assert.Equal(t, 1, client.Stats().ProbesSent)
}

func TestPongOnSameChannel(t *testing.T) {
	_, _, raw := connectRaw(t, testOpts, ClientConfig{Channels: []ChannelSpec{data}}, nil)
	raw.send(probeFrame(1, TypePing, 42))
	pong := raw.next(func(f Frame) bool { return f.IsProbe() })
	assert.Equal(t, probeFrame(1, TypePong, 42), pong)
	raw.send(Frame{Channel: 1, Type: TypePing, Flags: FlagProbe, Payload: []byte{1, 2, 3}})
	raw.send(probeFrame(0, TypePing, 7))
	assert.Equal(t, probeFrame(0, TypePong, 7), raw.next(func(f Frame) bool { return f.IsProbe() }), "malformed probe discarded")
}

func TestPingTimeoutCountsLoss(t *testing.T) {
	client, _, _ := connectRaw(t, testOpts, ClientConfig{}, nil)
	c, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_, err := client.Ping(c)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Equal(t, 1, client.Stats().ProbesLost)
}

func TestConcurrentPingsGetDistinctTokens(t *testing.T) {
	client, _, _ := pair(t, testOpts, testOpts)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := client.Ping(ctx(t))
			assert.NoError(t, err)
		}()
	}
	wg.Wait()
	assert.Zero(t, client.Stats().ProbesLost)
}

func TestKeepaliveTimeout(t *testing.T) {
	o := testOpts
	client, _, raw := connectRaw(t, o, ClientConfig{}, map[string]any{"pingInterval": 0.05, "pingTimeout": 0.05})
	ping := raw.next(func(f Frame) bool { return f.IsProbe() })
	assert.Equal(t, TypePing, ping.Type)
	select {
	case <-client.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("keepalive did not close the session")
	}
	assert.True(t, isClose(client.Err(), CodeKeepaliveTimeout), "%v", client.Err())
}

func TestKeepaliveAnswered(t *testing.T) {
	a, b := net.Pipe()
	so := testOpts
	so.PingInterval, so.PingTimeout = 40*time.Millisecond, 40*time.Millisecond
	go Accept(context.Background(), NewStreamConn(b), ServerConfig{}, so)
	client, _, err := Connect(context.Background(), NewStreamConn(a), ClientConfig{}, testOpts)
	require.NoError(t, err)
	time.Sleep(300 * time.Millisecond)
	select {
	case <-client.Done():
		t.Fatalf("session closed: %v", client.Err())
	default:
	}
}

// ─── Channels ───────────────────────────────────────────────────────────────

func TestPeerOpenedChannelsUseEvenIDs(t *testing.T) {
	got := make(chan *Channel, 1)
	client, server, _ := pair(t, acceptAll(func(ch *Channel) { got <- ch }), testOpts)
	_ = client
	ch, err := server.OpenChannel(ctx(t), ChannelSpec{Name: "srv", Reliable: true, Ordered: true, Metadata: map[string]any{"k": "v"}})
	require.NoError(t, err)
	assert.Equal(t, uint16(2), ch.ID())
	remote := <-got
	assert.Equal(t, "srv", remote.Name())
	assert.Equal(t, map[string]any{"k": "v"}, remote.Metadata())
	require.NoError(t, remote.Send(ctx(t), 3, []byte("hi")))
	m, err := ch.Receive(ctx(t))
	require.NoError(t, err)
	assert.Equal(t, "hi", string(m.Payload))
}

func TestOpenChannelRefused(t *testing.T) {
	o := testOpts
	o.Acceptor = func(context.Context, ChannelRequest) (Handler, error) { return nil, errors.New("not today") }
	client, _, _ := pair(t, testOpts, o)
	_, err := client.OpenChannel(ctx(t), data)
	assert.True(t, isClose(err, CodeChannelRefused), "%v", err)
}

func TestTwoStepCloseAndIDReuse(t *testing.T) {
	client, res, raw := connectRaw(t, testOpts, ClientConfig{Channels: []ChannelSpec{data}}, nil)
	ch := res.Channels[0]
	closed := make(chan error, 1)
	go func() { closed <- ch.Close(ctx(t), CodeNormal, "") }()
	assert.Equal(t, map[string]any{"id": float64(1)}, jsonOf(t, raw.control(TypeCloseChannel)))
	// ID 1 is still closing, so a new channel gets 3.
	opening := make(chan *Channel, 1)
	go func() {
		c, err := client.OpenChannel(ctx(t), data)
		assert.NoError(t, err)
		opening <- c
	}()
	assert.Equal(t, float64(3), jsonOf(t, raw.control(TypeOpenChannel))["id"])
	raw.send(ctrl(TypeChannelAck, map[string]any{"id": 3, "reliable": true, "ordered": true}))
	<-opening
	select {
	case <-closed:
		t.Fatal("close completed before the peer's CLOSE_CHANNEL")
	case <-time.After(20 * time.Millisecond):
	}
	raw.send(ctrl(TypeCloseChannel, map[string]any{"id": 1}))
	require.NoError(t, <-closed)
	_, err := ch.Receive(ctx(t))
	assert.ErrorIs(t, err, io.EOF)
	go client.OpenChannel(ctx(t), data)
	assert.Equal(t, float64(1), jsonOf(t, raw.control(TypeOpenChannel))["id"], "ID 1 is free again")
}

func TestRemoteChannelCloseCode(t *testing.T) {
	_, res, raw := connectRaw(t, testOpts, ClientConfig{Channels: []ChannelSpec{data}}, nil)
	raw.send(Frame{Channel: 1, Type: 1, Payload: []byte("last")})
	raw.send(ctrl(TypeCloseChannel, map[string]any{"id": 1, "code": 4601, "reason": "app"}))
	assert.Equal(t, map[string]any{"id": float64(1)}, jsonOf(t, raw.control(TypeCloseChannel)), "reply")
	ch := res.Channels[0]
	m, err := ch.Receive(ctx(t))
	require.NoError(t, err, "queued messages stay readable")
	assert.Equal(t, "last", string(m.Payload))
	_, err = ch.Receive(ctx(t))
	assert.True(t, isClose(err, 4601), "%v", err)
	assert.Error(t, ch.Send(ctx(t), 1, nil))
}

func TestCloseUnblocksCreditWaiters(t *testing.T) {
	o := testOpts
	o.CloseTimeout = 50 * time.Millisecond
	_, res, raw := connectRaw(t, o, ClientConfig{Channels: []ChannelSpec{data}}, map[string]any{"maxFramePayload": 4096, "maxMessageSize": 4096, "initialWindow": 4096})
	ch := res.Channels[0]
	require.NoError(t, ch.Send(ctx(t), 1, make([]byte, 4096)))
	sends := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() { sends <- ch.Send(ctx(t), 1, make([]byte, 100)) }()
	}
	time.Sleep(10 * time.Millisecond)
	closed := make(chan error, 1)
	go func() { closed <- ch.Close(ctx(t), CodeNormal, "") }()
	raw.control(TypeCloseChannel)
	raw.send(ctrl(TypeCloseChannel, map[string]any{"id": 1}))
	require.NoError(t, <-closed)
	assert.Error(t, <-sends)
	assert.Error(t, <-sends)
}

func TestFragmentErrorsCloseOnlyTheChannel(t *testing.T) {
	wide := testOpts
	wide.Limits = Limits{MaxFramePayload: 65536, MaxMessageSize: 262144, InitialWindow: 1 << 20}
	_, res, raw := connectRaw(t, wide, ClientConfig{Channels: []ChannelSpec{data, data}}, nil)
	raw.send(Frame{Channel: 1, Type: 1, Flags: FlagMore, Payload: []byte("a")})
	raw.send(Frame{Channel: 1, Type: 2, Payload: []byte("b")})
	cc := jsonOf(t, raw.control(TypeCloseChannel))
	assert.Equal(t, float64(1), cc["id"])
	assert.Equal(t, float64(CodeProtocolError), cc["code"])
	for i := 0; i < 5; i++ {
		raw.send(Frame{Channel: 3, Type: 1, Flags: FlagMore, Payload: make([]byte, 60000)})
	}
	cc = jsonOf(t, raw.control(TypeCloseChannel))
	assert.Equal(t, []any{float64(3), float64(CodeMessageTooLarge)}, []any{cc["id"], cc["code"]})
	assert.Nil(t, res.Channels[0].Session().Err(), "connection stays open")
}

func TestHeldFramesBeforeAck(t *testing.T) {
	client, _, raw := connectRaw(t, testOpts, ClientConfig{}, nil)
	opened := make(chan *Channel, 1)
	go func() {
		c, err := client.OpenChannel(ctx(t), data)
		assert.NoError(t, err)
		opened <- c
	}()
	id := uint16(jsonOf(t, raw.control(TypeOpenChannel))["id"].(float64))
	raw.send(probeFrame(id, TypePing, 9))
	raw.send(Frame{Channel: id, Type: 4, Payload: []byte("early")})
	raw.send(ctrl(TypeChannelAck, map[string]any{"id": id, "reliable": true, "ordered": true}))
	ch := <-opened
	m, err := ch.Receive(ctx(t))
	require.NoError(t, err)
	assert.Equal(t, "early", string(m.Payload))
	assert.Equal(t, probeFrame(id, TypePong, 9), raw.next(func(f Frame) bool { return f.IsProbe() }))
}

func TestHeldFramesAreBounded(t *testing.T) {
	client, _, raw := connectRaw(t, testOpts, ClientConfig{}, nil)
	go client.OpenChannel(ctx(t), data)
	id := uint16(jsonOf(t, raw.control(TypeOpenChannel))["id"].(float64))
	for i := 0; i < 5; i++ {
		raw.send(Frame{Channel: id, Type: 1, Payload: make([]byte, 65536)})
	}
	assert.Equal(t, CodeFlowControlError, raw.closeCode())
}

func TestFramesInSameWriteAsWelcome(t *testing.T) {
	lib, raw := rawPair(t, "stream")
	got := make(chan *Channel, 1)
	done := make(chan error, 1)
	go func() {
		_, _, err := Connect(context.Background(), lib, ClientConfig{}, acceptAll(func(c *Channel) { got <- c }))
		done <- err
	}()
	raw.control(TypeHello)
	w := ctrl(TypeWelcome, map[string]any{"version": []int{0, 2, 0}}).Encode()
	o := ctrl(TypeOpenChannel, map[string]any{"id": 2, "name": "srv", "reliable": true, "ordered": true}).Encode()
	raw.sendBytes(append(w, o...), false)
	require.NoError(t, <-done)
	assert.Equal(t, float64(2), jsonOf(t, raw.control(TypeChannelAck))["id"])
	assert.Equal(t, "srv", (<-got).Name())
}

// ─── Robustness ─────────────────────────────────────────────────────────────

func TestRobustness(t *testing.T) {
	_, raw := acceptRaw(t, testOpts, nil)
	raw.send(Frame{Channel: 0, Type: 0x55, Payload: []byte("{}")})
	assert.Equal(t, float64(CodeUnsupported), jsonOf(t, raw.control(TypeError))["code"], "unknown control type")
	raw.send(ctrl(TypeCloseChannel, map[string]any{"id": 0}))
	assert.Equal(t, float64(CodeProtocolError), jsonOf(t, raw.control(TypeError))["code"], "CLOSE_CHANNEL for channel 0")
	raw.send(Frame{Channel: 9, Type: 1, Payload: []byte("x")})
	e := jsonOf(t, raw.control(TypeError))
	assert.Equal(t, []any{float64(CodeChannelNotFound), float64(9)}, []any{e["code"], e["channel"]})
	raw.send(ctrl(TypeCloseChannel, map[string]any{"id": 77}))
	raw.send(Frame{Channel: 0, Type: TypePing, Flags: FlagProbe | 0, Payload: []byte{0, 0, 0, 1}})
	raw.next(func(f Frame) bool { return f.IsProbe() })
	raw.send(ctrl(TypeChannelAck, map[string]any{"id": 2, "reliable": true, "ordered": true}))
	assert.Equal(t, CodeProtocolError, raw.closeCode(), "ACK for a channel not opening")
}

func TestPeerErrorCallback(t *testing.T) {
	got := make(chan ErrorMessage, 1)
	o := testOpts
	o.OnError = func(e ErrorMessage) { got <- e }
	_, raw := acceptRaw(t, o, nil)
	raw.send(ctrl(TypeError, map[string]any{"code": 4004, "reason": "slow down"}))
	assert.Equal(t, ErrorMessage{Code: 4004, Reason: "slow down"}, <-got)
}

func TestTextMessageCloses1002(t *testing.T) {
	lib, raw := rawPair(t, "message")
	go Accept(context.Background(), lib, ServerConfig{}, testOpts)
	raw.helloAndWelcome(nil)
	raw.msg.out <- nil // a text message
	assert.Equal(t, CodeProtocolError, raw.closeCode())
}

func TestMessageBindingRoundTrip(t *testing.T) {
	a, b := memMessagePair()
	got := make(chan Message, 1)
	go Accept(context.Background(), NewMessageConn(b), ServerConfig{}, acceptAll(func(ch *Channel) {
		m, _ := ch.Receive(context.Background())
		got <- m
	}))
	_, res, err := Connect(context.Background(), NewMessageConn(a), ClientConfig{Channels: []ChannelSpec{data}}, testOpts)
	require.NoError(t, err)
	big := bytes.Repeat([]byte("z"), 100_000)
	require.NoError(t, res.Channels[0].Send(ctx(t), 1, big))
	assert.Equal(t, big, (<-got).Payload)
}

// ─── Connection close ───────────────────────────────────────────────────────

func TestGracefulCloseFlushesData(t *testing.T) {
	got := make(chan int, 1)
	client, server, res := pair(t, testOpts, acceptAll(func(ch *Channel) {
		m, err := ch.Receive(context.Background())
		if err == nil {
			got <- len(m.Payload)
		}
	}), data)
	go res.Channels[0].Send(context.Background(), 1, make([]byte, 200_000))
	time.Sleep(time.Millisecond)
	require.NoError(t, client.Close(ctx(t), CodeNormal, "bye"))
	assert.Equal(t, 200_000, <-got)
	<-server.Done()
	assert.Equal(t, CloseError{Code: CodeNormal, Reason: "bye", Remote: true}, server.CloseInfo())
	assert.NoError(t, client.Err())
}

func TestRemoteCloseCodePropagates(t *testing.T) {
	client, server, res := pair(t, testOpts, acceptAll(nil), data)
	require.NoError(t, server.Close(ctx(t), 4600, "app says bye"))
	<-client.Done()
	assert.True(t, isClose(client.Err(), 4600))
	_, err := res.Channels[0].Receive(ctx(t))
	assert.True(t, isClose(err, 4600), "%v", err)
	_, err = client.Ping(ctx(t))
	assert.ErrorIs(t, err, ErrClosed)
	_, err = client.OpenChannel(ctx(t), data)
	assert.ErrorIs(t, err, ErrClosed)
}

func TestTransportEOFIsGoingAway(t *testing.T) {
	client, _, raw := connectRaw(t, testOpts, ClientConfig{}, nil)
	raw.fc.Close()
	<-client.Done()
	assert.True(t, isClose(client.Err(), CodeGoingAway))
}

func TestCloseTimeoutWithSilentPeer(t *testing.T) {
	client, _, raw := connectRaw(t, testOpts, ClientConfig{}, nil)
	start := time.Now()
	require.NoError(t, client.Close(ctx(t), CodeNormal, ""))
	raw.control(TypeClose)
	assert.GreaterOrEqual(t, time.Since(start), 250*time.Millisecond, "waited for the peer's CLOSE")
}

func TestStreamFramingSplitReads(t *testing.T) {
	var buf bytes.Buffer
	buf.Write(Magic[:])
	for i := 0; i < 50; i++ {
		buf.Write(Frame{Channel: uint16(i), Type: byte(i), Payload: bytes.Repeat([]byte{byte(i)}, i*7)}.Encode())
	}
	fc := NewStreamConn(rwc{iotestOneByte{bytes.NewReader(buf.Bytes())}})
	for i := 0; i < 50; i++ {
		f, err := fc.ReadFrame(MaxWindow)
		require.NoError(t, err)
		assert.Equal(t, uint16(i), f.Channel)
		assert.Len(t, f.Payload, i*7)
	}
}

type iotestOneByte struct{ r io.Reader }

func (o iotestOneByte) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	return o.r.Read(p[:1])
}

func TestDecodeFrameErrors(t *testing.T) {
	_, err := DecodeFrame([]byte{0, 1}, 100)
	assert.Equal(t, CodeInvalidMessage, err.(*ProtocolError).Code)
	big := make([]byte, 8)
	big[1] = 1 // channel 1 (channel 0 always allows 65536)
	binary.BigEndian.PutUint32(big[4:], 200)
	_, err = DecodeFrame(big, 100)
	assert.Equal(t, CodeMessageTooLarge, err.(*ProtocolError).Code)
	assert.Contains(t, (&CloseError{Code: 1, Reason: "r", Remote: true}).Error(), "remote")
	assert.Contains(t, (&Refusal{Code: 4010, Reason: "x"}).Error(), "4010")
}

// Over real TCP the closer half-closes after CLOSE; the peer must still get
// its CLOSE reply out, so neither side waits for CloseTimeout.
func TestCloseOverTCPIsPrompt(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()
	slow := Options{PingInterval: -1, CloseTimeout: 5 * time.Second}
	srv := make(chan *Session, 1)
	go func() {
		c, err := l.Accept()
		if err != nil {
			return
		}
		s, _, err := Accept(context.Background(), NewStreamConn(c), ServerConfig{}, slow)
		assert.NoError(t, err)
		srv <- s
	}()
	c, err := net.Dial("tcp", l.Addr().String())
	require.NoError(t, err)
	client, _, err := Connect(context.Background(), NewStreamConn(c), ClientConfig{}, slow)
	require.NoError(t, err)
	server := <-srv
	start := time.Now()
	require.NoError(t, client.Close(ctx(t), CodeNormal, ""))
	<-server.Done()
	assert.Less(t, time.Since(start), time.Second)
	assert.Equal(t, CloseError{Code: CodeNormal, Remote: true}, server.CloseInfo())
}
