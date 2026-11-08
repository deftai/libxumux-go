package webrtc

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	xumux "github.com/deftai/libxumux-go"
	"github.com/pion/ice/v4"
	pion "github.com/pion/webrtc/v4"
	"github.com/stretchr/testify/require"
)

// newPC returns a PeerConnection that gathers loopback host candidates
// only, so the tests need no network beyond 127.0.0.1.
func newPC(t *testing.T) *pion.PeerConnection {
	t.Helper()
	var se pion.SettingEngine
	se.SetIncludeLoopbackCandidate(true)
	se.SetIPFilter(func(ip net.IP) bool { return ip.IsLoopback() })
	se.SetNetworkTypes([]pion.NetworkType{pion.NetworkTypeUDP4})
	se.SetICEMulticastDNSMode(ice.MulticastDNSModeDisabled)
	se.SetSCTPMaxMessageSize(RecommendedMaxMessageSize)
	pc, err := pion.NewAPI(pion.WithSettingEngine(se)).NewPeerConnection(pion.Configuration{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = pc.Close() })
	return pc
}

// signal does the SDP exchange in-process, with all candidates bundled
// into the descriptions (no trickle), as the binding's HTTP POST signalling
// would.
func signal(t *testing.T, offerer, answerer *pion.PeerConnection) {
	t.Helper()
	offer, err := offerer.CreateOffer(nil)
	require.NoError(t, err)
	done := pion.GatheringCompletePromise(offerer)
	require.NoError(t, offerer.SetLocalDescription(offer))
	<-done
	require.NoError(t, answerer.SetRemoteDescription(*offerer.LocalDescription()))
	answer, err := answerer.CreateAnswer(nil)
	require.NoError(t, err)
	done = pion.GatheringCompletePromise(answerer)
	require.NoError(t, answerer.SetLocalDescription(answer))
	<-done
	require.NoError(t, offerer.SetRemoteDescription(*answerer.LocalDescription()))
}

// negotiatedPair connects two PeerConnections with the recommended
// negotiated xumux DataChannel and returns the client and server Transports.
func negotiatedPair(t *testing.T, cfgC, cfgS Config) (*Transport, *Transport) {
	t.Helper()
	pcC, pcS := newPC(t), newPC(t)
	dcC, err := CreateDataChannel(pcC)
	require.NoError(t, err)
	dcS, err := CreateDataChannel(pcS)
	require.NoError(t, err)
	tc, err := NewTransport(dcC, cfgC)
	require.NoError(t, err)
	ts, err := NewTransport(dcS, cfgS)
	require.NoError(t, err)
	signal(t, pcC, pcS)
	return tc, ts
}

func ctxT(t *testing.T, d time.Duration) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

func echo(ctx context.Context, ch *xumux.Channel) {
	for {
		m, err := ch.Receive(ctx)
		if err != nil {
			return
		}
		if err := ch.Send(ctx, m.Type, m.Payload); err != nil {
			return
		}
	}
}

type served struct {
	s     *xumux.Session
	hello *xumux.Hello
	err   error
}

func serve(ctx context.Context, ts *Transport, opts xumux.Options) <-chan served {
	out := make(chan served, 1)
	go func() {
		s, h, err := Accept(ctx, ts, xumux.ServerConfig{}, opts)
		out <- served{s, h, err}
	}()
	return out
}

func echoAcceptor(ctx context.Context) xumux.Acceptor {
	return func(context.Context, xumux.ChannelRequest) (xumux.Handler, error) {
		return func(ch *xumux.Channel) { echo(ctx, ch) }, nil
	}
}

// A whole session over a real DataChannel: handshake with a channel in
// HELLO, a channel opened later, echo on both, probes, and a normal close
// that closes both DataChannels.
func TestSession(t *testing.T) {
	ctx := ctxT(t, 30*time.Second)
	tc, ts := negotiatedPair(t, Config{}, Config{})
	srv := serve(ctx, ts, xumux.Options{Acceptor: echoAcceptor(ctx)})

	s, res, err := Connect(ctx, tc, xumux.ClientConfig{
		Application: "webrtc-test/1",
		Channels:    []xumux.ChannelSpec{{Name: "hello", Reliable: true, Ordered: true}},
	}, xumux.Options{})
	require.NoError(t, err)
	sv := <-srv
	require.NoError(t, sv.err)
	require.Equal(t, "webrtc-test/1", sv.hello.Application)
	require.Len(t, res.Channels, 1)

	more, err := s.OpenChannel(ctx, xumux.ChannelSpec{Name: "more", Reliable: true, Ordered: true})
	require.NoError(t, err)
	for i, ch := range []*xumux.Channel{res.Channels[0], more} {
		msg := bytes.Repeat([]byte{byte(i + 1)}, 100_000) // fragmented
		require.NoError(t, ch.Send(ctx, 0x07, msg))
		m, err := ch.Receive(ctx)
		require.NoError(t, err)
		require.Equal(t, uint8(0x07), m.Type)
		require.Equal(t, msg, m.Payload)
		_, err = ch.Ping(ctx)
		require.NoError(t, err)
	}
	_, err = s.Ping(ctx)
	require.NoError(t, err)

	require.NoError(t, s.Close(ctx, xumux.CodeNormal, "bye"))
	<-s.Done()
	select {
	case <-sv.s.Done():
	case <-ctx.Done():
		t.Fatal("server session did not close")
	}
	info := sv.s.CloseInfo()
	require.Equal(t, xumux.CodeNormal, info.Code)
	require.True(t, info.Remote)
	require.False(t, info.Lost, "the CLOSE exchange must complete over the DataChannel")
	require.False(t, s.CloseInfo().Lost)
	for _, tr := range []*Transport{tc, ts} {
		require.Eventually(t, func() bool { return tr.DataChannel().ReadyState() == pion.DataChannelStateClosed },
			10*time.Second, 10*time.Millisecond)
	}
}

// A bulk transfer much larger than the send buffer limit: the peer grants a
// large window, so only the DataChannel's buffered amount holds the sender
// back. The buffer must stay within HighWaterMark plus one frame, and the
// data must arrive intact.
func TestBackpressure(t *testing.T) {
	ctx := ctxT(t, 60*time.Second)
	const high = 64 << 10
	tc, ts := negotiatedPair(t, Config{HighWaterMark: high}, Config{})

	const total = 16 << 20
	sum := make(chan [32]byte, 1)
	srv := serve(ctx, ts, xumux.Options{
		Limits: xumux.Limits{InitialWindow: 8 << 20},
		Acceptor: func(context.Context, xumux.ChannelRequest) (xumux.Handler, error) {
			return func(ch *xumux.Channel) {
				h := sha256.New()
				for n := 0; n < total; {
					m, err := ch.Receive(ctx)
					if err != nil {
						return
					}
					h.Write(m.Payload)
					n += len(m.Payload)
				}
				var out [32]byte
				copy(out[:], h.Sum(nil))
				sum <- out
			}, nil
		},
	})
	s, _, err := Connect(ctx, tc, xumux.ClientConfig{}, xumux.Options{})
	require.NoError(t, err)
	sv := <-srv
	require.NoError(t, sv.err)
	defer sv.s.Close(ctx, xumux.CodeNormal, "")
	defer s.Close(ctx, xumux.CodeNormal, "")

	ch, err := s.OpenChannel(ctx, xumux.ChannelSpec{Name: "bulk", Reliable: true, Ordered: true})
	require.NoError(t, err)
	h := sha256.New()
	chunk := make([]byte, 64<<10)
	for i := 0; i < total/len(chunk); i++ {
		for j := range chunk {
			chunk[j] = byte(i*7 + j)
		}
		h.Write(chunk)
		require.NoError(t, ch.Send(ctx, 0x01, chunk))
	}
	var want [32]byte
	copy(want[:], h.Sum(nil))
	select {
	case got := <-sum:
		require.Equal(t, want, got)
	case <-ctx.Done():
		t.Fatal("transfer did not finish")
	}

	peak := tc.peakBuffered.Load()
	t.Logf("peak buffered amount %d (limit %d)", peak, high)
	require.LessOrEqual(t, peak, uint64(high+xumux.HeaderSize+xumux.DefaultFragmentSize),
		"WriteMessage must wait while the send buffer is above HighWaterMark")
	require.Greater(t, peak, uint64(high/2), "the transfer should have filled the send buffer")
}

// A DataChannel created in-band by the client (not negotiated) and taken
// by the server in OnDataChannel works the same way.
func TestInBandDataChannel(t *testing.T) {
	ctx := ctxT(t, 30*time.Second)
	pcC, pcS := newPC(t), newPC(t)
	init := DataChannelInit()
	f := false
	init.Negotiated, init.ID = &f, nil
	dcC, err := pcC.CreateDataChannel(Label, init)
	require.NoError(t, err)
	tc, err := NewTransport(dcC, Config{})
	require.NoError(t, err)
	got := make(chan *Transport, 1)
	pcS.OnDataChannel(func(dc *pion.DataChannel) {
		ts, err := NewTransport(dc, Config{})
		if err != nil {
			t.Error(err)
			return
		}
		got <- ts
	})
	signal(t, pcC, pcS)

	cres := make(chan error, 1)
	var s *xumux.Session
	go func() {
		var err error
		s, _, err = Connect(ctx, tc, xumux.ClientConfig{}, xumux.Options{})
		cres <- err
	}()
	var ts *Transport
	select {
	case ts = <-got:
	case <-ctx.Done():
		t.Fatal("no DataChannel")
	}
	sv := <-serve(ctx, ts, xumux.Options{Acceptor: echoAcceptor(ctx)})
	require.NoError(t, sv.err)
	require.NoError(t, <-cres)
	ch, err := s.OpenChannel(ctx, xumux.ChannelSpec{Name: "e", Reliable: true, Ordered: true})
	require.NoError(t, err)
	require.NoError(t, ch.Send(ctx, 1, []byte("in-band")))
	m, err := ch.Receive(ctx)
	require.NoError(t, err)
	require.Equal(t, "in-band", string(m.Payload))
	require.NoError(t, s.Close(ctx, xumux.CodeNormal, ""))
}

// If the DataChannel closes without a CLOSE exchange the connection is dead.
func TestDataChannelClosedUnexpectedly(t *testing.T) {
	ctx := ctxT(t, 30*time.Second)
	tc, ts := negotiatedPair(t, Config{}, Config{})
	srv := serve(ctx, ts, xumux.Options{Acceptor: echoAcceptor(ctx)})
	s, _, err := Connect(ctx, tc, xumux.ClientConfig{}, xumux.Options{})
	require.NoError(t, err)
	sv := <-srv
	require.NoError(t, sv.err)

	require.NoError(t, ts.DataChannel().Close()) // behind the session's back
	select {
	case <-s.Done():
	case <-ctx.Done():
		t.Fatal("client session survived the DataChannel closing")
	}
	require.True(t, s.CloseInfo().Lost)
	require.Error(t, s.Err())
}

// A text message is a protocol error: the binding allows binary only.
func TestTextMessage(t *testing.T) {
	ctx := ctxT(t, 30*time.Second)
	tc, ts := negotiatedPair(t, Config{}, Config{})
	srv := serve(ctx, ts, xumux.Options{CloseTimeout: 200 * time.Millisecond})
	require.NoError(t, tc.WaitOpen(ctx))
	require.NoError(t, tc.DataChannel().SendText("HELLO"))
	sv := <-srv
	require.Error(t, sv.err)
	var ce *xumux.CloseError
	var pe *xumux.ProtocolError
	if errors.As(sv.err, &ce) {
		require.Equal(t, xumux.CodeProtocolError, ce.Code)
	} else {
		require.ErrorAs(t, sv.err, &pe)
		require.Equal(t, xumux.CodeProtocolError, pe.Code)
	}

	_, err := tc.ReadMessage() // the server's CLOSE
	require.NoError(t, err)
}

// ReadMessage and WriteMessage at the transport level: binary messages
// both ways, text reported as ErrTextMessage, EOF after the peer closes,
// ErrClosed after Close.
func TestTransport(t *testing.T) {
	ctx := ctxT(t, 30*time.Second)
	tc, ts := negotiatedPair(t, Config{}, Config{})
	require.NoError(t, tc.WaitOpen(ctx))
	require.NoError(t, ts.WaitOpen(ctx))
	require.Greater(t, tc.MaxMessageSize(), uint32(0))

	require.NoError(t, tc.WriteMessage([]byte{1, 2, 3}))
	b, err := ts.ReadMessage()
	require.NoError(t, err)
	require.Equal(t, []byte{1, 2, 3}, b)
	require.NoError(t, ts.DataChannel().SendText("x"))
	_, err = tc.ReadMessage()
	require.ErrorIs(t, err, xumux.ErrTextMessage)

	// Messages sent just before a close still arrive, even when the reader
	// gets to them only after the close was reported, then EOF.
	for i := range 10 {
		require.NoError(t, tc.WriteMessage([]byte{byte(10 + i)}))
	}
	require.NoError(t, tc.Close())
	require.NoError(t, tc.Close()) // idempotent
	select {
	case <-ts.ended:
	case <-ctx.Done():
		t.Fatal("close not reported")
	}
	for i := range 10 {
		b, err = ts.ReadMessage()
		require.NoError(t, err)
		require.Equal(t, []byte{byte(10 + i)}, b)
	}
	_, err = ts.ReadMessage()
	require.ErrorIs(t, err, io.EOF)
	require.Error(t, ts.WriteMessage([]byte{6}))

	_, err = tc.ReadMessage()
	require.ErrorIs(t, err, ErrClosed)
	require.ErrorIs(t, tc.WriteMessage([]byte{7}), ErrClosed)
	require.ErrorIs(t, tc.WaitOpen(ctx), nil, "already open")
}

// WriteMessage blocked on a full send buffer returns once the Transport is
// closed.
func TestWriteUnblocksOnClose(t *testing.T) {
	ctx := ctxT(t, 30*time.Second)
	const high = 64 << 10
	tc, ts := negotiatedPair(t, Config{HighWaterMark: high}, Config{})
	require.NoError(t, tc.WaitOpen(ctx))
	require.NoError(t, ts.WaitOpen(ctx))
	// Nobody reads ts, so once its queue and SCTP's receive window fill,
	// the sender's buffer stops draining and WriteMessage waits.
	var writes atomic.Int64
	errc := make(chan error, 1)
	go func() {
		msg := make([]byte, 16<<10)
		for {
			if err := tc.WriteMessage(msg); err != nil {
				errc <- err
				return
			}
			writes.Add(1)
		}
	}()
	last := int64(-1)
	require.Eventually(t, func() bool {
		n := writes.Load()
		stalled := n == last && tc.DataChannel().BufferedAmount() > high
		last = n
		return stalled
	}, 20*time.Second, 300*time.Millisecond, "the writer should stall on the full send buffer")
	select {
	case err := <-errc:
		t.Fatalf("write failed early: %v", err)
	default:
	}
	require.NoError(t, tc.Close())
	select {
	case err := <-errc:
		require.ErrorIs(t, err, ErrClosed)
	case <-ctx.Done():
		t.Fatal("WriteMessage stayed blocked after Close")
	}
}

func TestWaitOpen(t *testing.T) {
	pc := newPC(t)
	dc, err := CreateDataChannel(pc)
	require.NoError(t, err)
	tr, err := NewTransport(dc, Config{})
	require.NoError(t, err)
	require.Equal(t, uint32(UnknownMaxMessageSize), tr.MaxMessageSize(), "not open yet")

	// No signalling: the channel never opens.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, _, err = Connect(ctx, tr, xumux.ClientConfig{}, xumux.Options{})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.ErrorIs(t, tr.WaitOpen(context.Background()), ErrClosed, "Connect closes the transport on failure")

	dc2, err := CreateDataChannel(newPC(t))
	require.NoError(t, err)
	tr2, err := NewTransport(dc2, Config{})
	require.NoError(t, err)
	_, _, err = Accept(ctx, tr2, xumux.ServerConfig{}, xumux.Options{})
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestCheck(t *testing.T) {
	pc := newPC(t)
	ptr := func(v uint16) *uint16 { return &v }
	str := func(v string) *string { return &v }
	bl := func(v bool) *bool { return &v }
	for _, tc := range []struct {
		name  string
		label string
		init  *pion.DataChannelInit
		ok    bool
	}{
		{"recommended", Label, DataChannelInit(), true},
		{"label", "other", DataChannelInit(), false},
		{"protocol", Label, &pion.DataChannelInit{Protocol: str("")}, false},
		{"unordered", Label, &pion.DataChannelInit{Protocol: str(Protocol), Ordered: bl(false)}, false},
		{"maxRetransmits", Label, &pion.DataChannelInit{Protocol: str(Protocol), MaxRetransmits: ptr(0)}, false},
		{"maxPacketLifeTime", Label, &pion.DataChannelInit{Protocol: str(Protocol), MaxPacketLifeTime: ptr(100)}, false},
		{"in-band", Label, &pion.DataChannelInit{Protocol: str(Protocol)}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dc, err := pc.CreateDataChannel(tc.label, tc.init)
			require.NoError(t, err)
			_, err = NewTransport(dc, Config{})
			if tc.ok {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
	init := DataChannelInit()
	require.True(t, *init.Negotiated)
	require.Equal(t, ID, *init.ID)
	require.True(t, *init.Ordered)
	require.Equal(t, Protocol, *init.Protocol)
	require.Nil(t, init.MaxRetransmits)
	require.Nil(t, init.MaxPacketLifeTime)
}

func TestClampOptions(t *testing.T) {
	o := clampOptions(xumux.Options{}, 65536)
	require.Equal(t, xumux.DefaultFragmentSize, o.FragmentSize)
	o = clampOptions(xumux.Options{FragmentSize: 1 << 20}, 65536)
	require.Equal(t, 65536-xumux.HeaderSize, o.FragmentSize)
	o = clampOptions(xumux.Options{FragmentSize: 1 << 20}, 0)
	require.Equal(t, 1<<20, o.FragmentSize, "no known limit")
	o = clampOptions(xumux.Options{}, 4096)
	require.Equal(t, 4096-xumux.HeaderSize, o.FragmentSize)
}
