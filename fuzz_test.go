package xumux

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"testing"
	"time"
)

// Seeds: a few valid encodings, so the fuzzer starts from the real grammar.
func frameSeeds(f *testing.F) {
	f.Add(controlFrame(TypeHello, Hello{Version: Version, Limits: Limits{MaxFramePayload: DefaultMaxFramePayload, MaxMessageSize: DefaultMaxMessageSize, InitialWindow: DefaultInitialWindow}}).Encode())
	f.Add(controlFrame(TypeOpenChannel, ChannelRequest{ID: 1, Name: "x", Reliable: true, Ordered: true, Metadata: map[string]any{"profile": "pipe/1"}}).Encode())
	f.Add(controlFrame(TypeCloseChannel, closeChannelMsg{ID: 1, Code: 1000}).Encode())
	f.Add(Frame{Channel: 1, Type: 1, Flags: FlagMore, Payload: []byte("hello")}.Encode())
	f.Add(Frame{Channel: 0, Flags: FlagProbe, Type: 0x10, Payload: []byte{1, 2, 3, 4}}.Encode())
	f.Add([]byte{})
	f.Add([]byte("XMUX"))
}

// DecodeFrame must never panic, must respect the payload limit, and a frame
// it accepts must re-encode to exactly the bytes it came from.
func FuzzDecodeFrame(f *testing.F) {
	frameSeeds(f)
	f.Fuzz(func(t *testing.T, b []byte) {
		fr, err := DecodeFrame(b, MinMaxFramePayload)
		if err != nil {
			return
		}
		if uint32(len(fr.Payload)) > payloadLimit(fr.Channel, MinMaxFramePayload) {
			t.Fatalf("payload of %d bytes accepted over the limit", len(fr.Payload))
		}
		if got := fr.Encode(); !bytes.Equal(got, b) {
			t.Fatalf("round trip changed the frame:\n in %x\nout %x", b, got)
		}
		_ = validateFrame(fr)
	})
}

// The stream reader must never panic, never allocate a payload over the
// limit, and always end on arbitrary input.
func FuzzStreamReadFrame(f *testing.F) {
	frameSeeds(f)
	f.Fuzz(func(t *testing.T, b []byte) {
		// Streams start with the magic; without it every input stops at the
		// magic check.
		c := NewStreamConn(fuzzRWC{bytes.NewReader(append([]byte("XMUX"), b...))})
		for i := 0; i < 1000; i++ {
			fr, err := c.ReadFrame(MinMaxFramePayload)
			if err != nil {
				return
			}
			if uint32(len(fr.Payload)) > payloadLimit(fr.Channel, MinMaxFramePayload) {
				t.Fatalf("payload of %d bytes read over the limit", len(fr.Payload))
			}
		}
	})
}

type fuzzRWC struct{ io.Reader }

func (fuzzRWC) Write(b []byte) (int, error) { return len(b), nil }
func (fuzzRWC) Close() error                { return nil }

// Every control-message decoder must reject or accept arbitrary payloads
// without panicking. Seeds include each message's real JSON.
func FuzzControlDecoders(f *testing.F) {
	for _, v := range []any{
		Hello{Version: Version, Limits: Limits{MaxFramePayload: 65536, MaxMessageSize: 262144, InitialWindow: 262144},
			Channels: []ChannelRequest{{ID: 1, Name: "x", Reliable: true, Ordered: true}}},
		Welcome{Version: Version, Limits: Limits{MaxFramePayload: 65536, MaxMessageSize: 262144, InitialWindow: 262144}},
		closeChannelMsg{ID: 3, Code: 1001, Reason: "r"},
		closeMsg{Code: 1000},
		ChannelRequest{ID: 1, Name: "x", Reliable: true, Ordered: true, Metadata: map[string]any{"target": "tcp:a:1"}},
	} {
		b, _ := json.Marshal(v)
		f.Add(b)
	}
	f.Add([]byte(`{"id":1e400}`))
	f.Add([]byte(`{"version":[0,2,"x"]}`))
	f.Add([]byte{0, 1, 0, 0, 0, 1})
	f.Fuzz(func(t *testing.T, p []byte) {
		_, _ = decodeHello(p)
		_, _ = decodeWelcome(p)
		_, _ = decodeCloseChannel(p)
		_, _ = decodeClose(p)
		_, _ = decodeError(p)
		_, _ = decodeWindowUpdate(p)
		_, _ = decodeChannelRequest(p)
		_, _ = decodeChannelAccept(p)
		_, _ = decodeChannelRefusal(p)
	})
}

// A server session fed a valid HELLO and then arbitrary bytes must never
// panic, and must finish promptly once the input ends.
func FuzzServerSession(f *testing.F) {
	hello := controlFrame(TypeHello, Hello{Version: Version, Limits: Limits{MaxFramePayload: DefaultMaxFramePayload, MaxMessageSize: DefaultMaxMessageSize, InitialWindow: DefaultInitialWindow},
		Channels: []ChannelRequest{{ID: 1, Name: "a", Reliable: true, Ordered: true}}}).Encode()
	var seq []byte
	for _, fr := range []Frame{
		controlFrame(TypeOpenChannel, ChannelRequest{ID: 3, Name: "b", Reliable: true, Ordered: true}),
		{Channel: 1, Type: 1, Payload: []byte("data")},
		{Channel: 1, Type: 1, Flags: FlagMore, Payload: []byte("frag")},
		{Channel: 1, Type: 1, Payload: []byte("ment")},
		{Channel: ControlChannel, Type: TypeWindowUpdate, Payload: []byte{0, 1, 0, 0, 1, 0}},
		{Channel: 0, Flags: FlagProbe, Type: 0x10, Payload: []byte{1, 2, 3, 4}},
		{Channel: 3, Flags: FlagProbe, Type: 0x10, Payload: []byte{1, 2, 3, 4}},
		controlFrame(TypeCloseChannel, closeChannelMsg{ID: 1}),
		controlFrame(TypeOpenChannel, ChannelRequest{ID: 1, Name: "again", Reliable: true, Ordered: true}),
		controlFrame(TypeClose, closeMsg{Code: 1000}),
	} {
		seq = append(seq, fr.Encode()...)
	}
	f.Add(seq)
	f.Add([]byte{})
	f.Add(seq[:len(seq)/2])
	f.Fuzz(func(t *testing.T, tail []byte) {
		in := append(append([]byte("XMUX"), hello...), tail...)
		opts := Options{PingInterval: -1, CloseTimeout: 100 * time.Millisecond,
			Acceptor: func(context.Context, ChannelRequest) (Handler, error) {
				return func(c *Channel) {
					for {
						if _, err := c.Receive(context.Background()); err != nil {
							return
						}
					}
				}, nil
			}}
		s, _, err := Accept(context.Background(), NewStreamConn(fuzzRWC{bytes.NewReader(in)}), ServerConfig{}, opts)
		if err != nil {
			return
		}
		select {
		case <-s.Done():
		case <-time.After(5 * time.Second):
			t.Fatalf("session still running 5 s after its input ended")
		}
	})
}
