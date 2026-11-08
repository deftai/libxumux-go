// Package webrtc runs xumux over a WebRTC DataChannel, as the base [webrtc
// binding] defines it: the whole connection travels over one reliable,
// ordered DataChannel labelled "xumux", one frame per binary message.
//
// xumux does not do signalling. The application creates the PeerConnection,
// exchanges SDP and ICE however it likes, and hands the DataChannel to this
// package:
//
//	dc, err := webrtc.CreateDataChannel(pc) // before the offer, on both sides
//	t, err := webrtc.NewTransport(dc, webrtc.Config{})
//	// ... signalling ...
//	s, res, err := webrtc.Connect(ctx, t, cfg, opts) // or webrtc.Accept
//
// The DataChannel uses pion ([github.com/pion/webrtc/v4]). The core module
// [github.com/deftai/libxumux-go] stays standard-library only; this package
// is a separate module so that only programs that use WebRTC depend on pion.
//
// [webrtc binding]: https://github.com/deftai/xumux/blob/main/spec/bindings/webrtc.md
package webrtc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	xumux "github.com/deftai/libxumux-go"
	pion "github.com/pion/webrtc/v4"
)

const (
	// Label is the label of the xumux DataChannel.
	Label = "xumux"
	// Protocol is the DataChannel subprotocol of the xumux DataChannel.
	Protocol = "xumux"
	// ID is the stream ID of a negotiated xumux DataChannel.
	ID uint16 = 0

	// DefaultHighWaterMark is the default Config.HighWaterMark.
	DefaultHighWaterMark = 1 << 20
	// UnknownMaxMessageSize is the SCTP maximum message size assumed when the
	// peer's SDP does not declare one. The binding recommends 65536 because
	// every current browser accepts it.
	UnknownMaxMessageSize = 65536
	// RecommendedMaxMessageSize suits [pion.SettingEngine.SetSCTPMaxMessageSize]:
	// a default-sized frame (8-byte header plus 65536-byte payload). pion
	// otherwise accepts, and advertises, messages of up to 1 GiB, which it
	// buffers whole before xumux sees them.
	RecommendedMaxMessageSize = xumux.HeaderSize + xumux.DefaultMaxFramePayload
)

// ErrClosed is returned by a Transport after Close.
var ErrClosed = errors.New("webrtc: transport closed")

// DataChannelInit returns the DataChannel options the binding asks for:
// negotiated with ID 0, ordered, reliable, protocol "xumux". Both sides
// create the channel with the same options, so neither waits for an in-band
// open.
func DataChannelInit() *pion.DataChannelInit {
	negotiated, id, ordered, protocol := true, ID, true, Protocol
	return &pion.DataChannelInit{Negotiated: &negotiated, ID: &id, Ordered: &ordered, Protocol: &protocol}
}

// CreateDataChannel creates the xumux DataChannel on pc with [DataChannelInit].
// Call it on both sides before the offer is created, and pass the result to
// [NewTransport] straight away, before the channel can receive anything.
func CreateDataChannel(pc *pion.PeerConnection) (*pion.DataChannel, error) {
	return pc.CreateDataChannel(Label, DataChannelInit())
}

// Check reports whether dc is a valid xumux DataChannel: label and protocol
// "xumux", ordered, and reliable (no maxRetransmits or maxPacketLifeTime).
func Check(dc *pion.DataChannel) error {
	switch {
	case dc.Label() != Label:
		return fmt.Errorf("webrtc: DataChannel label %q, want %q", dc.Label(), Label)
	case dc.Protocol() != Protocol:
		return fmt.Errorf("webrtc: DataChannel protocol %q, want %q", dc.Protocol(), Protocol)
	case !dc.Ordered():
		return errors.New("webrtc: xumux DataChannel must be ordered")
	case dc.MaxRetransmits() != nil || dc.MaxPacketLifeTime() != nil:
		return errors.New("webrtc: xumux DataChannel must be reliable (no maxRetransmits or maxPacketLifeTime)")
	}
	return nil
}

// Config configures a Transport. The zero value selects the defaults.
type Config struct {
	// HighWaterMark bounds the DataChannel's send buffer: WriteMessage waits
	// while BufferedAmount is above it, until it drains to half. Default
	// [DefaultHighWaterMark].
	HighWaterMark uint64
}

// Transport is a [xumux.MessageTransport] over a pion DataChannel. Each
// message is one binary DataChannel message.
//
// NewTransport takes over the DataChannel's OnOpen, OnClose, OnError,
// OnMessage and OnBufferedAmountLow handlers and its buffered-amount-low
// threshold; do not set them afterwards.
type Transport struct {
	dc   *pion.DataChannel
	high uint64

	msgs   chan pion.DataChannelMessage
	opened chan struct{} // closed when the DataChannel opens
	ended  chan struct{} // closed when the DataChannel closes
	closed chan struct{} // closed by Close
	low    chan struct{} // buffered-amount-low signal, capacity 1

	writeMu                      sync.Mutex // serializes WriteMessage: check, then send
	openOnce, endOnce, closeOnce sync.Once
	errMu                        sync.Mutex
	err                          error // from OnError

	peakBuffered atomic.Uint64 // largest BufferedAmount seen after a send (tests)
}

// NewTransport wraps dc, which must pass [Check]. Call it before dc can
// receive messages (right after creating a negotiated channel, or inside
// OnDataChannel): pion drops messages that arrive with no handler set.
func NewTransport(dc *pion.DataChannel, cfg Config) (*Transport, error) {
	if err := Check(dc); err != nil {
		return nil, err
	}
	if cfg.HighWaterMark == 0 {
		cfg.HighWaterMark = DefaultHighWaterMark
	}
	t := &Transport{
		dc:     dc,
		high:   cfg.HighWaterMark,
		msgs:   make(chan pion.DataChannelMessage, 16),
		opened: make(chan struct{}),
		ended:  make(chan struct{}),
		closed: make(chan struct{}),
		low:    make(chan struct{}, 1),
	}
	// pion calls OnMessage synchronously from the channel's read loop, so
	// blocking here stops reading and lets SCTP push back on the sender
	// instead of queueing without bound.
	dc.OnMessage(func(m pion.DataChannelMessage) {
		select {
		case t.msgs <- m:
		case <-t.closed:
		}
	})
	dc.OnError(func(err error) {
		t.errMu.Lock()
		if t.err == nil {
			t.err = err
		}
		t.errMu.Unlock()
	})
	dc.SetBufferedAmountLowThreshold(t.high / 2)
	dc.OnBufferedAmountLow(func() {
		select {
		case t.low <- struct{}{}:
		default:
		}
	})
	// pion runs OnOpen and OnClose at once if the channel is already in
	// that state.
	dc.OnOpen(func() { t.openOnce.Do(func() { close(t.opened) }) })
	dc.OnClose(func() { t.endOnce.Do(func() { close(t.ended) }) })
	return t, nil
}

// DataChannel returns the underlying DataChannel.
func (t *Transport) DataChannel() *pion.DataChannel { return t.dc }

// WaitOpen waits until the DataChannel is open. It fails if ctx ends, the
// DataChannel closes first, or the Transport is closed. The binding
// recommends giving up 10 seconds after sending the offer and falling back
// to WebSocket.
func (t *Transport) WaitOpen(ctx context.Context) error {
	select {
	case <-t.opened:
		return nil
	default:
	}
	select {
	case <-t.opened:
		return nil
	case <-t.ended:
		return t.endErr("DataChannel closed before it opened")
	case <-t.closed:
		return ErrClosed
	case <-ctx.Done():
		return ctx.Err()
	}
}

// MaxMessageSize returns the largest message the peer accepts, from its SDP
// (a=max-message-size), or [UnknownMaxMessageSize] if that is not known yet
// (before the DataChannel opens).
func (t *Transport) MaxMessageSize() uint32 {
	if sctp := t.dc.Transport(); sctp != nil {
		if n := sctp.GetCapabilities().MaxMessageSize; n > 0 {
			return n
		}
	}
	return UnknownMaxMessageSize
}

// Options adjusts opts for this DataChannel: the frames this side sends
// (FragmentSize) are kept within the peer's maximum message size. Connect
// and Accept call it; use it when calling [xumux.Connect] or [xumux.Accept]
// directly. Call it after the DataChannel opens.
func (t *Transport) Options(opts xumux.Options) xumux.Options {
	return clampOptions(opts, t.MaxMessageSize())
}

func clampOptions(opts xumux.Options, maxMessageSize uint32) xumux.Options {
	limit := int(maxMessageSize) - xumux.HeaderSize
	if opts.FragmentSize <= 0 {
		opts.FragmentSize = xumux.DefaultFragmentSize
	}
	if limit > 0 && opts.FragmentSize > limit {
		opts.FragmentSize = limit
	}
	return opts
}

// ReadMessage returns the next binary message. It returns
// [xumux.ErrTextMessage] for a text message, io.EOF once the DataChannel has
// closed and every message received before has been read, and [ErrClosed]
// after Close.
func (t *Transport) ReadMessage() ([]byte, error) {
	select {
	case m := <-t.msgs:
		return message(m)
	case <-t.closed:
		return nil, ErrClosed
	case <-t.ended:
		// pion delivers every message before it reports the close.
		select {
		case m := <-t.msgs:
			return message(m)
		default:
			return nil, t.endErr("")
		}
	}
}

func message(m pion.DataChannelMessage) ([]byte, error) {
	if m.IsString {
		return nil, xumux.ErrTextMessage
	}
	return m.Data, nil
}

func (t *Transport) endErr(msg string) error {
	t.errMu.Lock()
	err := t.err
	t.errMu.Unlock()
	switch {
	case err != nil:
		return fmt.Errorf("webrtc: DataChannel failed: %w", err)
	case msg != "":
		return errors.New("webrtc: " + msg)
	}
	return io.EOF
}

// WriteMessage sends b as one binary message. While the DataChannel's send
// buffer holds more than HighWaterMark bytes it first waits for the buffer to
// drain, so a slow peer slows the writer rather than filling memory. A
// message larger than the peer's maximum message size fails. Concurrent
// calls are serialized, so the bound holds for them too.
func (t *Transport) WriteMessage(b []byte) error {
	t.writeMu.Lock()
	defer t.writeMu.Unlock()
	for t.dc.BufferedAmount() > t.high {
		select {
		case <-t.low:
		case <-t.closed:
			return ErrClosed
		case <-t.ended:
			return t.endErr("DataChannel closed")
		}
	}
	select {
	case <-t.closed:
		return ErrClosed
	default:
	}
	if err := t.dc.Send(b); err != nil {
		return err
	}
	if n := t.dc.BufferedAmount(); n > t.peakBuffered.Load() {
		t.peakBuffered.Store(n)
	}
	return nil
}

// Close closes the DataChannel. Messages already handed to Send are still
// delivered: pion resets the SCTP stream after them. The PeerConnection
// stays open; close it too unless it carries other things, such as media
// tracks.
func (t *Transport) Close() error {
	var err error
	t.closeOnce.Do(func() {
		close(t.closed)
		err = t.dc.Close()
	})
	return err
}

// Connect waits for the DataChannel to open, then runs the client handshake
// over it with [xumux.Connect], with opts adjusted by [Transport.Options].
// On failure the DataChannel is closed.
func Connect(ctx context.Context, t *Transport, cfg xumux.ClientConfig, opts xumux.Options) (*xumux.Session, *xumux.ConnectResult, error) {
	if err := t.WaitOpen(ctx); err != nil {
		t.Close()
		return nil, nil, err
	}
	return xumux.Connect(ctx, xumux.NewMessageConn(t), cfg, t.Options(opts))
}

// Accept waits for the DataChannel to open, then runs the server handshake
// over it with [xumux.Accept], with opts adjusted by [Transport.Options].
// On failure the DataChannel is closed.
func Accept(ctx context.Context, t *Transport, cfg xumux.ServerConfig, opts xumux.Options) (*xumux.Session, *xumux.Hello, error) {
	if err := t.WaitOpen(ctx); err != nil {
		t.Close()
		return nil, nil, err
	}
	return xumux.Accept(ctx, xumux.NewMessageConn(t), cfg, t.Options(opts))
}
