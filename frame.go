// Package xumux implements the xumux 0.2 core protocol: framing, the
// HELLO/WELCOME handshake, channels, per-channel flow control,
// fragmentation, probes and keepalive.
//
// A [Session] runs over a [FrameConn], which is either a byte stream
// ([NewStreamConn], for TCP, Unix sockets and stdio) or a message
// transport ([NewMessageConn], for WebSocket).
package xumux

import (
	"encoding/binary"
	"fmt"
)

// Protocol constants from the core specification.
const (
	HeaderSize        = 8
	ControlChannel    = 0x0000
	ReservedChannel   = 0xFFFF
	MaxChannelID      = 0xFFFE
	ControlMaxPayload = 65536

	FlagMore   byte = 0x01
	FlagProbe  byte = 0x02
	knownFlags      = FlagMore | FlagProbe
)

// Control message types (channel 0, PROBE clear).
const (
	TypeHello         byte = 0x01
	TypeWelcome       byte = 0x02
	TypeOpenChannel   byte = 0x03
	TypeChannelAck    byte = 0x04
	TypeCloseChannel  byte = 0x05
	TypeChannelReject byte = 0x06
	TypeWindowUpdate  byte = 0x07
	TypeClose         byte = 0x20
	TypeError         byte = 0xF0
)

// Probe types (any channel, PROBE set).
const (
	TypePing byte = 0x10
	TypePong byte = 0x11
)

// Magic is sent before the first frame on stream bindings.
var Magic = [4]byte{'X', 'M', 'U', 'X'}

// Version is the protocol version this package implements.
var Version = [3]int{0, 2, 0}

// Close and error codes.
const (
	CodeNormal           = 1000
	CodeGoingAway        = 1001
	CodeProtocolError    = 1002
	CodeUnsupported      = 1003
	CodeAuthFailed       = 4000
	CodeInvalidMessage   = 4001
	CodeChannelLimit     = 4002
	CodeChannelNotFound  = 4003
	CodeRateLimited      = 4004
	CodeMessageTooLarge  = 4005
	CodeVersionMismatch  = 4006
	CodeHelloTimeout     = 4007
	CodeKeepaliveTimeout = 4008
	CodeFlowControlError = 4009
	CodeChannelRefused   = 4010
)

// Default limits and bounds (core §3.4).
const (
	DefaultMaxFramePayload = 65536
	DefaultMaxMessageSize  = 262144
	DefaultInitialWindow   = 262144
	MinMaxFramePayload     = 4096
	MaxWindow              = 1<<31 - 1
)

// Frame is one xumux frame.
type Frame struct {
	Channel uint16
	Type    byte
	Flags   byte
	Payload []byte
}

// IsProbe reports whether the PROBE flag is set.
func (f Frame) IsProbe() bool { return f.Flags&FlagProbe != 0 }

// IsControl reports whether f is a control message (channel 0, not a probe).
func (f Frame) IsControl() bool { return f.Channel == ControlChannel && !f.IsProbe() }

// Encode returns the wire bytes of f.
func (f Frame) Encode() []byte {
	b := make([]byte, HeaderSize+len(f.Payload))
	binary.BigEndian.PutUint16(b[0:], f.Channel)
	b[2] = f.Type
	b[3] = f.Flags
	binary.BigEndian.PutUint32(b[4:], uint32(len(f.Payload)))
	copy(b[HeaderSize:], f.Payload)
	return b
}

// payloadLimit is the receiver's payload limit for a frame on ch (core §2.3).
func payloadLimit(ch uint16, maxFramePayload uint32) uint32 {
	if ch == ControlChannel {
		return ControlMaxPayload
	}
	return maxFramePayload
}

// DecodeFrame decodes one frame from b, which must hold exactly one frame
// (message bindings). It checks the payload limit before the length.
func DecodeFrame(b []byte, maxFramePayload uint32) (Frame, error) {
	if len(b) < HeaderSize {
		return Frame{}, protoErr(CodeInvalidMessage, "frame too short: %d bytes", len(b))
	}
	ch := binary.BigEndian.Uint16(b[0:])
	n := binary.BigEndian.Uint32(b[4:])
	if n > payloadLimit(ch, maxFramePayload) {
		return Frame{}, protoErr(CodeMessageTooLarge, "frame payload %d exceeds limit", n)
	}
	if uint64(len(b)) != HeaderSize+uint64(n) {
		return Frame{}, protoErr(CodeInvalidMessage, "frame length mismatch: header %d, message %d", n, len(b)-HeaderSize)
	}
	return Frame{Channel: ch, Type: b[2], Flags: b[3], Payload: b[HeaderSize:]}, nil
}

// validateFrame applies the structural checks of core §2.3 step 3.
func validateFrame(f Frame) error {
	switch {
	case f.Channel == ReservedChannel:
		return protoErr(CodeProtocolError, "frame on reserved channel 0xFFFF")
	case f.Flags&^knownFlags != 0:
		return protoErr(CodeProtocolError, "reserved flag bits set: %#x", f.Flags)
	case f.Flags&FlagMore != 0 && f.Channel == ControlChannel:
		return protoErr(CodeProtocolError, "MORE set on control channel")
	case f.Flags&FlagMore != 0 && f.IsProbe():
		return protoErr(CodeProtocolError, "MORE set on a probe frame")
	}
	return nil
}

// ProtocolError is a protocol violation with the code the peer is closed with.
type ProtocolError struct {
	Code    int
	Message string
	// Channel is set (with ChannelScope) when only one channel is closed.
	Channel      uint16
	ChannelScope bool
	// Transport means close the transport without sending CLOSE (bad magic).
	Transport bool
}

func (e *ProtocolError) Error() string { return fmt.Sprintf("xumux: %s (code %d)", e.Message, e.Code) }

func protoErr(code int, format string, args ...any) *ProtocolError {
	return &ProtocolError{Code: code, Message: fmt.Sprintf(format, args...)}
}

func channelErr(ch uint16, code int, format string, args ...any) *ProtocolError {
	e := protoErr(code, format, args...)
	e.Channel, e.ChannelScope = ch, true
	return e
}

// CloseError reports why a connection or channel closed with a code other than 1000.
type CloseError struct {
	Code   int
	Reason string
	// Remote is true when the peer initiated the close.
	Remote bool
}

func (e *CloseError) Error() string {
	who := "local"
	if e.Remote {
		who = "remote"
	}
	if e.Reason != "" {
		return fmt.Sprintf("xumux: closed by %s with code %d: %s", who, e.Code, e.Reason)
	}
	return fmt.Sprintf("xumux: closed by %s with code %d", who, e.Code)
}

// Refusal refuses a channel request with a code (core §5.2). Return one
// from an [Acceptor] to choose the code; other errors refuse with 4010.
type Refusal struct {
	Code   int
	Reason string
}

func (r *Refusal) Error() string { return fmt.Sprintf("channel refused (%d): %s", r.Code, r.Reason) }
