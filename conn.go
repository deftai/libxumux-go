package xumux

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
)

// FrameConn carries whole frames. A Session calls ReadFrame from one
// goroutine and WriteFrame from another; neither is called concurrently
// with itself.
type FrameConn interface {
	// ReadFrame returns the next frame, rejecting payloads above the
	// receiver's limit (maxFramePayload, or 65536 on channel 0) before
	// reading them. It returns io.EOF when the transport ends cleanly.
	ReadFrame(maxFramePayload uint32) (Frame, error)
	// WriteFrame sends one frame. It returns once the frame is handed to
	// the transport, which provides backpressure.
	WriteFrame(f Frame) error
	// CloseWrite shuts down the write side, if the transport has one.
	CloseWrite() error
	// Close closes the transport.
	Close() error
}

// HalfCloser is implemented by streams that can shut down their write side.
type HalfCloser interface{ CloseWrite() error }

type streamConn struct {
	rwc       io.ReadWriteCloser
	r         *bufio.Reader
	readMagic bool
	sentMagic bool
	header    [HeaderSize]byte
	closeOnce sync.Once
	closeErr  error
}

// NewStreamConn frames a byte stream (TCP, Unix socket, stdio). Each side
// sends the XMUX magic bytes first and checks the peer's.
func NewStreamConn(rwc io.ReadWriteCloser) FrameConn {
	return &streamConn{rwc: rwc, r: bufio.NewReaderSize(rwc, 64<<10)}
}

func (c *streamConn) ReadFrame(maxFramePayload uint32) (Frame, error) {
	if !c.readMagic {
		var m [4]byte
		if _, err := io.ReadFull(c.r, m[:]); err != nil {
			return Frame{}, err
		}
		if m != Magic {
			return Frame{}, &ProtocolError{Code: CodeProtocolError, Message: fmt.Sprintf("bad magic % X", m[:]), Transport: true}
		}
		c.readMagic = true
	}
	if _, err := io.ReadFull(c.r, c.header[:]); err != nil {
		return Frame{}, err
	}
	f := Frame{Channel: binary.BigEndian.Uint16(c.header[0:]), Type: c.header[2], Flags: c.header[3]}
	n := binary.BigEndian.Uint32(c.header[4:])
	if n > payloadLimit(f.Channel, maxFramePayload) {
		return Frame{}, protoErr(CodeMessageTooLarge, "frame payload %d exceeds limit", n)
	}
	f.Payload = make([]byte, n)
	if _, err := io.ReadFull(c.r, f.Payload); err != nil {
		return Frame{}, err
	}
	return f, nil
}

func (c *streamConn) WriteFrame(f Frame) error {
	b := f.Encode()
	if !c.sentMagic {
		c.sentMagic = true
		b = append(Magic[:], b...)
	}
	_, err := c.rwc.Write(b)
	return err
}

func (c *streamConn) CloseWrite() error {
	if hc, ok := c.rwc.(HalfCloser); ok {
		return hc.CloseWrite()
	}
	return nil
}

func (c *streamConn) Close() error {
	c.closeOnce.Do(func() { c.closeErr = c.rwc.Close() })
	return c.closeErr
}

// MessageTransport carries whole binary messages (WebSocket).
type MessageTransport interface {
	// ReadMessage returns the next binary message. A text message must be
	// reported as ErrTextMessage.
	ReadMessage() ([]byte, error)
	WriteMessage(b []byte) error
	Close() error
}

// ErrTextMessage is returned by a MessageTransport for a non-binary message.
var ErrTextMessage = errors.New("text message on a binary-only transport")

type messageConn struct {
	t         MessageTransport
	closeOnce sync.Once
	closeErr  error
}

// NewMessageConn carries one frame per message, with no magic bytes.
func NewMessageConn(t MessageTransport) FrameConn { return &messageConn{t: t} }

func (c *messageConn) ReadFrame(maxFramePayload uint32) (Frame, error) {
	b, err := c.t.ReadMessage()
	if errors.Is(err, ErrTextMessage) {
		return Frame{}, protoErr(CodeProtocolError, "text message")
	}
	if err != nil {
		return Frame{}, err
	}
	return DecodeFrame(b, maxFramePayload)
}

func (c *messageConn) WriteFrame(f Frame) error { return c.t.WriteMessage(f.Encode()) }
func (c *messageConn) CloseWrite() error        { return nil }
func (c *messageConn) Close() error {
	c.closeOnce.Do(func() { c.closeErr = c.t.Close() })
	return c.closeErr
}
