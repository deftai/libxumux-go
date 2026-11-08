package pipe_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	xumux "github.com/deftai/libxumux-go"
	"github.com/deftai/libxumux-go/pipe"
)

// stallable is a FrameConn whose writes can be held, like a transport
// whose peer has stopped reading.
type stallable struct {
	xumux.FrameConn
	mu   sync.Mutex
	gate chan struct{} // nil: writes pass
}

func (s *stallable) WriteFrame(f xumux.Frame) error {
	s.mu.Lock()
	g := s.gate
	s.mu.Unlock()
	if g != nil {
		<-g
	}
	return s.FrameConn.WriteFrame(f)
}

func (s *stallable) stall() {
	s.mu.Lock()
	s.gate = make(chan struct{})
	s.mu.Unlock()
}

func (s *stallable) resume() {
	s.mu.Lock()
	close(s.gate)
	s.gate = nil
	s.mu.Unlock()
}

// A write deadline holds even while the transport itself is stuck, and the
// bytes already accepted still arrive intact after the caller reuses its
// buffer.
func TestWriteDeadlineWhileTransportStalls(t *testing.T) {
	l := pipe.NewListener()
	defer l.Close()
	a, b := net.Pipe()
	sopts := opts
	sopts.Acceptor = l.Acceptor()
	go func() {
		_, _, _ = xumux.Accept(context.Background(), xumux.NewStreamConn(b), xumux.ServerConfig{}, sopts)
	}()
	fc := &stallable{FrameConn: xumux.NewStreamConn(a)}
	client, _, err := xumux.Connect(context.Background(), fc, xumux.ClientConfig{}, opts)
	require.NoError(t, err)
	defer client.Close(context.Background(), xumux.CodeNormal, "")
	c, err := pipe.Dial(ctx(t), client, "x", "")
	require.NoError(t, err)
	srv, err := l.AcceptPipe(ctx(t))
	require.NoError(t, err)

	fc.stall()
	buf := []byte("intact")
	require.NoError(t, c.SetWriteDeadline(time.Now().Add(100*time.Millisecond)))
	start := time.Now()
	n, err := c.Write(buf)
	assert.Less(t, time.Since(start), 2*time.Second, "Write returned at its deadline")
	assert.ErrorIs(t, err, os.ErrDeadlineExceeded)
	assert.Equal(t, len(buf), n, "the bytes were queued")
	copy(buf, "XXXXXX") // the caller may reuse its buffer at once

	fc.resume()
	got := make([]byte, n)
	_, err = io.ReadFull(srv, got)
	require.NoError(t, err)
	assert.Equal(t, "intact", string(got))
}

// RESET goes whole even when there is little credit left, so the peer
// gets the code and reason, not a truncated JSON prefix.
func TestResetGoesWholeWithLittleCredit(t *testing.T) {
	a, b := connPair(t)
	_, err := a.Write(make([]byte, xumux.DefaultInitialWindow-5)) // b isn't reading
	require.NoError(t, err)
	reason := strings.Repeat("r", 500)
	reset := make(chan error, 1)
	go func() { reset <- a.Reset(4501, reason) }()
	// Let Reset use the 5 bytes of credit left before b frees any more.
	time.Sleep(100 * time.Millisecond)

	got, err := io.ReadAll(b) // reading returns credit, so the RESET can go
	assert.Len(t, got, xumux.DefaultInitialWindow-5)
	assert.True(t, bytes.Equal(got, make([]byte, len(got))))
	var re *pipe.ResetError
	require.True(t, errors.As(err, &re), "want a ResetError, got %v", err)
	assert.Equal(t, 4501, re.Code)
	assert.Equal(t, reason, re.Reason)
	assert.NoError(t, <-reset)
}
