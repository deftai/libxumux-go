package pipe_test

import (
	"bytes"
	"io"
	"net"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	xumux "github.com/deftai/libxumux-go"
	"github.com/deftai/libxumux-go/pipe"
)

// gated is a writer whose first Write waits for open.
type gated struct {
	open chan struct{}
	buf  bytes.Buffer
}

func (g *gated) Write(p []byte) (int, error) {
	<-g.open
	return g.buf.Write(p)
}

var _ io.WriterTo = (*pipe.Conn)(nil)

// io.Copy from a Conn returns credit only once the destination has taken
// the bytes: while it is stuck, the peer gets no new credit, not even a
// buffer's worth.
func TestCopyReturnsCreditOnlyAfterTheDestinationWrites(t *testing.T) {
	a, b := connPair(t)
	full := bytes.Repeat([]byte{7}, xumux.DefaultInitialWindow)
	_, err := a.Write(full) // exactly the window: a has no credit left
	require.NoError(t, err)

	dst := &gated{open: make(chan struct{})}
	copied := make(chan error, 1)
	go func() { _, err := io.Copy(dst, b); copied <- err }()

	// The copy has taken the first DATA but dst hasn't written it yet.
	require.NoError(t, a.SetWriteDeadline(time.Now().Add(300*time.Millisecond)))
	n, err := a.Write([]byte("more"))
	assert.Zero(t, n, "no credit came back while the destination was stuck")
	assert.ErrorIs(t, err, os.ErrDeadlineExceeded)

	close(dst.open)
	require.NoError(t, a.SetWriteDeadline(time.Time{}))
	_, err = a.Write([]byte("more"))
	require.NoError(t, err)
	require.NoError(t, a.CloseWrite())
	require.NoError(t, <-copied, "io.Copy ends cleanly at EOF")
	assert.Equal(t, append(full, "more"...), dst.buf.Bytes())
}

// WriteTo honours the read deadline, like Read.
func TestWriteToHonoursTheReadDeadline(t *testing.T) {
	_, b := connPair(t)
	require.NoError(t, b.SetReadDeadline(time.Now().Add(100*time.Millisecond)))
	_, err := b.WriteTo(io.Discard)
	var ne net.Error
	require.ErrorAs(t, err, &ne)
	assert.True(t, ne.Timeout())
}

// Like Read, WriteTo refuses to hand over even data already waiting once
// the read deadline has passed.
func TestWriteToStopsAtAnExpiredDeadlineWithDataWaiting(t *testing.T) {
	a, b := connPair(t)
	_, err := a.Write([]byte("0123456789"))
	require.NoError(t, err)
	head := make([]byte, 3)
	_, err = io.ReadFull(b, head) // the rest of that DATA waits in b
	require.NoError(t, err)

	require.NoError(t, b.SetReadDeadline(time.Now().Add(-time.Second)))
	var dst bytes.Buffer
	n, err := b.WriteTo(&dst)
	assert.Zero(t, n)
	assert.Empty(t, dst.Bytes())
	assert.ErrorIs(t, err, os.ErrDeadlineExceeded)

	require.NoError(t, b.SetReadDeadline(time.Time{}))
	require.NoError(t, a.CloseWrite())
	_, err = b.WriteTo(&dst)
	require.NoError(t, err)
	assert.Equal(t, "3456789", dst.String(), "nothing was lost")
}

type badWriter struct{ n int }

func (w badWriter) Write([]byte) (int, error) { return w.n, nil }

// A Writer that makes no progress, or reports an impossible count, ends
// the copy with an error instead of spinning or panicking.
func TestWriteToRejectsBrokenWriters(t *testing.T) {
	for _, tc := range []struct {
		name string
		n    int
		want string
	}{
		{"no progress", 0, io.ErrShortWrite.Error()},
		{"negative count", -1, "invalid write result"},
		{"count past the buffer", 1 << 20, "invalid write result"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, b := connPair(t)
			_, err := a.Write([]byte("data"))
			require.NoError(t, err)
			done := make(chan error, 1)
			go func() { _, err := b.WriteTo(badWriter{tc.n}); done <- err }()
			select {
			case err := <-done:
				assert.ErrorContains(t, err, tc.want)
			case <-time.After(2 * time.Second):
				t.Fatal("WriteTo did not return")
			}
		})
	}
}
