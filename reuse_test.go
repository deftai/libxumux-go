package xumux

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// tcpPair connects a client and server over real TCP, where the writer's
// completion callbacks lag behind the reader the way they do in production.
func tcpPair(t *testing.T, copts, sopts Options) (*Session, *Session) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()
	srv := make(chan *Session, 1)
	go func() {
		c, err := l.Accept()
		if err != nil {
			srv <- nil
			return
		}
		s, _, _ := Accept(context.Background(), NewStreamConn(c), ServerConfig{}, sopts)
		srv <- s
	}()
	c, err := net.Dial("tcp", l.Addr().String())
	require.NoError(t, err)
	client, _, err := Connect(context.Background(), NewStreamConn(c), ClientConfig{}, copts)
	require.NoError(t, err)
	server := <-srv
	require.NotNil(t, server)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = client.Close(ctx, CodeNormal, "")
	})
	return client, server
}

// Channel IDs are reused as soon as a close completes (core §5.4). Opening and
// closing in a tight loop must never make the peer see a reused ID as in use.
func TestRapidChannelIDReuse(t *testing.T) {
	for _, tc := range []struct {
		name        string
		serverClose bool
	}{{"peer closes, opener replies", true}, {"opener closes, peer replies", false}} {
		t.Run(tc.name, func(t *testing.T) {
			sopts := acceptAll(func(c *Channel) {
				if tc.serverClose {
					_ = c.Close(context.Background(), CodeNormal, "")
					return
				}
				_, _ = c.Receive(context.Background()) // wait for the client's close
			})
			sopts.PingInterval = -1
			client, server := tcpPair(t, Options{PingInterval: -1}, sopts)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			for i := 0; i < 3000; i++ {
				ch, err := client.OpenChannel(ctx, ChannelSpec{Name: "x", Reliable: true, Ordered: true})
				require.NoError(t, err, "open %d", i)
				if tc.serverClose {
					_, err = ch.Receive(ctx)
					require.True(t, errors.Is(err, io.EOF), "open %d: %v", i, err)
				} else {
					require.NoError(t, ch.Close(ctx, CodeNormal, ""), "close %d", i)
				}
				select {
				case <-server.Done():
					t.Fatalf("after %d channels the server closed: %v", i, server.CloseInfo())
				case <-client.Done():
					t.Fatalf("after %d channels the client closed: %v", i, client.CloseInfo())
				default:
				}
			}
		})
	}
}

// The same under contention: many channels open and close at once, so the
// writer's completion callbacks queue behind other work on the session lock.
func TestRapidChannelIDReuseConcurrent(t *testing.T) {
	for _, serverClose := range []bool{true, false} {
		sopts := acceptAll(func(c *Channel) {
			if serverClose {
				_ = c.Close(context.Background(), CodeNormal, "")
				return
			}
			_, _ = c.Receive(context.Background())
		})
		sopts.PingInterval = -1
		client, server := tcpPair(t, Options{PingInterval: -1}, sopts)
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		errs := make(chan error, 32)
		for w := 0; w < 32; w++ {
			go func() {
				for i := 0; i < 300; i++ {
					ch, err := client.OpenChannel(ctx, ChannelSpec{Name: "x", Reliable: true, Ordered: true})
					if err != nil {
						errs <- err
						return
					}
					if serverClose {
						_, err = ch.Receive(ctx)
						if !errors.Is(err, io.EOF) {
							errs <- err
							return
						}
					} else if err := ch.Close(ctx, CodeNormal, ""); err != nil {
						errs <- err
						return
					}
				}
				errs <- nil
			}()
		}
		for w := 0; w < 32; w++ {
			if err := <-errs; err != nil {
				cancel()
				t.Fatalf("serverClose=%v: %v (server: %v)", serverClose, err, server.CloseInfo())
			}
		}
		cancel()
	}
}

// If the connection ends while the Acceptor is still deciding, the handler it
// returns must still run (on a closed channel), so it can release what the
// Acceptor opened. Before, it was dropped, leaking the target.
func TestHandlerRunsWhenConnectionEndsDuringAccept(t *testing.T) {
	deciding, release := make(chan struct{}), make(chan struct{})
	ran := make(chan *Channel, 1)
	sopts := Options{PingInterval: -1, Acceptor: func(context.Context, ChannelRequest) (Handler, error) {
		close(deciding)
		<-release
		return func(c *Channel) { ran <- c }, nil
	}}
	client, server := tcpPair(t, Options{PingInterval: -1}, sopts)
	go func() {
		_, _ = client.OpenChannel(context.Background(), ChannelSpec{Name: "x", Reliable: true, Ordered: true})
	}()
	<-deciding
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = client.Close(ctx, CodeNormal, "")
	<-server.Done()
	close(release)
	select {
	case c := <-ran:
		select {
		case <-c.Done():
		default:
			t.Fatal("handler got a channel that is not closed")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("handler never ran: whatever the Acceptor opened leaks")
	}
}

// Channels accepted in the handshake must have their handlers run even when
// the client drops the transport at once: otherwise their targets leak.
// Before the fix, Accept looked the channels up after starting the reader,
// which could already have removed them. That window is too narrow to hit
// reliably here, so this is a guard on the requirement rather than a
// reproduction.
func TestHandshakeChannelHandlersRunWhenPeerDropsAtOnce(t *testing.T) {
	const rounds, perRound = 50, 8
	for round := 0; round < rounds; round++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		ran := make(chan struct{}, perRound)
		served := make(chan struct{})
		go func() {
			defer close(served)
			c, err := l.Accept()
			if err != nil {
				return
			}
			_, _, _ = Accept(context.Background(), NewStreamConn(c), ServerConfig{}, Options{PingInterval: -1,
				Acceptor: func(context.Context, ChannelRequest) (Handler, error) {
					time.Sleep(2 * time.Millisecond) // let the client's close arrive first
					return func(*Channel) { ran <- struct{}{} }, nil
				}})
		}()
		raw, err := net.Dial("tcp", l.Addr().String())
		require.NoError(t, err)
		specs := make([]ChannelSpec, perRound)
		for i := range specs {
			specs[i] = ChannelSpec{Name: "x", Reliable: true, Ordered: true}
		}
		// Send HELLO and drop the transport before WELCOME arrives, so the
		// server's reader sees EOF the moment it starts.
		hello := Hello{Version: Version, Limits: Limits{MaxFramePayload: DefaultMaxFramePayload, MaxMessageSize: DefaultMaxMessageSize, InitialWindow: DefaultInitialWindow}}
		for i := range specs {
			hello.Channels = append(hello.Channels, ChannelRequest{ID: uint16(2*i + 1), Name: "x", Reliable: true, Ordered: true})
		}
		fc := NewStreamConn(raw)
		require.NoError(t, fc.WriteFrame(controlFrame(TypeHello, hello)))
		raw.(*net.TCPConn).SetLinger(0)
		raw.Close()
		for i := 0; i < perRound; i++ {
			select {
			case <-ran:
			case <-time.After(2 * time.Second):
				t.Fatalf("round %d: only %d of %d handlers ran", round, i, perRound)
			}
		}
		l.Close()
		<-served // the server goroutine never outlives its round
	}
}
