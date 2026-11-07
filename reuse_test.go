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
