package xumux_test

import (
	"context"
	"fmt"
	"net"

	xumux "github.com/deftai/libxumux-go"
)

// An echo server and a client over TCP. Any byte stream works the same way
// through NewStreamConn; use NewMessageConn for WebSocket-like transports.
func Example() {
	ctx := context.Background()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	defer l.Close()

	go func() {
		c, err := l.Accept()
		if err != nil {
			return
		}
		// The Acceptor decides on every channel the client opens.
		echo := func(ch *xumux.Channel) {
			for {
				m, err := ch.Receive(ctx)
				if err != nil {
					return
				}
				_ = ch.Send(ctx, m.Type, m.Payload)
			}
		}
		s, _, err := xumux.Accept(ctx, xumux.NewStreamConn(c), xumux.ServerConfig{}, xumux.Options{
			Acceptor: func(context.Context, xumux.ChannelRequest) (xumux.Handler, error) { return echo, nil },
		})
		if err == nil {
			<-s.Done()
		}
	}()

	c, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		panic(err)
	}
	s, _, err := xumux.Connect(ctx, xumux.NewStreamConn(c), xumux.ClientConfig{Application: "example/1"}, xumux.Options{})
	if err != nil {
		panic(err)
	}
	ch, err := s.OpenChannel(ctx, xumux.ChannelSpec{Name: "echo", Reliable: true, Ordered: true})
	if err != nil {
		panic(err)
	}
	_ = ch.Send(ctx, 0x01, []byte("hello")) // blocks for flow-control credit
	m, _ := ch.Receive(ctx)
	fmt.Printf("%s\n", m.Payload)
	_ = s.Close(ctx, xumux.CodeNormal, "done")
	// Output: hello
}
