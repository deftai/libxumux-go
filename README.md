# libxumux-go

A Go implementation of the [xumux](https://github.com/deftai/xumux) 0.2 core protocol: many channels, each with its own flow control, over one connection.

It was written from the spec independently of the TypeScript [libxumux](https://github.com/deftai/libxumux), passes the spec's [conformance vectors](https://github.com/deftai/xumux/tree/main/conformance), and is tested against libxumux in both directions. [xumcat](https://github.com/deftai/xumcat), netcat for xumux, is built on it.

```sh
go get github.com/deftai/libxumux-go
```

The package name is `xumux`:

```go
import xumux "github.com/deftai/libxumux-go"
```

The only dependency is the standard library (testify is used in tests).

## Use

```go
// Client: any byte stream (TCP, TLS, a Unix socket, stdio) via NewStreamConn,
// or a message transport such as a WebSocket via NewMessageConn.
s, res, err := xumux.Connect(ctx, xumux.NewStreamConn(conn), xumux.ClientConfig{
	Application: "myapp/1",
	Channels:    []xumux.ChannelSpec{{Name: "data", Reliable: true, Ordered: true}},
}, xumux.Options{})
ch := res.Channels[0]                 // channels requested in HELLO
ch.Send(ctx, 0x01, []byte("hello"))   // blocks for flow-control credit
msg, err := ch.Receive(ctx)           // io.EOF after a normal close
rtt, err := ch.Ping(ctx)              // per-channel round-trip time
more, err := s.OpenChannel(ctx, xumux.ChannelSpec{Name: "more", Reliable: true, Ordered: true})

// Server: the Acceptor decides on every channel the peer opens.
s, hello, err := xumux.Accept(ctx, xumux.NewStreamConn(conn), xumux.ServerConfig{}, xumux.Options{
	Acceptor: func(ctx context.Context, req xumux.ChannelRequest) (xumux.Handler, error) {
		return func(ch *xumux.Channel) { /* serve ch */ }, nil
	},
})
```

See the [package example](example_test.go) for a complete echo server and client.

## What's implemented

- Framing over byte streams (with the `XMUX` magic) and message transports.
- The HELLO/WELCOME handshake: version negotiation, limits, auth, channels opened in HELLO.
- Channels: open, accept or refuse, reliable/ordered flags, metadata, fragmentation and reassembly, half-close, ID reuse after close (§5.4).
- Flow control: per-channel credit with receive-window auto-tuning, bounded per connection.
- Probes and keepalive: PING/PONG per channel and per connection.
- Close codes and errors as in the spec.

**Not implemented yet:** the `webrtc-multichannel/1` and `session-resume/1` extensions, and the WebRTC and QUIC bindings.

## Limits

A peer can't make this side use memory without limit. The `Options` defaults are:

| Option | Default | |
|---|---|---|
| `MaxChannels` | 1024 | Channels open or being opened, HELLO channels included. Past it, opens are refused with 4002 |
| `Limits.InitialWindow` | 256 KiB | Receive credit granted per channel |
| `MaxChannelWindow` | 16 MiB | How far auto-tuning may grow one channel's window |
| `ConnectionWindow` | 64 MiB | Stop growing windows once their sum reaches this |
| `MaxControlQueue` | 1 MiB | Queued control and probe replies. A peer that never reads them is closed with 4004 (negative: off) |

## Development

```sh
make check                                   # fmt, vet, race tests, coverage
make test/interop LIBXUMUX_DIR=../libxumux   # against a built libxumux checkout
make test/fuzz FUZZTIME=30s                  # every fuzz target
```

`testdata/vectors.json` is copied from [`deftai/xumux` `conformance/vectors.json`](https://github.com/deftai/xumux/tree/main/conformance). Update it when the spec changes.

This code was developed in the xumcat repository, and its history came along when it moved here.

## License

MIT
