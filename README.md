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

The only dependency is the standard library. Tests also use testify and `golang.org/x/net/nettest`. WebSocket support is in the separate [`ws`](#websocket-the-ws-module) module.

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

A server can take channels from a [`ChannelListener`](listener.go) instead, in the style of `net.Listener`:

```go
l := xumux.NewChannelListener()
s, _, err := xumux.Accept(ctx, fc, xumux.ServerConfig{}, xumux.Options{Acceptor: l.Acceptor()})
for {
	ch, err := l.Accept(ctx)
	...
}
```

## Streams: `net.Conn` over xumux

A core channel carries typed messages. For byte streams, the [`pipe`](pipe) package implements the [`pipe/1`](https://github.com/deftai/xumux/blob/main/spec/profiles/pipe.md) profile, where each pipe is a `net.Conn` with `CloseWrite` for half-close. `io.Copy`, `bufio`, `crypto/tls` and `net/http` work over it unchanged:

```go
import "github.com/deftai/libxumux-go/pipe"

// Client: one pipe per HTTP connection.
hc := &http.Client{Transport: &http.Transport{
	DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return pipe.Dial(ctx, session, "http", "")
	},
}}

// Server: a net.Listener for the pipes peers open.
l := pipe.NewListener()
s, _, err := xumux.Accept(ctx, fc, xumux.ServerConfig{}, xumux.Options{Acceptor: l.Acceptor()})
http.Serve(l, handler)
```

- **Flow control is backpressure.** Credit goes back to the peer only as `Read` hands bytes to the caller, so a reader that stops reading stops the writer.
- **Deadlines** (`SetDeadline`, `SetReadDeadline`, `SetWriteDeadline`) work as on a socket, including on calls already waiting. A timeout is a `net.Error` with `Timeout()` true and matches `os.ErrDeadlineExceeded`.
- **Ending a pipe:** `CloseWrite` sends EOF and reading goes on. `Close` sends EOF if needed and closes the channel. `Reset` reports a failure to the peer, whose `Read` returns a `*pipe.ResetError`.
- **Targets:** `pipe.Dial` can name a target for the peer to connect the pipe to. A responder must check targets against an allow-list (`Listener.Check`), or it becomes an open proxy.

`pipe.Conn` passes `nettest.TestConn`, the conformance suite the Go team uses for `net.Conn` implementations.

## WebSocket: the `ws` module

[`ws`](ws) implements the xumux [WebSocket binding](https://github.com/deftai/xumux/blob/main/spec/bindings/websocket.md): subprotocol `xumux`, one frame per binary message, text messages refused with CLOSE 1002, and a WebSocket close of 1000 (1001 when going away) after the xumux CLOSE. It is a separate module, so the core keeps no dependencies; it uses [`github.com/coder/websocket`](https://github.com/coder/websocket).

```sh
go get github.com/deftai/libxumux-go/ws
```

```go
import "github.com/deftai/libxumux-go/ws"

// Client
fc, _, err := ws.Dial(ctx, "wss://example.com/xumux", nil)
s, res, err := xumux.Connect(ctx, fc, cfg, xumux.Options{})

// Server: Handler upgrades, refuses clients without the subprotocol (1008),
// and closes the connection when the callback returns. ws.Accept(w, r, opts)
// does the upgrade alone.
http.Handle("/xumux", ws.Handler(nil, func(fc xumux.FrameConn, r *http.Request) {
	s, hello, err := xumux.Accept(r.Context(), fc, xumux.ServerConfig{}, opts)
	if err == nil {
		<-s.Done()
	}
}))
```

Set `DialOptions.MaxFramePayload` / `AcceptOptions.MaxFramePayload` to the session's `Limits.MaxFramePayload` if you change it; it sizes the WebSocket read limit. Cross-origin browser requests are refused unless listed in `AcceptOptions.OriginPatterns`.

The module is versioned on its own, with tags of the form `ws/vX.Y.Z` (Go's rule for nested modules), and `ws/go.mod` requires a released core version. A change that needs new core API lands in two steps: release the core, then bump the requirement in `ws/go.mod` and tag `ws/v…`.

## What's implemented

- Framing over byte streams (with the `XMUX` magic) and message transports.
- The HELLO/WELCOME handshake: version negotiation, limits, auth, channels opened in HELLO.
- Channels: open, accept or refuse (per channel, or through a `ChannelListener`), reliable/ordered flags, metadata, fragmentation and reassembly, ID reuse after close (§5.4).
- The `pipe/1` profile as `net.Conn` and `net.Listener` (package `pipe`).
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
make check                                   # fmt, vet, race tests, coverage, ws module
make test/ws                                 # the nested ws module only
make test/interop LIBXUMUX_DIR=../libxumux   # against a built libxumux checkout
make test/fuzz FUZZTIME=30s                  # every fuzz target
```

`testdata/vectors.json` is copied from [`deftai/xumux` `conformance/vectors.json`](https://github.com/deftai/xumux/tree/main/conformance). Update it when the spec changes.

This code was developed in the xumcat repository, and its history came along when it moved here.

## License

MIT
