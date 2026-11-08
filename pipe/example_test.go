package pipe_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"

	xumux "github.com/deftai/libxumux-go"
	"github.com/deftai/libxumux-go/pipe"
)

// HTTP over one xumux connection: the server serves on a pipe.Listener,
// and the client dials a pipe for each HTTP connection.
func Example_http() {
	ctx := context.Background()
	a, b := net.Pipe() // any byte stream: TCP, TLS, a Unix socket, stdio

	l := pipe.NewListener()
	go func() {
		s, _, err := xumux.Accept(ctx, xumux.NewStreamConn(b), xumux.ServerConfig{}, xumux.Options{Acceptor: l.Acceptor()})
		if err != nil {
			return
		}
		defer s.Close(ctx, xumux.CodeNormal, "")
		_ = http.Serve(l, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprintf(w, "hello from %s", r.URL.Path)
		}))
	}()

	s, _, err := xumux.Connect(ctx, xumux.NewStreamConn(a), xumux.ClientConfig{}, xumux.Options{})
	if err != nil {
		panic(err)
	}
	hc := &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return pipe.Dial(ctx, s, "http", "")
		},
	}}
	resp, err := hc.Get("http://xumux/pipes")
	if err != nil {
		panic(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	fmt.Println(string(body))
	l.Close()
	// Output: hello from /pipes
}
