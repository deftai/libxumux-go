package webrtc_test

import (
	"context"

	xumux "github.com/deftai/libxumux-go"
	"github.com/deftai/libxumux-go/webrtc"
	pion "github.com/pion/webrtc/v4"
)

// The client side. The server does the same with webrtc.Accept, creating
// its DataChannel with webrtc.CreateDataChannel before applying the offer.
// Signalling (sending the offer, receiving the answer) is the application's.
func Example() {
	ctx := context.Background()
	pc, err := pion.NewPeerConnection(pion.Configuration{})
	if err != nil {
		panic(err)
	}
	defer pc.Close()

	dc, err := webrtc.CreateDataChannel(pc) // negotiated, id 0: before the offer
	if err != nil {
		panic(err)
	}
	t, err := webrtc.NewTransport(dc, webrtc.Config{}) // before it can receive
	if err != nil {
		panic(err)
	}

	// ... create the offer, exchange SDP and ICE with the server ...

	s, _, err := webrtc.Connect(ctx, t, xumux.ClientConfig{Application: "example/1"}, xumux.Options{})
	if err != nil {
		panic(err) // or fall back to the WebSocket binding
	}
	ch, err := s.OpenChannel(ctx, xumux.ChannelSpec{Name: "chat", Reliable: true, Ordered: true})
	if err != nil {
		panic(err)
	}
	_ = ch.Send(ctx, 0x01, []byte("hello"))
	_ = s.Close(ctx, xumux.CodeNormal, "done") // closes the DataChannel; pc.Close above ends the rest
}
