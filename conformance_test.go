package xumux

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type vectorFrame struct {
	Channel     uint16          `json:"channel"`
	Type        byte            `json:"type"`
	Flags       byte            `json:"flags"`
	PayloadHex  *string         `json:"payloadHex"`
	PayloadJSON json.RawMessage `json:"payloadJson"`
}

type vectors struct {
	Valid []struct {
		Name    string `json:"name"`
		Hex     string `json:"hex"`
		Decoded struct {
			vectorFrame
			Magic          string        `json:"magic"`
			Frames         []vectorFrame `json:"frames"`
			ReassembledHex string        `json:"reassembledHex"`
		} `json:"decoded"`
	} `json:"valid"`
	Invalid []struct {
		Name   string `json:"name"`
		Hex    string `json:"hex"`
		State  string `json:"state"`
		Expect struct {
			Action   string   `json:"action"`
			Code     int      `json:"code"`
			Bindings []string `json:"bindings"`
			Receiver string   `json:"receiver"`
		} `json:"expect"`
	} `json:"invalid"`
}

func loadVectors(t *testing.T) vectors {
	b, err := os.ReadFile("testdata/vectors.json")
	require.NoError(t, err)
	var v vectors
	require.NoError(t, json.Unmarshal(b, &v))
	return v
}

func (f vectorFrame) payload(t *testing.T) []byte {
	if f.PayloadJSON != nil {
		var buf bytes.Buffer
		require.NoError(t, json.Compact(&buf, f.PayloadJSON))
		return buf.Bytes()
	}
	if f.PayloadHex == nil {
		return nil
	}
	return fromHex(t, *f.PayloadHex)
}

type rwc struct{ io.Reader }

func (rwc) Write(b []byte) (int, error) { return len(b), nil }
func (rwc) Close() error                { return nil }

func TestConformanceValid(t *testing.T) {
	for _, v := range loadVectors(t).Valid {
		t.Run(v.Name, func(t *testing.T) {
			wire := fromHex(t, v.Hex)
			if v.Decoded.Magic != "" {
				assert.Equal(t, Magic[:], wire)
				return
			}
			want := v.Decoded.Frames
			if want == nil {
				want = []vectorFrame{v.Decoded.vectorFrame}
			}
			// Decode: a stream (after magic) yields exactly these frames.
			fc := NewStreamConn(rwc{bytes.NewReader(append(Magic[:], wire...))})
			var enc []byte
			for _, w := range want {
				f, err := fc.ReadFrame(MaxWindow)
				require.NoError(t, err)
				assert.Equal(t, []any{w.Channel, w.Type, w.Flags}, []any{f.Channel, f.Type, f.Flags})
				if w.PayloadJSON != nil {
					assert.JSONEq(t, string(w.PayloadJSON), string(f.Payload))
				} else {
					assert.Equal(t, w.payload(t), f.Payload)
				}
				assert.NoError(t, validateFrame(f))
				acceptedByDecoder(t, f)
				enc = append(enc, Frame{Channel: w.Channel, Type: w.Type, Flags: w.Flags, Payload: w.payload(t)}.Encode()...)
			}
			_, err := fc.ReadFrame(MaxWindow)
			assert.ErrorIs(t, err, io.EOF)
			// Encode: these frames produce exactly the wire bytes.
			assert.Equal(t, strings.ToUpper(hex.EncodeToString(wire)), strings.ToUpper(hex.EncodeToString(enc)))
		})
	}
}

// acceptedByDecoder checks that control payloads in valid vectors decode.
func acceptedByDecoder(t *testing.T, f Frame) {
	if !f.IsControl() {
		return
	}
	var err error
	switch f.Type {
	case TypeHello:
		_, err = decodeHello(f.Payload)
	case TypeWelcome:
		_, err = decodeWelcome(f.Payload)
	case TypeOpenChannel:
		_, err = decodeChannelRequest(f.Payload)
	case TypeChannelAck:
		_, err = decodeChannelAccept(f.Payload)
	case TypeChannelReject:
		_, err = decodeChannelRefusal(f.Payload)
	case TypeCloseChannel:
		_, err = decodeCloseChannel(f.Payload)
	case TypeWindowUpdate:
		_, err = decodeWindowUpdate(f.Payload)
	case TypeClose:
		_, err = decodeClose(f.Payload)
	case TypeError:
		_, err = decodeError(f.Payload)
	}
	assert.NoError(t, err)
}

func TestConformanceInvalid(t *testing.T) {
	for _, v := range loadVectors(t).Invalid {
		kind := "stream"
		if len(v.Expect.Bindings) > 0 && v.Expect.Bindings[0] == "websocket" {
			kind = "message"
		}
		receiver := v.Expect.Receiver
		if receiver == "" {
			receiver = "server"
		}
		t.Run(v.Name+"/"+receiver+"/"+kind, func(t *testing.T) {
			lib, raw := rawPair(t, kind)
			bytes := fromHex(t, v.Hex)
			if receiver == "client" {
				done := make(chan error, 1)
				go func() {
					_, _, err := Connect(context.Background(), lib, ClientConfig{}, acceptAll(nil))
					done <- err
				}()
				raw.awaitHelloSendWelcome(nil)
				require.NoError(t, <-done)
				raw.sendBytes(bytes, false)
				assert.Equal(t, v.Expect.Code, raw.closeCode())
				return
			}
			done := make(chan error, 1)
			go func() {
				_, _, err := Accept(context.Background(), lib, ServerConfig{}, acceptAll(nil))
				done <- err
			}()
			if v.State == "connected" {
				raw.helloAndWelcome(nil)
				require.NoError(t, <-done)
			}
			raw.sendBytes(bytes, v.State == "start")
			if v.Expect.Action == "close-transport" {
				raw.waitEnded()
				assert.Empty(t, raw.frames)
				assert.Error(t, <-done)
				return
			}
			assert.Equal(t, v.Expect.Code, raw.closeCode())
		})
	}
}
