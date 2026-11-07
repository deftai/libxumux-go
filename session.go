package xumux

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

// Defaults for Options.
const (
	DefaultPingInterval     = 30 * time.Second
	DefaultPingTimeout      = 10 * time.Second
	DefaultHandshakeTimeout = 10 * time.Second
	DefaultCloseTimeout     = 5 * time.Second
	DefaultFragmentSize     = 16384
	DefaultMaxChannels      = 1024
)

// ErrClosed is returned by operations on a closed session or channel.
var ErrClosed = errors.New("xumux: closed")

// Handler runs a channel the peer opened, once it is open. It is called in
// its own goroutine.
type Handler func(ch *Channel)

// Acceptor decides on a channel the peer asks for. Return a Handler to
// accept it, or an error to refuse it: a *Refusal chooses the code, any
// other error refuses with 4010. ctx is cancelled when the session closes.
type Acceptor func(ctx context.Context, req ChannelRequest) (Handler, error)

// Options configure a Session. Zero values select the defaults.
type Options struct {
	// Limits this side declares for what it receives (core §3.4).
	Limits Limits
	// PingInterval and PingTimeout: keepalive (core §8.2). The client
	// requests them; the server's values apply to both sides. A negative
	// PingInterval disables keepalive.
	PingInterval time.Duration
	PingTimeout  time.Duration
	// HandshakeTimeout bounds the wait for HELLO (server) or WELCOME (client).
	HandshakeTimeout time.Duration
	// CloseTimeout bounds the wait for the peer's CLOSE, and the flush of
	// queued data before a close.
	CloseTimeout time.Duration
	// FragmentSize is the largest frame payload this side sends (default
	// 16 KiB, so that channels interleave fairly).
	FragmentSize int
	// MaxChannels caps open plus pending peer-opened channels (code 4002).
	MaxChannels int
	// Acceptor decides on peer-opened channels. Nil refuses them with 1003.
	Acceptor Acceptor
	// OnError is called (in its own goroutine) for each ERROR the peer sends.
	OnError func(ErrorMessage)
}

func (o Options) withDefaults() Options {
	if o.Limits.MaxFramePayload == 0 {
		o.Limits.MaxFramePayload = DefaultMaxFramePayload
	}
	if o.Limits.MaxMessageSize == 0 {
		o.Limits.MaxMessageSize = max(DefaultMaxMessageSize, o.Limits.MaxFramePayload)
	}
	if o.Limits.InitialWindow == 0 {
		o.Limits.InitialWindow = max(DefaultInitialWindow, o.Limits.MaxMessageSize)
	}
	if o.PingInterval == 0 {
		o.PingInterval = DefaultPingInterval
	}
	if o.PingTimeout <= 0 {
		o.PingTimeout = DefaultPingTimeout
	}
	if o.HandshakeTimeout <= 0 {
		o.HandshakeTimeout = DefaultHandshakeTimeout
	}
	if o.CloseTimeout <= 0 {
		o.CloseTimeout = DefaultCloseTimeout
	}
	if o.FragmentSize <= 0 {
		o.FragmentSize = DefaultFragmentSize
	}
	if o.MaxChannels <= 0 {
		o.MaxChannels = DefaultMaxChannels
	}
	return o
}

type sessionState int

const (
	stateOpen sessionState = iota
	stateClosing
	stateClosed
)

type outItem struct {
	f       Frame
	done    chan error // buffered; may be nil
	after   func(err error)
	isClose bool
}

func (it *outItem) finish(err error) {
	if it.done != nil {
		it.done <- err
	}
	if it.after != nil {
		it.after(err)
	}
}

type pendingPing struct {
	sent time.Time
	rtt  chan time.Duration
}

// Session is one xumux connection. Create it with [Connect] or [Accept].
type Session struct {
	conn   FrameConn
	opts   Options
	client bool
	local  Limits
	remote Limits
	epoch  time.Time
	ctx    context.Context
	cancel context.CancelFunc

	requestedExt []string      // client: extensions asked for in HELLO
	pingInterval time.Duration // effective; 0 = disabled
	pingTimeout  time.Duration
	lastRx       atomic.Int64 // UnixNano of the last received frame

	mu            sync.Mutex
	state         sessionState
	chans         map[uint16]*Channel
	pendingRemote map[uint16]bool
	closeInfo     CloseError
	closeTimer    *time.Timer
	lastToken     uint32
	pings         map[uint32]*pendingPing
	stats         statsRecorder
	keepaliveAt   time.Time // when the outstanding keepalive PING was sent

	// Outbound scheduler, guarded by mu.
	urgent       []*outItem
	data         map[uint16][]*outItem
	rr           []uint16
	windowDirty  bool
	writeStopped bool
	wake         chan struct{}
	closeWritten chan struct{}

	done chan struct{}
}

func newSession(conn FrameConn, opts Options, client bool) *Session {
	opts = opts.withDefaults()
	ctx, cancel := context.WithCancel(context.Background())
	s := &Session{
		conn: conn, opts: opts, client: client, local: opts.Limits, epoch: time.Now(),
		ctx: ctx, cancel: cancel,
		chans: map[uint16]*Channel{}, pendingRemote: map[uint16]bool{}, pings: map[uint32]*pendingPing{},
		data: map[uint16][]*outItem{}, wake: make(chan struct{}, 1), closeWritten: make(chan struct{}),
		done: make(chan struct{}),
	}
	s.lastRx.Store(time.Now().UnixNano())
	return s
}

// ─── Handshake ──────────────────────────────────────────────────────────────

// ChannelSpec describes a channel to open.
type ChannelSpec struct {
	Name              string
	Reliable          bool
	Ordered           bool
	MaxRetransmits    *int
	MaxPacketLifeTime *int
	Metadata          map[string]any
}

// ClientConfig is what the client puts in HELLO.
type ClientConfig struct {
	Application string
	Extensions  []string
	Auth        map[string]any
	// Channels to open during the handshake.
	Channels []ChannelSpec
}

// ConnectResult is the outcome of the handshake for a client.
type ConnectResult struct {
	Welcome  Welcome
	Channels []*Channel // accepted HELLO channels, in request order
	Rejected []ChannelRefusal
}

// Connect performs the client handshake on conn. On failure conn is closed.
func Connect(ctx context.Context, conn FrameConn, cfg ClientConfig, opts Options) (*Session, *ConnectResult, error) {
	s := newSession(conn, opts, true)
	if err := checkLimits(s.local); err != nil {
		conn.Close()
		return nil, nil, fmt.Errorf("xumux: invalid local limits: %w", err)
	}
	interval := s.opts.PingInterval
	if interval < 0 {
		interval = 0
	}
	hello := Hello{
		Version: Version, Application: cfg.Application, Extensions: cfg.Extensions, Limits: s.local,
		PingInterval: interval.Seconds(), PingTimeout: s.opts.PingTimeout.Seconds(), Auth: cfg.Auth,
	}
	s.requestedExt = cfg.Extensions
	order := make([]*Channel, len(cfg.Channels))
	for i, spec := range cfg.Channels {
		r := spec.request(uint16(2*i + 1))
		hello.Channels = append(hello.Channels, r)
		order[i] = s.newChannel(r, true, chanOpening)
	}

	stop := s.handshakeDeadline(ctx)
	if err := conn.WriteFrame(controlFrame(TypeHello, hello)); err != nil {
		stop()
		conn.Close()
		return nil, nil, err
	}
	w, err := s.readWelcome()
	timedOut := !stop()
	if err != nil {
		if timedOut {
			return nil, nil, fmt.Errorf("xumux: handshake: %w", context.DeadlineExceeded)
		}
		return nil, nil, err
	}

	s.remote = w.Limits
	s.pingInterval = time.Duration(w.PingInterval * float64(time.Second))
	s.pingTimeout = time.Duration(w.PingTimeout * float64(time.Second))
	s.start()

	s.mu.Lock()
	res := &ConnectResult{Welcome: w, Rejected: w.Rejected}
	accepted := map[uint16]ChannelAccept{}
	for _, a := range w.Channels {
		accepted[a.ID] = a
	}
	for _, r := range w.Rejected {
		if c := s.chans[r.ID]; c != nil {
			c.openErr = &CloseError{Code: r.Code, Reason: r.Reason, Remote: true}
			s.finishChannelLocked(c)
		}
	}
	var replayErr error
	for _, c := range order {
		if a, ok := accepted[c.id]; ok {
			if err := s.ackLocked(c, a); err != nil && replayErr == nil {
				replayErr = err
			}
			res.Channels = append(res.Channels, c)
		}
	}
	if replayErr != nil {
		s.handleErrLocked(replayErr)
	}
	s.mu.Unlock()
	return s, res, nil
}

// readWelcome reads frames until WELCOME and validates it (core §3.1, §3.3).
func (s *Session) readWelcome() (Welcome, error) {
	for {
		f, err := s.conn.ReadFrame(s.local.MaxFramePayload)
		if err != nil {
			return Welcome{}, s.abortHandshake(err)
		}
		if err := validateFrame(f); err != nil {
			return Welcome{}, s.abortHandshake(err)
		}
		if f.Channel == ControlChannel {
			if f.IsProbe() || (f.Type != TypeWelcome && f.Type != TypeClose) {
				return Welcome{}, s.abortHandshake(protoErr(CodeProtocolError, "unexpected frame before WELCOME"))
			}
			if f.Type == TypeClose {
				return Welcome{}, s.remoteHandshakeClose(f.Payload)
			}
			w, err := decodeWelcome(f.Payload)
			if err == nil {
				err = s.checkWelcome(w)
			}
			if err != nil {
				return Welcome{}, s.abortHandshake(err)
			}
			return w, nil
		}
		// A responder's frames for a HELLO channel may overtake WELCOME (core §5.2).
		s.mu.Lock()
		c := s.chans[f.Channel]
		if c != nil {
			err = s.holdLocked(c, f)
		}
		s.mu.Unlock()
		if err != nil {
			return Welcome{}, s.abortHandshake(err)
		}
	}
}

func (s *Session) checkWelcome(w Welcome) error {
	if !VersionsCompatible(w.Version, Version) {
		return protoErr(CodeProtocolError, "incompatible server version %v", w.Version)
	}
	if err := checkLimits(w.Limits); err != nil {
		return err
	}
	for _, e := range w.Extensions {
		if !slices.Contains(s.requestedExt, e) {
			return protoErr(CodeProtocolError, "WELCOME accepted extension %q that was not requested", e)
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	answered := map[uint16]bool{}
	for _, id := range append(idsOf(w.Channels), refusalIDs(w.Rejected)...) {
		if c := s.chans[id]; c == nil || answered[id] {
			return protoErr(CodeProtocolError, "WELCOME lists channel %d incorrectly", id)
		}
		answered[id] = true
	}
	if len(answered) != len(s.chans) {
		return protoErr(CodeProtocolError, "WELCOME does not answer every HELLO channel")
	}
	return nil
}

func idsOf(a []ChannelAccept) []uint16 {
	out := make([]uint16, len(a))
	for i, x := range a {
		out[i] = x.ID
	}
	return out
}

func refusalIDs(r []ChannelRefusal) []uint16 {
	out := make([]uint16, len(r))
	for i, x := range r {
		out[i] = x.ID
	}
	return out
}

// ServerConfig controls how a server answers HELLO.
type ServerConfig struct {
	// Extensions this server supports; WELCOME accepts the overlap.
	Extensions []string
	// Authorize may refuse the connection. A *Refusal chooses the code,
	// any other error refuses with 4000 (AUTH_FAILED).
	Authorize func(ctx context.Context, h *Hello) error
}

// Accept performs the server handshake on conn. HELLO channels are
// decided by opts.Acceptor before WELCOME is sent. On failure conn is closed.
func Accept(ctx context.Context, conn FrameConn, cfg ServerConfig, opts Options) (*Session, *Hello, error) {
	s := newSession(conn, opts, false)
	if err := checkLimits(s.local); err != nil {
		conn.Close()
		return nil, nil, fmt.Errorf("xumux: invalid local limits: %w", err)
	}
	stop := s.handshakeDeadline(ctx)
	f, err := conn.ReadFrame(s.local.MaxFramePayload)
	timedOut := !stop()
	if timedOut {
		return nil, nil, fmt.Errorf("xumux: HELLO: %w", context.DeadlineExceeded)
	}
	if err == nil {
		err = validateFrame(f)
	}
	if err != nil {
		return nil, nil, s.abortHandshake(err)
	}
	if !f.IsControl() || f.Type != TypeHello {
		if f.IsControl() && f.Type == TypeClose {
			return nil, nil, s.remoteHandshakeClose(f.Payload)
		}
		return nil, nil, s.abortHandshake(protoErr(CodeProtocolError, "first frame must be HELLO"))
	}
	h, err := decodeHello(f.Payload)
	if err == nil {
		err = s.checkHello(h)
	}
	if err != nil {
		return nil, nil, s.abortHandshake(err)
	}
	if cfg.Authorize != nil {
		if err := cfg.Authorize(ctx, &h); err != nil {
			code, reason := CodeAuthFailed, err.Error()
			var r *Refusal
			if errors.As(err, &r) {
				code, reason = r.Code, r.Reason
			}
			return nil, nil, s.abortHandshake(&ProtocolError{Code: code, Message: reason})
		}
	}

	welcome := Welcome{Version: Version, Limits: s.local}
	for _, e := range h.Extensions {
		if slices.Contains(cfg.Extensions, e) {
			welcome.Extensions = append(welcome.Extensions, e)
		}
	}
	type acceptedChannel struct {
		req     ChannelRequest
		handler Handler
	}
	var open []acceptedChannel
	for _, r := range h.Channels {
		handler, refusal := s.decide(r)
		if refusal != nil {
			welcome.Rejected = append(welcome.Rejected, *refusal)
			continue
		}
		welcome.Channels = append(welcome.Channels, ChannelAccept{ID: r.ID, Properties: effectiveProperties(r)})
		open = append(open, acceptedChannel{r, handler})
	}
	if s.opts.PingInterval > 0 {
		s.pingInterval, s.pingTimeout = s.opts.PingInterval, s.opts.PingTimeout
		welcome.PingInterval, welcome.PingTimeout = s.pingInterval.Seconds(), s.pingTimeout.Seconds()
	} else {
		welcome.PingTimeout = s.opts.PingTimeout.Seconds()
	}
	s.remote = h.Limits
	for _, a := range open {
		c := s.newChannel(a.req, false, chanOpen)
		c.props = effectiveProperties(a.req)
		c.sendCredit = int64(s.remote.InitialWindow)
	}
	if err := conn.WriteFrame(controlFrame(TypeWelcome, welcome)); err != nil {
		conn.Close()
		return nil, nil, err
	}
	s.start()
	for _, a := range open {
		s.mu.Lock()
		c := s.chans[a.req.ID]
		s.mu.Unlock()
		if c != nil {
			go a.handler(c)
		}
	}
	return s, &h, nil
}

func (s *Session) checkHello(h Hello) error {
	if !VersionsCompatible(h.Version, Version) {
		return protoErr(CodeVersionMismatch, "unsupported version %v", h.Version)
	}
	if err := checkLimits(h.Limits); err != nil {
		return err
	}
	seen := map[uint16]bool{}
	for _, c := range h.Channels {
		if c.ID == 0 || c.ID == ReservedChannel || c.ID%2 != 1 || seen[c.ID] {
			return protoErr(CodeProtocolError, "invalid HELLO channel ID %d", c.ID)
		}
		seen[c.ID] = true
	}
	return nil
}

// handshakeDeadline closes the transport if the handshake outlives the
// timeout or ctx. The returned stop reports false if it already fired.
func (s *Session) handshakeDeadline(ctx context.Context) (stop func() bool) {
	var fired atomic.Bool
	fire := func() {
		fired.Store(true)
		if !s.client {
			// Nothing else has been written yet, so this cannot interleave (core §3.1).
			_ = s.conn.WriteFrame(controlFrame(TypeClose, closeMsg{Code: CodeHelloTimeout, Reason: "no HELLO received"}))
		}
		s.conn.Close()
	}
	t := time.AfterFunc(s.opts.HandshakeTimeout, fire)
	cancelCtx := context.AfterFunc(ctx, fire)
	return func() bool {
		t.Stop()
		cancelCtx()
		return !fired.Load()
	}
}

// abortHandshake closes a connection that failed during the handshake,
// following core §9.1, and returns the error to report.
func (s *Session) abortHandshake(err error) error {
	var pe *ProtocolError
	if !errors.As(err, &pe) {
		s.conn.Close()
		return err
	}
	if pe.Transport {
		s.conn.Close()
		return pe
	}
	s.tryClose(pe.Code, pe.Message)
	return pe
}

// remoteHandshakeClose answers a CLOSE received in place of HELLO/WELCOME.
func (s *Session) remoteHandshakeClose(payload []byte) error {
	m, err := decodeClose(payload)
	if err != nil {
		return s.abortHandshake(err)
	}
	_ = s.conn.WriteFrame(controlFrame(TypeClose, closeMsg{Code: m.Code}))
	_ = s.conn.CloseWrite()
	s.conn.Close()
	return &CloseError{Code: m.Code, Reason: m.Reason, Remote: true}
}

// tryClose sends CLOSE, then waits briefly for the peer's CLOSE or EOF.
func (s *Session) tryClose(code int, reason string) {
	_ = s.conn.WriteFrame(controlFrame(TypeClose, closeMsg{Code: code, Reason: reason}))
	_ = s.conn.CloseWrite()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			f, err := s.conn.ReadFrame(s.local.MaxFramePayload)
			if err != nil || (f.IsControl() && f.Type == TypeClose) {
				return
			}
		}
	}()
	select {
	case <-done:
	case <-time.After(s.opts.CloseTimeout):
	}
	s.conn.Close()
}

func (s *Session) start() {
	go s.writeLoop()
	go s.readLoop()
	if s.pingInterval > 0 {
		go s.keepalive()
	}
}

// ─── Public API ─────────────────────────────────────────────────────────────

// Done is closed when the session has fully closed.
func (s *Session) Done() <-chan struct{} { return s.done }

// Context is cancelled when the session closes.
func (s *Session) Context() context.Context { return s.ctx }

// Err returns nil while the session is open or after a normal (1000)
// close, and a *CloseError otherwise.
func (s *Session) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state != stateClosed || s.closeInfo.Code == CodeNormal {
		return nil
	}
	ci := s.closeInfo
	return &ci
}

// CloseInfo reports how the session closed (valid once Done is closed).
func (s *Session) CloseInfo() CloseError {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closeInfo
}

// RemoteLimits are the limits the peer declared.
func (s *Session) RemoteLimits() Limits { return s.remote }

// IsClient reports whether this side sent HELLO.
func (s *Session) IsClient() bool { return s.client }

// Stats returns connection-level (channel 0) probe statistics.
func (s *Session) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stats.Stats
}

// Channels returns the open channels.
func (s *Session) Channels() []*Channel {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*Channel
	for _, c := range s.chans {
		if c.state == chanOpen {
			out = append(out, c)
		}
	}
	slices.SortFunc(out, func(a, b *Channel) int { return int(a.id) - int(b.id) })
	return out
}

// Ping measures the round-trip time on channel 0.
func (s *Session) Ping(ctx context.Context) (time.Duration, error) {
	return s.ping(ctx, nil)
}

// OpenChannel opens a channel and waits for the peer's answer. A refusal
// is returned as a *CloseError carrying the peer's code.
func (s *Session) OpenChannel(ctx context.Context, spec ChannelSpec) (*Channel, error) {
	s.mu.Lock()
	if s.state != stateOpen {
		s.mu.Unlock()
		return nil, ErrClosed
	}
	id, ok := s.allocateIDLocked()
	if !ok {
		s.mu.Unlock()
		return nil, errors.New("xumux: no free channel IDs")
	}
	r := spec.request(id)
	c := s.newChannel(r, true, chanOpening)
	if _, err := s.enqueueLocked(controlFrame(TypeOpenChannel, r), nil, false); err != nil {
		s.finishChannelLocked(c)
		s.mu.Unlock()
		return nil, err
	}
	s.mu.Unlock()
	select {
	case <-c.opened:
		if c.openErr != nil {
			return nil, c.openErr
		}
		return c, nil
	case <-ctx.Done():
		s.mu.Lock()
		if c.state == chanOpening {
			c.abandon = true // close it as soon as it is acknowledged (core §5.2)
		} else if c.state == chanOpen {
			s.sendCloseChannelLocked(c, CodeGoingAway, "open abandoned")
		}
		s.mu.Unlock()
		return nil, ctx.Err()
	}
}

// Close closes the session gracefully: it lets in-progress sends finish
// (up to CloseTimeout), sends CLOSE, and waits for the peer's CLOSE.
func (s *Session) Close(ctx context.Context, code int, reason string) error {
	deadline := time.After(s.opts.CloseTimeout)
	for _, c := range s.Channels() {
		select {
		case c.sendSem <- struct{}{}:
			<-c.sendSem
		case <-c.done:
		case <-deadline:
		case <-ctx.Done():
		}
	}
	s.mu.Lock()
	s.beginCloseLocked(code, reason, false)
	s.mu.Unlock()
	select {
	case <-s.done:
		return nil
	case <-ctx.Done():
		s.mu.Lock()
		s.finishLocked(s.closeInfo)
		s.mu.Unlock()
		return ctx.Err()
	}
}

// ─── Receive path ───────────────────────────────────────────────────────────

func (s *Session) readLoop() {
	for {
		f, err := s.conn.ReadFrame(s.local.MaxFramePayload)
		if err != nil {
			s.readFailed(err)
			return
		}
		s.lastRx.Store(time.Now().UnixNano())
		s.mu.Lock()
		if err := s.handleFrameLocked(f); err != nil {
			s.handleErrLocked(err)
		}
		closed := s.state == stateClosed
		s.mu.Unlock()
		if closed {
			return
		}
	}
}

func (s *Session) readFailed(err error) {
	var pe *ProtocolError
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case s.state == stateClosed:
	case errors.As(err, &pe) && pe.Transport:
		s.finishLocked(CloseError{Code: pe.Code, Reason: pe.Message})
	case errors.As(err, &pe):
		// The input cannot be parsed further: send CLOSE, then finish.
		s.beginCloseLocked(pe.Code, pe.Message, false)
		s.finishAfterCloseWrittenLocked()
	case s.state == stateClosing:
		// The peer half-closed after its CLOSE (core §9.1). Our CLOSE (the
		// reply, or our own) may still be queued: let it reach the wire.
		s.finishAfterCloseWrittenLocked()
	default:
		reason := "transport closed"
		if !errors.Is(err, io.EOF) {
			reason = err.Error()
		}
		s.finishLocked(CloseError{Code: CodeGoingAway, Reason: reason, Remote: true})
	}
}

// finishAfterCloseWrittenLocked waits (unlocked, bounded by CloseTimeout)
// for the queued CLOSE to be written, then finishes the session.
func (s *Session) finishAfterCloseWrittenLocked() {
	written := s.closeWritten
	s.mu.Unlock()
	select {
	case <-written:
	case <-time.After(s.opts.CloseTimeout):
	case <-s.done:
	}
	s.mu.Lock()
	s.finishLocked(s.closeInfo)
}

func (s *Session) handleErrLocked(err error) {
	var pe *ProtocolError
	if !errors.As(err, &pe) {
		pe = protoErr(CodeProtocolError, "%v", err)
	}
	if pe.ChannelScope {
		if c := s.chans[pe.Channel]; c != nil {
			c.reasm = nil
			s.sendCloseChannelLocked(c, pe.Code, pe.Message)
		}
		return
	}
	s.beginCloseLocked(pe.Code, pe.Message, false)
}

func (s *Session) handleFrameLocked(f Frame) error {
	if err := validateFrame(f); err != nil {
		return err
	}
	switch s.state {
	case stateClosed:
		return nil
	case stateClosing:
		// After sending CLOSE, only the peer's CLOSE matters (core §9.1).
		if f.IsControl() && f.Type == TypeClose {
			s.finishLocked(s.closeInfo)
		}
		return nil
	}
	s.keepaliveAt = time.Time{}
	switch {
	case f.IsProbe():
		return s.handleProbeLocked(f)
	case f.Channel == ControlChannel:
		return s.handleControlLocked(f)
	default:
		return s.handleDataLocked(f)
	}
}

func (s *Session) handleControlLocked(f Frame) error {
	switch f.Type {
	case TypeHello, TypeWelcome:
		return protoErr(CodeProtocolError, "unexpected HELLO or WELCOME")
	case TypeOpenChannel:
		r, err := decodeChannelRequest(f.Payload)
		if err != nil {
			return err
		}
		return s.onOpenChannelLocked(r)
	case TypeChannelAck:
		a, err := decodeChannelAccept(f.Payload)
		if err != nil {
			return err
		}
		c := s.chans[a.ID]
		if c == nil || c.state != chanOpening || !c.localOpener {
			return protoErr(CodeProtocolError, "CHANNEL_ACK for channel %d, which is not opening", a.ID)
		}
		return s.ackLocked(c, a)
	case TypeChannelReject:
		r, err := decodeChannelRefusal(f.Payload)
		if err != nil {
			return err
		}
		c := s.chans[r.ID]
		if c == nil || c.state != chanOpening || !c.localOpener {
			return protoErr(CodeProtocolError, "CHANNEL_REJECT for channel %d, which is not opening", r.ID)
		}
		c.openErr = &CloseError{Code: r.Code, Reason: r.Reason, Remote: true}
		s.finishChannelLocked(c)
		return nil
	case TypeCloseChannel:
		m, err := decodeCloseChannel(f.Payload)
		if err != nil {
			return err
		}
		if m.ID == ControlChannel {
			s.sendErrorLocked(CodeProtocolError, nil, "CLOSE_CHANNEL for channel 0")
			return nil
		}
		c := s.chans[m.ID]
		if c == nil || c.state == chanOpening {
			return nil // not open or closing: ignore (core §5.4)
		}
		s.onCloseChannelLocked(c, m)
		return nil
	case TypeWindowUpdate:
		entries, err := decodeWindowUpdate(f.Payload)
		if err != nil {
			return err
		}
		return s.onWindowUpdateLocked(entries)
	case TypeClose:
		m, err := decodeClose(f.Payload)
		if err != nil {
			return err
		}
		s.beginCloseLocked(m.Code, m.Reason, true)
		return nil
	case TypeError:
		e, err := decodeError(f.Payload)
		if err != nil {
			return err
		}
		if s.opts.OnError != nil {
			go s.opts.OnError(e)
		}
		return nil
	default:
		s.sendErrorLocked(CodeUnsupported, nil, fmt.Sprintf("unknown control type %#x", f.Type))
		return nil
	}
}

func (s *Session) onOpenChannelLocked(r ChannelRequest) error {
	peerParity := uint16(0) // the server opens even IDs
	if !s.client {
		peerParity = 1
	}
	if r.ID == 0 || r.ID == ReservedChannel || r.ID%2 != peerParity || s.chans[r.ID] != nil || s.pendingRemote[r.ID] {
		return protoErr(CodeProtocolError, "invalid OPEN_CHANNEL ID %d", r.ID)
	}
	if len(s.chans)+len(s.pendingRemote) >= s.opts.MaxChannels {
		s.enqueueLocked(controlFrame(TypeChannelReject, ChannelRefusal{ID: r.ID, Code: CodeChannelLimit, Reason: "too many channels"}), nil, false)
		return nil
	}
	s.pendingRemote[r.ID] = true
	go func() {
		handler, refusal := s.decide(r)
		s.mu.Lock()
		defer s.mu.Unlock()
		delete(s.pendingRemote, r.ID)
		if s.state != stateOpen {
			return
		}
		if refusal != nil {
			s.enqueueLocked(controlFrame(TypeChannelReject, *refusal), nil, false)
			return
		}
		c := s.newChannel(r, false, chanOpen)
		c.props = effectiveProperties(r)
		c.sendCredit = int64(s.remote.InitialWindow)
		s.enqueueLocked(controlFrame(TypeChannelAck, ChannelAccept{ID: r.ID, Properties: c.props}), nil, false)
		go handler(c)
	}()
	return nil
}

// decide runs the Acceptor (not under the lock).
func (s *Session) decide(r ChannelRequest) (Handler, *ChannelRefusal) {
	if s.opts.Acceptor == nil {
		return nil, &ChannelRefusal{ID: r.ID, Code: CodeUnsupported, Reason: "channels opened by the peer are not accepted"}
	}
	h, err := s.opts.Acceptor(s.ctx, r)
	if err == nil && h == nil {
		err = errors.New("no handler")
	}
	if err != nil {
		var ref *Refusal
		if errors.As(err, &ref) {
			return nil, &ChannelRefusal{ID: r.ID, Code: ref.Code, Reason: ref.Reason}
		}
		return nil, &ChannelRefusal{ID: r.ID, Code: CodeChannelRefused, Reason: err.Error()}
	}
	return h, nil
}

// effectiveProperties: every binding implemented here is a single stream,
// so every channel is reliable and ordered (core §5.3).
func effectiveProperties(ChannelRequest) Properties {
	return Properties{Reliable: true, Ordered: true}
}

// ackLocked opens a locally opened channel, then replays frames held for it.
func (s *Session) ackLocked(c *Channel, a ChannelAccept) error {
	c.props = a.Properties
	c.state = chanOpen
	c.sendCredit = int64(s.remote.InitialWindow)
	close(c.opened)
	held := c.held
	c.held, c.heldBytes = nil, 0
	if c.abandon {
		s.sendCloseChannelLocked(c, CodeGoingAway, "open abandoned")
		return nil
	}
	for _, f := range held {
		var err error
		if f.IsProbe() {
			err = s.handleProbeLocked(f)
		} else {
			err = s.handleDataLocked(f)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// holdLocked keeps a frame that overtook the ACK or WELCOME (core §5.2).
// The peer may have sent at most our initialWindow on the channel.
func (s *Session) holdLocked(c *Channel, f Frame) error {
	if f.IsProbe() {
		if f.Type == TypePing && c.heldPings < 8 {
			c.heldPings++
			c.held = append(c.held, f)
		}
		return nil
	}
	c.heldBytes += int64(len(f.Payload))
	if c.heldBytes > int64(s.local.InitialWindow) {
		return protoErr(CodeFlowControlError, "peer sent more than initialWindow on channel %d before it was acknowledged", f.Channel)
	}
	c.held = append(c.held, f)
	return nil
}

func (s *Session) handleDataLocked(f Frame) error {
	c := s.chans[f.Channel]
	if c == nil {
		if !s.pendingRemote[f.Channel] {
			ch := f.Channel
			s.sendErrorLocked(CodeChannelNotFound, &ch, "")
		}
		return nil
	}
	if c.state == chanOpening {
		if c.localOpener {
			return s.holdLocked(c, f)
		}
		return nil
	}
	if c.state != chanOpen || c.sentClose || c.recvClose {
		return nil // closing: discard (core §5.5)
	}
	n := int64(len(f.Payload))
	if c.props.Reliable {
		if n > c.recvCredit {
			return protoErr(CodeFlowControlError, "peer exceeded credit on channel %d", f.Channel)
		}
		c.recvCredit -= n
	}
	more := f.Flags&FlagMore != 0
	if more && !(c.props.Reliable && c.props.Ordered) {
		return channelErr(f.Channel, CodeProtocolError, "MORE on a channel that cannot be fragmented")
	}
	if c.reasm != nil {
		if f.Type != c.reasm.typ {
			return channelErr(f.Channel, CodeProtocolError, "fragment type mismatch")
		}
		c.reasm.parts = append(c.reasm.parts, f.Payload)
		c.reasm.size += n
		if c.reasm.size > int64(s.local.MaxMessageSize) {
			return channelErr(f.Channel, CodeMessageTooLarge, "reassembled message too large")
		}
		if !more {
			payload := make([]byte, 0, c.reasm.size)
			for _, p := range c.reasm.parts {
				payload = append(payload, p...)
			}
			c.deliverLocked(Message{Type: c.reasm.typ, Payload: payload})
			c.reasm = nil
		}
		return nil
	}
	if more {
		if n > int64(s.local.MaxMessageSize) {
			return channelErr(f.Channel, CodeMessageTooLarge, "reassembled message too large")
		}
		c.reasm = &reassembly{typ: f.Type, parts: [][]byte{f.Payload}, size: n}
		return nil
	}
	c.deliverLocked(Message{Type: f.Type, Payload: f.Payload})
	return nil
}

func (s *Session) onWindowUpdateLocked(entries []windowEntry) error {
	for _, e := range entries {
		if e.Channel == ControlChannel {
			return protoErr(CodeProtocolError, "WINDOW_UPDATE for channel 0")
		}
		c := s.chans[e.Channel]
		if c == nil || c.state != chanOpen || c.sentClose || c.recvClose {
			continue // closing or unknown: ignore (core §6.1)
		}
		if !c.props.Reliable {
			return protoErr(CodeProtocolError, "WINDOW_UPDATE for unreliable channel %d", e.Channel)
		}
		if c.sendCredit+int64(e.Increment) > MaxWindow {
			return protoErr(CodeFlowControlError, "WINDOW_UPDATE raises credit above 2^31-1 on channel %d", e.Channel)
		}
		c.sendCredit += int64(e.Increment)
		c.signalCreditLocked()
	}
	return nil
}

func (s *Session) handleProbeLocked(f Frame) error {
	token, ok := probeToken(f.Payload)
	if !ok || (f.Type != TypePing && f.Type != TypePong) {
		return nil // malformed or reserved probe: discard (core §2.2, §8.1)
	}
	pings, stats := s.pings, &s.stats
	if f.Channel != ControlChannel {
		c := s.chans[f.Channel]
		if c == nil {
			return nil
		}
		if c.state == chanOpening && c.localOpener {
			return s.holdLocked(c, f)
		}
		if c.state != chanOpen || c.sentClose || c.recvClose {
			return nil
		}
		pings, stats = c.pings, &c.stats
	}
	if f.Type == TypePing {
		s.enqueueLocked(probeFrame(f.Channel, TypePong, token), nil, false)
		return nil
	}
	if p := pings[token]; p != nil {
		delete(pings, token)
		rtt := time.Since(p.sent)
		stats.sample(rtt)
		p.rtt <- rtt
	}
	return nil
}

// ─── Channel close ──────────────────────────────────────────────────────────

func (s *Session) onCloseChannelLocked(c *Channel, m closeChannelMsg) {
	c.recvClose = true
	c.reasm = nil
	if c.closeInfo == nil {
		c.closeInfo = &CloseError{Code: m.Code, Reason: m.Reason, Remote: true}
	}
	if !c.sentClose {
		s.sendCloseChannelLocked(c, CodeNormal, "")
		return
	}
	if c.closeWritten {
		s.finishChannelLocked(c)
	}
}

func (s *Session) sendCloseChannelLocked(c *Channel, code int, reason string) {
	if c.sentClose || c.state == chanClosed || c.state == chanOpening {
		return
	}
	c.sentClose = true
	c.state = chanClosing
	if c.closeInfo == nil {
		c.closeInfo = &CloseError{Code: code, Reason: reason}
	}
	c.unreturned = 0
	s.dropQueuedLocked(c.id, errChannelClosed(c.id))
	c.signalCreditLocked()
	m := closeChannelMsg{ID: c.id}
	if code != CodeNormal || reason != "" {
		m.Code, m.Reason = code, reason
	}
	_, err := s.enqueueLocked(controlFrame(TypeCloseChannel, m), func(error) {
		s.mu.Lock()
		defer s.mu.Unlock()
		c.closeWritten = true
		if c.recvClose {
			s.finishChannelLocked(c)
		}
	}, false)
	if err != nil {
		s.finishChannelLocked(c)
	}
}

func (s *Session) finishChannelLocked(c *Channel) {
	if c.state == chanClosed {
		return
	}
	wasOpening := c.state == chanOpening
	c.state = chanClosed
	if s.chans[c.id] == c {
		delete(s.chans, c.id)
	}
	for tok, p := range c.pings {
		delete(c.pings, tok)
		close(p.rtt)
	}
	if c.closeInfo == nil {
		c.closeInfo = &CloseError{Code: CodeNormal}
	}
	if wasOpening {
		if c.openErr == nil {
			c.openErr = c.closeInfo
		}
		close(c.opened)
	}
	close(c.done)
	c.signalCreditLocked()
	c.signalInboxLocked()
}

func errChannelClosed(id uint16) error {
	return fmt.Errorf("xumux: channel %d closed: %w", id, ErrClosed)
}

// ─── Connection close ───────────────────────────────────────────────────────

// beginCloseLocked sends CLOSE (core §9.1). For a remote close, CLOSE is the
// reply and the session finishes once it is written.
func (s *Session) beginCloseLocked(code int, reason string, remote bool) {
	if s.state != stateOpen {
		return
	}
	s.state = stateClosing
	s.closeInfo = CloseError{Code: code, Reason: reason, Remote: remote}
	for id := range s.data {
		s.dropQueuedLocked(id, ErrClosed)
	}
	for _, c := range s.chans {
		c.signalCreditLocked()
	}
	it := &outItem{f: controlFrame(TypeClose, closeMsg{Code: code, Reason: reason}), isClose: true}
	if remote {
		it.f = controlFrame(TypeClose, closeMsg{Code: code})
		it.after = func(error) {
			s.mu.Lock()
			defer s.mu.Unlock()
			s.finishLocked(s.closeInfo)
		}
	}
	s.urgent = append(s.urgent, it)
	s.signalWriter()
	s.closeTimer = time.AfterFunc(s.opts.CloseTimeout, func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.finishLocked(s.closeInfo)
	})
}

func (s *Session) finishLocked(info CloseError) {
	if s.state == stateClosed {
		return
	}
	s.state = stateClosed
	s.closeInfo = info
	if s.closeTimer != nil {
		s.closeTimer.Stop()
	}
	s.cancel()
	for _, it := range s.urgent {
		it.finish(ErrClosed)
	}
	s.urgent = nil
	for id := range s.data {
		s.dropQueuedLocked(id, ErrClosed)
	}
	for tok, p := range s.pings {
		delete(s.pings, tok)
		close(p.rtt)
	}
	for _, c := range s.chans {
		ci := info
		c.closeInfo = &ci
		if c.state == chanOpening {
			c.openErr = &ci
		}
		s.finishChannelLocked(c)
	}
	close(s.done)
	s.signalWriter()
	go s.conn.Close()
}

// ─── Outbound scheduler ─────────────────────────────────────────────────────

// enqueueLocked queues a frame. Control and probe frames (dataChannel false)
// go before data, which is sent round-robin across channels.
func (s *Session) enqueueLocked(f Frame, after func(error), dataChannel bool) (*outItem, error) {
	if s.state != stateOpen {
		return nil, ErrClosed
	}
	it := &outItem{f: f, after: after}
	if dataChannel {
		it.done = make(chan error, 1)
		q := s.data[f.Channel]
		if len(q) == 0 {
			s.rr = append(s.rr, f.Channel)
		}
		s.data[f.Channel] = append(q, it)
	} else {
		s.urgent = append(s.urgent, it)
	}
	s.signalWriter()
	return it, nil
}

func (s *Session) sendErrorLocked(code int, ch *uint16, reason string) {
	s.enqueueLocked(controlFrame(TypeError, ErrorMessage{Code: code, Channel: ch, Reason: reason}), nil, false)
}

func (s *Session) dropQueuedLocked(ch uint16, err error) {
	q, ok := s.data[ch]
	if !ok {
		return
	}
	delete(s.data, ch)
	if i := slices.Index(s.rr, ch); i >= 0 {
		s.rr = slices.Delete(s.rr, i, i+1)
	}
	for _, it := range q {
		it.finish(err)
	}
}

func (s *Session) signalWriter() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *Session) nextLocked() *outItem {
	if len(s.urgent) > 0 {
		it := s.urgent[0]
		s.urgent = s.urgent[1:]
		return it
	}
	if s.windowDirty && s.state == stateOpen {
		s.windowDirty = false
		var entries []windowEntry
		for id, c := range s.chans {
			if c.unreturned > 0 && c.state == chanOpen && !c.sentClose {
				entries = append(entries, windowEntry{id, uint32(c.unreturned)})
				c.recvCredit += c.unreturned
				c.unreturned = 0
			}
		}
		if len(entries) > 0 {
			return &outItem{f: windowUpdateFrame(entries)}
		}
	}
	if len(s.rr) == 0 {
		return nil
	}
	id := s.rr[0]
	s.rr = s.rr[1:]
	q := s.data[id]
	it := q[0]
	if len(q) > 1 {
		s.data[id] = q[1:]
		s.rr = append(s.rr, id)
	} else {
		delete(s.data, id)
	}
	return it
}

func (s *Session) writeLoop() {
	defer func() {
		s.mu.Lock()
		s.writeStopped = true
		select {
		case <-s.closeWritten:
		default:
			close(s.closeWritten)
		}
		s.mu.Unlock()
	}()
	for {
		s.mu.Lock()
		it := s.nextLocked()
		for it == nil {
			if s.state == stateClosed {
				s.mu.Unlock()
				return
			}
			s.mu.Unlock()
			<-s.wake
			s.mu.Lock()
			it = s.nextLocked()
		}
		s.mu.Unlock()

		err := s.conn.WriteFrame(it.f)
		it.finish(err)
		if err != nil {
			s.mu.Lock()
			info := s.closeInfo
			if s.state == stateOpen {
				info = CloseError{Code: CodeGoingAway, Reason: "transport write failed: " + err.Error()}
			}
			s.finishLocked(info)
			s.mu.Unlock()
			return
		}
		if it.isClose {
			_ = s.conn.CloseWrite()
			return
		}
	}
}

// ─── Probes and keepalive ───────────────────────────────────────────────────

func (s *Session) nextTokenLocked(pending map[uint32]*pendingPing) uint32 {
	t := uint32(time.Since(s.epoch).Microseconds())
	for t == s.lastToken || pending[t] != nil {
		t++
	}
	s.lastToken = t
	return t
}

func (s *Session) ping(ctx context.Context, c *Channel) (time.Duration, error) {
	s.mu.Lock()
	pings, stats, id := s.pings, &s.stats, uint16(ControlChannel)
	if c != nil {
		if c.state != chanOpen || c.sentClose || c.recvClose {
			s.mu.Unlock()
			return 0, errChannelClosed(c.id)
		}
		pings, stats, id = c.pings, &c.stats, c.id
	}
	if s.state != stateOpen {
		s.mu.Unlock()
		return 0, ErrClosed
	}
	token := s.nextTokenLocked(pings)
	p := &pendingPing{sent: time.Now(), rtt: make(chan time.Duration, 1)}
	pings[token] = p
	stats.ProbesSent++
	s.enqueueLocked(probeFrame(id, TypePing, token), nil, false)
	s.mu.Unlock()

	select {
	case rtt, ok := <-p.rtt:
		if !ok {
			return 0, ErrClosed
		}
		return rtt, nil
	case <-ctx.Done():
		s.mu.Lock()
		if pings[token] == p {
			delete(pings, token)
			stats.ProbesLost++
		}
		s.mu.Unlock()
		return 0, ctx.Err()
	}
}

func (s *Session) keepalive() {
	tick := min(s.pingInterval, s.pingTimeout) / 4
	tick = max(tick, 10*time.Millisecond)
	t := time.NewTicker(tick)
	defer t.Stop()
	for {
		select {
		case <-s.done:
			return
		case <-t.C:
		}
		now := time.Now()
		last := time.Unix(0, s.lastRx.Load())
		s.mu.Lock()
		switch {
		case s.state != stateOpen:
		case !s.keepaliveAt.IsZero():
			if last.Before(s.keepaliveAt) && now.Sub(s.keepaliveAt) >= s.pingTimeout {
				// The peer is gone; close the transport without waiting (core §8.2).
				s.finishLocked(CloseError{Code: CodeKeepaliveTimeout, Reason: "peer stopped responding"})
			}
		case now.Sub(last) >= s.pingInterval:
			s.keepaliveAt = now
			s.enqueueLocked(probeFrame(ControlChannel, TypePing, s.nextTokenLocked(s.pings)), nil, false)
		}
		s.mu.Unlock()
	}
}

// allocateIDLocked picks the lowest free ID of this side's parity (core §5.1).
func (s *Session) allocateIDLocked() (uint16, bool) {
	start := uint32(2)
	if s.client {
		start = 1
	}
	for id := start; id <= MaxChannelID; id += 2 {
		if s.chans[uint16(id)] == nil {
			return uint16(id), true
		}
	}
	return 0, false
}

// Stats are probe statistics. SRTT and RTTVar follow RFC 6298.
type Stats struct {
	RTT, SRTT, RTTVar      time.Duration
	ProbesSent, ProbesLost int
}

type statsRecorder struct{ Stats }

func (r *statsRecorder) sample(rtt time.Duration) {
	r.RTT = rtt
	if r.SRTT == 0 && r.RTTVar == 0 {
		r.SRTT, r.RTTVar = rtt, rtt/2
		return
	}
	d := r.SRTT - rtt
	if d < 0 {
		d = -d
	}
	r.RTTVar = (3*r.RTTVar + d) / 4
	r.SRTT = (7*r.SRTT + rtt) / 8
}
