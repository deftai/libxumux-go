package xumux

import (
	"context"
	"encoding/json"
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

// errKeepaliveTimeout is the base-path loss a keepalive timeout reports.
var errKeepaliveTimeout = errors.New("xumux: keepalive timeout")

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
	// MaxChannelWindow is how far auto-tuning may grow one channel's receive
	// window (default 16 MiB). Each channel starts at Limits.InitialWindow;
	// when the peer uses a whole window in under two round trips, so that the
	// window rather than the link limits it, the window doubles. A value at or
	// below Limits.InitialWindow, or negative, disables auto-tuning.
	MaxChannelWindow int64
	// ConnectionWindow limits auto-tuning across the connection (default
	// 64 MiB): no window grows once the sum of all channel receive windows
	// would exceed it. Initial windows are always granted, even on top of
	// windows that already grew, so the total is at most ConnectionWindow +
	// open channels × InitialWindow. Windows do
	// not shrink; a channel's window leaves the sum when it closes, though
	// data it received that the application has not read yet stays buffered.
	ConnectionWindow int64
	// MaxControlQueue caps the bytes of control and probe frames (CHANNEL_ACK,
	// WINDOW_UPDATE, PONG, ERROR, ...) queued for sending, default 1 MiB. They
	// pile up only if the peer stops reading while it keeps sending frames
	// that need a reply; past the cap the connection closes with 4004. Data
	// frames are bounded by flow control instead. Negative: no cap.
	MaxControlQueue int64
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
	switch {
	case o.MaxChannelWindow == 0:
		o.MaxChannelWindow = DefaultMaxChannelWindow
	case o.MaxChannelWindow < 0:
		o.MaxChannelWindow = int64(o.Limits.InitialWindow)
	}
	o.MaxChannelWindow = min(o.MaxChannelWindow, MaxWindow)
	if o.ConnectionWindow <= 0 {
		o.ConnectionWindow = DefaultConnectionWindow
	}
	if o.MaxControlQueue == 0 {
		o.MaxControlQueue = DefaultMaxControlQueue
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
	data    bool     // queued in data (round-robin) rather than urgent
	ch      *Channel // the channel instance of a data frame
	cost    int64    // bytes counted against MaxControlQueue
	// session-resume/1 bookkeeping: the channel instance this CLOSE_CHANNEL
	// closes, and the token this RESUME_TOKEN carries.
	closeOf *Channel
	tokenOf *issuedToken
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
	lost bool // abandoned because the base path was lost (set before rtt closes)
}

// errPathLost is returned by Ping when the base path was lost, and the
// session detached, before the PONG arrived.
var errPathLost = errors.New("xumux: base path lost before the PONG arrived")

// basePath is one instance of the base path (core §1.2): a FrameConn with
// its own read and write loops. A session normally has one for its whole
// life. An extension that keeps the connection alive when the base path is
// lost (core §11.1) detaches it instead of closing the session, and later
// attaches a new one under the same session.
type basePath struct {
	conn         FrameConn
	wake         chan struct{} // wakes this path's writer; capacity 1
	stop         chan struct{} // closed when the path is detached
	closeWritten chan struct{} // closed when this path's writer stops
	// preamble is written before anything queued: an extension's answer and
	// retransmissions on a newly attached path. Guarded by s.mu.
	preamble []Frame
}

func newBasePath(conn FrameConn) *basePath {
	return &basePath{conn: conn, wake: make(chan struct{}, 1), stop: make(chan struct{}), closeWritten: make(chan struct{})}
}

// pathHooks is the seam through which an extension keeps a session alive
// across the loss of its base path (core §11.1). Every method is called with
// s.mu held. A session without hooks ends when its transport does.
type pathHooks interface {
	// pathLost reports whether to detach, rather than close, a session whose
	// base path failed without a CLOSE exchange (err says how).
	pathLost(err error) bool
	// writing is called as the writer takes it from the queue to write it to
	// the base path. It reports whether the extension keeps a copy of the
	// frame to send again on the next path: such a frame counts as sent even
	// if the write fails because the path was lost. Other frames that fail
	// that way are queued again, at the front.
	writing(it *outItem) (retained bool)
}

// Session is one xumux connection. Create it with [Connect] or [Accept].
type Session struct {
	// path is the attached base path, or nil while detached. It is set before
	// the handshake and changes only under mu.
	path  *basePath
	hooks pathHooks // nil: transport loss ends the session
	// resume is session-resume/1's state, when it was negotiated (also hooks).
	resume *resumeState
	opts   Options
	client bool
	local  Limits
	remote Limits
	epoch  time.Time
	ctx    context.Context
	cancel context.CancelFunc

	requestedExt []string // client: extensions asked for in HELLO
	checkExt     func(name string, params json.RawMessage) error
	// Negotiated extensions and their parameters (core §3.8), set by the
	// handshake and read-only afterwards.
	extensions     []string
	localExtParams map[string]json.RawMessage
	peerExtParams  map[string]json.RawMessage
	pingInterval   time.Duration // effective; 0 = disabled
	pingTimeout    time.Duration
	lastRx         atomic.Int64 // UnixNano of the last received frame

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
	windowTotal   int64     // sum of channel receive windows (auto-tuning budget)
	rttProbing    bool      // an auto-tuning RTT probe is in flight
	rttProbedAt   time.Time // when auto-tuning last sent one

	// Outbound scheduler, guarded by mu.
	urgent      []*outItem
	urgentBytes int64 // bytes queued in urgent, for MaxControlQueue
	data        map[uint16][]*outItem
	rr          []uint16
	windowDirty bool

	done chan struct{}
}

func newSession(conn FrameConn, opts Options, client bool) *Session {
	opts = opts.withDefaults()
	ctx, cancel := context.WithCancel(context.Background())
	s := &Session{
		path: newBasePath(conn), opts: opts, client: client, local: opts.Limits, epoch: time.Now(),
		ctx: ctx, cancel: cancel,
		chans: map[uint16]*Channel{}, pendingRemote: map[uint16]bool{}, pings: map[uint32]*pendingPing{},
		data: map[uint16][]*outItem{},
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
	// ExtensionParams are sent in HELLO as extensionParams (core §3.8):
	// parameters for requested extensions, keyed by extension name. Each
	// value must be a valid JSON value as its extension defines it. Keys
	// for extensions not in Extensions are not sent.
	ExtensionParams map[string]json.RawMessage
	// CheckExtensionParams, if set, validates the server's parameters for
	// each extension WELCOME accepts. params is WELCOME's extensionParams
	// value for name, or nil if it has none. An error closes the connection
	// with 4001 (core §3.8), and Connect fails.
	CheckExtensionParams func(name string, params json.RawMessage) error
	Auth                 map[string]any
	// Channels to open during the handshake.
	Channels []ChannelSpec
	// Resume, if set, requests session-resume/1 and, if the server accepts
	// it, keeps the session alive when the transport is lost: the session
	// detaches and resumes on a new transport from Resume.Redial.
	Resume *ResumeClient
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
		Version: Version, Application: cfg.Application, Extensions: cfg.Extensions,
		ExtensionParams: paramsFor(cfg.ExtensionParams, cfg.Extensions),
		Limits:          s.local, PingInterval: interval.Seconds(), PingTimeout: s.opts.PingTimeout.Seconds(), Auth: cfg.Auth,
	}
	if cfg.Resume != nil {
		if cfg.Resume.Redial == nil {
			conn.Close()
			return nil, nil, errors.New("xumux: ClientConfig.Resume needs Redial")
		}
		if !slices.Contains(cfg.Extensions, ExtensionSessionResume) {
			hello.Extensions = append(slices.Clone(cfg.Extensions), ExtensionSessionResume)
		}
	}
	s.requestedExt, s.checkExt = hello.Extensions, cfg.CheckExtensionParams
	order := make([]*Channel, len(cfg.Channels))
	for i, spec := range cfg.Channels {
		r := spec.request(uint16(2*i + 1))
		hello.Channels = append(hello.Channels, r)
		order[i] = s.newChannel(r, true, chanOpening)
	}

	helloFrame, err := encodeControl(TypeHello, hello)
	if err != nil {
		conn.Close()
		return nil, nil, err
	}

	stop := s.handshakeDeadline(ctx)
	if err := conn.WriteFrame(helloFrame); err != nil {
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
	s.extensions = slices.Clone(w.Extensions)
	s.localExtParams = paramsFor(hello.ExtensionParams, w.Extensions)
	s.peerExtParams = paramsFor(w.ExtensionParams, w.Extensions)
	s.pingInterval = time.Duration(w.PingInterval * float64(time.Second))
	s.pingTimeout = time.Duration(w.PingTimeout * float64(time.Second))
	if cfg.Resume != nil && slices.Contains(w.Extensions, ExtensionSessionResume) {
		s.mu.Lock()
		s.resume = newClientResume(s, cfg, opts)
		s.hooks = s.resume
		s.mu.Unlock()
	}

	// Apply WELCOME's answers before the reader starts, so that every frame
	// on a HELLO channel is handled (and, with session-resume/1, counted)
	// with the channel's effective properties.
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
	s.start()
	return s, res, nil
}

// readWelcome reads frames until WELCOME and validates it (core §3.1, §3.3).
func (s *Session) readWelcome() (Welcome, error) {
	for {
		f, err := s.path.conn.ReadFrame(s.local.MaxFramePayload)
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
		// Keys for extensions WELCOME does not accept are ignored (core §3.8).
		if s.checkExt != nil {
			if err := s.checkExt(e, w.ExtensionParams[e]); err != nil {
				return protoErr(CodeInvalidMessage, "WELCOME extensionParams for %q: %v", e, err)
			}
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
	// NegotiateExtension, if set, is called after Authorize for each
	// extension in Extensions that the client requested. params is the
	// client's extensionParams value for name, or nil if it sent none.
	// Return the parameters to send for name in WELCOME (nil for none), or
	// an error to leave the extension out of WELCOME, as a server does with
	// parameters it cannot accept (core §3.8). If NegotiateExtension is nil,
	// every requested extension in Extensions is accepted without
	// parameters. The client's parameters for extensions that are not
	// accepted are never passed on.
	NegotiateExtension func(ctx context.Context, name string, params json.RawMessage) (json.RawMessage, error)
	// Authorize may refuse the connection. A *Refusal chooses the code,
	// any other error refuses with 4000 (AUTH_FAILED).
	Authorize func(ctx context.Context, h *Hello) error
	// FirstFrames lets an extension claim the first frame of a transport
	// (core §3.1): a control message, of a type in 0x30–0x7F allocated to
	// the extension, that continues an existing connection on a new
	// transport instead of starting one with HELLO (session-resume/1's
	// RESUME, for example). With no handler for a first frame's type, a
	// first frame other than HELLO closes the transport with 1002.
	FirstFrames map[byte]FirstFrameHandler
	// Resume, if set, enables session-resume/1: WELCOME accepts it when the
	// client asks, and a RESUME on a new transport continues the detached
	// (or still attached) session it names. Share one ResumeServer across
	// every Accept. It takes the RESUME first frame (0x31) over FirstFrames.
	Resume *ResumeServer
	// AuthorizeResume authenticates a RESUME (session-resume §8): req is its
	// version and auth, s the session it would continue. If the server
	// authenticated the original HELLO (with Authorize, or by the transport),
	// it must check here that the identity is the same, by the same means.
	// A *Refusal chooses the code; any other error refuses with 4000. If it
	// is nil and Authorize is set, every RESUME is refused with 4000, so a
	// token alone never continues an authenticated session.
	AuthorizeResume func(ctx context.Context, req *ResumeRequest, s *Session) error
}

// FirstFrameHandler takes over a transport whose first frame f is an
// extension-defined first frame (core §3.1). It sends the answer the
// extension defines, which takes the place of WELCOME, and from then on
// owns conn. ctx expires at the handshake timeout, which bounds the answer
// as it does WELCOME: if the handler has not returned by then, conn is
// closed and Accept fails with ctx's error.
//
// If it returns nil, Accept returns [ErrFirstFrameHandled] and no Session.
// If it returns a *Refusal, Accept refuses the connection with CLOSE and
// that code (the extension's own codes are in 4100–4499); any other error
// closes conn without CLOSE. In both cases the handler must not have
// written to conn.
type FirstFrameHandler func(ctx context.Context, conn FrameConn, f Frame) error

// ErrFirstFrameHandled is returned by Accept when a [FirstFrameHandler]
// took the transport: the connection it continues is not a new Session.
var ErrFirstFrameHandled = errors.New("xumux: transport taken by an extension's first-frame handler")

// MinExtensionControlType and MaxExtensionControlType bound the control
// types an extension may define (core §11.1).
const (
	MinExtensionControlType byte = 0x30
	MaxExtensionControlType byte = 0x7F
)

// paramsFor returns copies of the entries of params for the extensions in
// names, or nil if there are none.
func paramsFor(params map[string]json.RawMessage, names []string) map[string]json.RawMessage {
	var out map[string]json.RawMessage
	for _, e := range names {
		if p, ok := params[e]; ok {
			if out == nil {
				out = map[string]json.RawMessage{}
			}
			out[e] = slices.Clone(p)
		}
	}
	return out
}

// Accept performs the server handshake on conn. HELLO channels are
// decided by opts.Acceptor before WELCOME is sent. On failure conn is closed.
func Accept(ctx context.Context, conn FrameConn, cfg ServerConfig, opts Options) (*Session, *Hello, error) {
	s := newSession(conn, opts, false)
	if err := checkLimits(s.local); err != nil {
		conn.Close()
		return nil, nil, fmt.Errorf("xumux: invalid local limits: %w", err)
	}
	for typ := range cfg.FirstFrames {
		if typ < MinExtensionControlType || typ > MaxExtensionControlType {
			conn.Close()
			return nil, nil, fmt.Errorf("xumux: FirstFrames type %#x is outside 0x30-0x7F", typ)
		}
	}
	started := time.Now()
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
		h := cfg.FirstFrames[f.Type]
		if cfg.Resume != nil && f.Type == TypeResume {
			h = cfg.Resume.handler(cfg)
		}
		if h != nil && f.IsControl() {
			return nil, nil, s.handOff(ctx, started, h, f)
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
		builtin := e == ExtensionSessionResume && cfg.Resume != nil
		if (!builtin && !slices.Contains(cfg.Extensions, e)) || slices.Contains(welcome.Extensions, e) {
			continue
		}
		// session-resume/1 defines no extensionParams (§1): its key is ignored.
		if cfg.NegotiateExtension != nil && !builtin {
			p, err := cfg.NegotiateExtension(ctx, e, h.ExtensionParams[e])
			if err != nil {
				continue
			}
			if p != nil {
				if welcome.ExtensionParams == nil {
					welcome.ExtensionParams = map[string]json.RawMessage{}
				}
				welcome.ExtensionParams[e] = p
			}
		}
		welcome.Extensions = append(welcome.Extensions, e)
	}
	type acceptedChannel struct {
		req     ChannelRequest
		handler Handler
		ch      *Channel
	}
	var open []acceptedChannel
	for _, r := range h.Channels {
		if len(open) >= s.opts.MaxChannels {
			// No reason: a long list of refusals must still fit in WELCOME.
			welcome.Rejected = append(welcome.Rejected, ChannelRefusal{ID: r.ID, Code: CodeChannelLimit})
			continue
		}
		handler, refusal := s.decide(r)
		if refusal != nil {
			welcome.Rejected = append(welcome.Rejected, *refusal)
			continue
		}
		welcome.Channels = append(welcome.Channels, ChannelAccept{ID: r.ID, Properties: effectiveProperties(r)})
		open = append(open, acceptedChannel{req: r, handler: handler})
	}
	if s.opts.PingInterval > 0 {
		s.pingInterval, s.pingTimeout = s.opts.PingInterval, s.opts.PingTimeout
		welcome.PingInterval, welcome.PingTimeout = s.pingInterval.Seconds(), s.pingTimeout.Seconds()
	} else {
		welcome.PingTimeout = s.opts.PingTimeout.Seconds()
	}
	s.remote = h.Limits
	s.extensions = welcome.Extensions
	s.localExtParams = paramsFor(welcome.ExtensionParams, welcome.Extensions)
	s.peerExtParams = paramsFor(h.ExtensionParams, welcome.Extensions)
	for i, a := range open {
		c := s.newChannel(a.req, false, chanOpen)
		c.props = effectiveProperties(a.req)
		c.sendCredit = int64(s.remote.InitialWindow)
		open[i].ch = c
	}
	welcomeFrame, err := encodeControl(TypeWelcome, welcome)
	if err == nil {
		err = conn.WriteFrame(welcomeFrame)
	} else {
		// NegotiateExtension's parameters do not encode, or WELCOME does not
		// fit in 64 KiB with them: refuse, as a server shutting down would.
		s.tryClose(CodeGoingAway, "server cannot encode WELCOME")
	}
	if err != nil {
		conn.Close()
		s.mu.Lock()
		for _, a := range open {
			s.discardHandlerLocked(a.ch, a.handler)
		}
		s.mu.Unlock()
		return nil, nil, err
	}
	if cfg.Resume != nil && slices.Contains(welcome.Extensions, ExtensionSessionResume) {
		// The server's first frame after WELCOME is RESUME_TOKEN (§2).
		s.mu.Lock()
		s.resume = newServerResume(s, cfg.Resume)
		s.hooks = s.resume
		s.resume.sendTokenLocked()
		s.mu.Unlock()
	}
	s.start()
	// Run every accepted handler, even if the peer has already gone and the
	// reader has closed its channel: the handler's cleanup releases the
	// target the Acceptor opened.
	for _, a := range open {
		go a.handler(a.ch)
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

// handOff passes a transport whose first frame is an extension's to that
// extension's handler (core §3.1).
func (s *Session) handOff(ctx context.Context, started time.Time, h FirstFrameHandler, f Frame) error {
	hctx, cancel := context.WithDeadline(ctx, started.Add(s.opts.HandshakeTimeout))
	defer cancel()
	// The answer is bounded like WELCOME: if ctx expires before the handler
	// returns, the transport is closed, which also unblocks a handler stuck
	// writing to it.
	stop := context.AfterFunc(hctx, func() { s.path.conn.Close() })
	err := h(hctx, s.path.conn, f)
	if !stop() {
		// The deadline closed the transport; whatever the handler saw, the
		// cause is the deadline.
		if err == nil {
			return fmt.Errorf("xumux: first-frame answer: %w", context.Cause(hctx))
		}
		return fmt.Errorf("xumux: first-frame answer: %w: %w", context.Cause(hctx), err)
	}
	if err == nil {
		return ErrFirstFrameHandled
	}
	var r *Refusal
	if errors.As(err, &r) {
		return s.abortHandshake(&ProtocolError{Code: r.Code, Message: r.Reason})
	}
	s.path.conn.Close()
	return err
}

// handshakeDeadline closes the transport if the handshake outlives the
// timeout or ctx. The returned stop reports false if it already fired.
func (s *Session) handshakeDeadline(ctx context.Context) (stop func() bool) {
	var fired atomic.Bool
	fire := func() {
		fired.Store(true)
		if !s.client {
			// Nothing else has been written yet, so this cannot interleave (core §3.1).
			_ = s.path.conn.WriteFrame(controlFrame(TypeClose, closeMsg{Code: CodeHelloTimeout, Reason: "no HELLO received"}))
		}
		s.path.conn.Close()
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
		s.path.conn.Close()
		return err
	}
	if pe.Transport {
		s.path.conn.Close()
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
	_ = s.path.conn.WriteFrame(controlFrame(TypeClose, closeMsg{Code: m.Code}))
	_ = s.path.conn.CloseWrite()
	s.path.conn.Close()
	return &CloseError{Code: m.Code, Reason: m.Reason, Remote: true}
}

// tryClose sends CLOSE, then waits briefly for the peer's CLOSE or EOF.
func (s *Session) tryClose(code int, reason string) {
	_ = s.path.conn.WriteFrame(controlFrame(TypeClose, closeMsg{Code: code, Reason: reason}))
	_ = s.path.conn.CloseWrite()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			f, err := s.path.conn.ReadFrame(s.local.MaxFramePayload)
			if err != nil || (f.IsControl() && f.Type == TypeClose) {
				return
			}
		}
	}()
	select {
	case <-done:
	case <-time.After(s.opts.CloseTimeout):
	}
	s.path.conn.Close()
}

func (s *Session) start() {
	s.runPath(s.path)
	if s.pingInterval > 0 {
		go s.keepalive()
	}
}

func (s *Session) runPath(p *basePath) {
	go s.writeLoop(p)
	go s.readLoop(p)
}

// ─── Base path: detach and attach ───────────────────────────────────────────

// lostLocked handles the loss of base path p while the session is open: it
// detaches p and reports true if the hooks keep the session alive.
func (s *Session) lostLocked(p *basePath, err error) bool {
	if p == nil || s.path != p || s.state != stateOpen || s.hooks == nil || !s.hooks.pathLost(err) {
		return false
	}
	s.detachLocked(p)
	return true
}

// detachLocked takes base path p off the session without closing the
// session (core §11.1): its loops stop, its transport is closed without a
// CLOSE, and the session keeps its channels and queues until a new path is
// attached. Outstanding PINGs are abandoned, not counted as lost, and
// keepalive is suspended (session-resume §6).
func (s *Session) detachLocked(p *basePath) {
	if p == nil || s.path != p {
		return
	}
	s.path = nil
	close(p.stop)
	go p.conn.Close()
	s.keepaliveAt = time.Time{}
	abandon := func(pings map[uint32]*pendingPing) {
		for tok, pp := range pings {
			delete(pings, tok)
			pp.lost = true
			close(pp.rtt)
		}
	}
	abandon(s.pings)
	for _, c := range s.chans {
		abandon(c.pings)
	}
}

// attachLocked puts conn under the session as its base path, replacing the
// current one if there is one (which is detached first, as on a takeover).
// preamble is written before anything queued.
func (s *Session) attachLocked(conn FrameConn, preamble []Frame) error {
	if s.state != stateOpen {
		return ErrClosed
	}
	if s.path != nil {
		s.detachLocked(s.path)
	}
	p := newBasePath(conn)
	p.preamble = preamble
	s.path = p
	s.lastRx.Store(time.Now().UnixNano())
	s.keepaliveAt = time.Time{}
	s.runPath(p)
	s.signalWriter()
	return nil
}

// detachedNow reports whether the session is open without a base path.
func (s *Session) detachedNow() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state == stateOpen && s.path == nil
}

// requeueLocked puts back, at the front of its queue, a frame whose write
// failed because the base path was lost, so that the next path sends it.
// It reports false for a data frame whose channel has closed meanwhile:
// that frame must not be sent after the CLOSE_CHANNEL.
func (s *Session) requeueLocked(it *outItem) bool {
	defer s.signalWriter()
	if it.data {
		if c := it.ch; c == nil || s.chans[c.id] != c || c.state != chanOpen || c.sentClose || c.recvClose {
			return false
		}
		q := s.data[it.f.Channel]
		if len(q) == 0 {
			s.rr = append([]uint16{it.f.Channel}, s.rr...)
		}
		s.data[it.f.Channel] = append([]*outItem{it}, q...)
		return true
	}
	s.urgentBytes += it.cost
	s.urgent = append([]*outItem{it}, s.urgent...)
	return true
}

// ─── Public API ─────────────────────────────────────────────────────────────

// Extensions returns the extensions negotiated in the handshake: those
// WELCOME accepted (core §3.6).
func (s *Session) Extensions() []string { return slices.Clone(s.extensions) }

// ExtensionParams returns the parameters each side sent for the accepted
// extension name (core §3.8): local is what this side sent in its HELLO or
// WELCOME, peer what the other side sent. Either is nil if that side sent
// none. Parameters for extensions that were not accepted are not kept, so
// both are nil for those.
func (s *Session) ExtensionParams(name string) (local, peer json.RawMessage) {
	return slices.Clone(s.localExtParams[name]), slices.Clone(s.peerExtParams[name])
}

// Detached reports whether the session is open but has no transport: with
// session-resume/1, between losing the transport and resuming on a new one.
func (s *Session) Detached() bool { return s.detachedNow() }

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
	// One deadline for all the waits: closed, so every wait sees it.
	deadline := make(chan struct{})
	t := time.AfterFunc(s.opts.CloseTimeout, func() { close(deadline) })
	defer t.Stop()
	for _, c := range s.Channels() {
		if s.detachedNow() {
			// No transport: sends in progress cannot finish, and closing
			// ends the session at once.
			break
		}
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

func (s *Session) readLoop(p *basePath) {
	for {
		f, err := p.conn.ReadFrame(s.local.MaxFramePayload)
		if err != nil {
			s.readFailed(p, err)
			return
		}
		s.mu.Lock()
		if s.path != p {
			// Detached (or taken over): stop reading this transport.
			s.mu.Unlock()
			return
		}
		s.lastRx.Store(time.Now().UnixNano())
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

func (s *Session) readFailed(p *basePath, err error) {
	var pe *ProtocolError
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case s.path != p:
		// A path that was already detached.
	case s.state == stateClosed:
	case errors.As(err, &pe) && pe.Transport:
		s.finishLocked(CloseError{Code: pe.Code, Reason: pe.Message})
	case errors.As(err, &pe):
		// The input cannot be parsed further: send CLOSE, then finish.
		s.beginCloseLocked(pe.Code, pe.Message, false)
		s.finishAfterCloseWrittenLocked(p)
	case s.state == stateClosing:
		// The peer half-closed after its CLOSE (core §9.1). Our CLOSE (the
		// reply, or our own) may still be queued: let it reach the wire.
		s.finishAfterCloseWrittenLocked(p)
	case s.lostLocked(p, err):
		// Detached: an extension keeps the session for a new base path.
	default:
		reason := "peer closed the transport without a CLOSE"
		if !errors.Is(err, io.EOF) {
			reason = err.Error()
		}
		s.finishLocked(CloseError{Code: CodeGoingAway, Reason: reason, Remote: true, Lost: true})
	}
}

// finishAfterCloseWrittenLocked waits (unlocked, bounded by CloseTimeout)
// for the queued CLOSE to be written, then finishes the session.
func (s *Session) finishAfterCloseWrittenLocked(p *basePath) {
	written := p.closeWritten
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
	if s.resume != nil {
		if handled, err := s.resume.receivedLocked(f); handled || err != nil {
			return err
		}
	}
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
			if handler != nil {
				s.discardHandlerLocked(s.newChannel(r, false, chanOpen), handler)
			}
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

// discardHandlerLocked runs an accepted channel's handler on a channel that
// is already closed, because the connection ended before the channel could
// open. The handler's normal cleanup then releases whatever the Acceptor
// opened for it (a dialed connection, a started process).
func (s *Session) discardHandlerLocked(c *Channel, h Handler) {
	if c.closeInfo == nil {
		c.closeInfo = &CloseError{Code: CodeGoingAway, Reason: "connection closed before the channel opened"}
	}
	s.finishChannelLocked(c)
	go h(c)
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
			if s.resume != nil {
				// Held frames bypassed the count on arrival (session-resume
				// §4): count them once, now that the properties are known.
				s.resume.countHeldLocked(c)
			}
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
	}
	// Both CLOSE_CHANNELs are now sent (ours at least queued, in order) and
	// received, so the close is complete (core §5.4). Don't wait for our
	// frame's write callback: the peer may reuse the ID at once, and its
	// OPEN_CHANNEL can arrive before that callback runs. Anything we later
	// send on a reused ID is queued behind our CLOSE_CHANNEL.
	s.finishChannelLocked(c)
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
	// The close completes when the peer's CLOSE_CHANNEL arrives
	// (onCloseChannelLocked), not when ours is written.
	it, err := s.enqueueLocked(controlFrame(TypeCloseChannel, m), nil, false)
	if err != nil {
		s.finishChannelLocked(c)
		return
	}
	it.closeOf = c
}

func (s *Session) finishChannelLocked(c *Channel) {
	if c.state == chanClosed {
		return
	}
	s.windowTotal -= c.recvWindow
	wasOpening := c.state == chanOpening
	c.state = chanClosed
	// The peer's CLOSE_CHANNEL (or the end of the channel some other way)
	// means retained frames will never be needed (session-resume §5).
	c.retained = nil
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
	if s.path == nil {
		// Detached: there is no transport to send CLOSE on, and closing ends
		// the session for good.
		s.finishLocked(s.closeInfo)
		return
	}
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
	s.urgent, s.urgentBytes = nil, 0
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
	if s.resume != nil {
		s.resume.endedLocked()
	}
	close(s.done)
	s.signalWriter()
	if p := s.path; p != nil {
		go p.conn.Close()
	}
}

// ─── Outbound scheduler ─────────────────────────────────────────────────────

// enqueueLocked queues a frame. Control and probe frames (dataChannel false)
// go before data, which is sent round-robin across channels.
func (s *Session) enqueueLocked(f Frame, after func(error), dataChannel bool) (*outItem, error) {
	if s.state != stateOpen {
		return nil, ErrClosed
	}
	it := &outItem{f: f, after: after, data: dataChannel}
	if dataChannel {
		it.ch = s.chans[f.Channel]
	}
	if dataChannel {
		it.done = make(chan error, 1)
		q := s.data[f.Channel]
		if len(q) == 0 {
			s.rr = append(s.rr, f.Channel)
		}
		s.data[f.Channel] = append(q, it)
	} else {
		it.cost = int64(HeaderSize + len(f.Payload))
		if limit := s.opts.MaxControlQueue; limit > 0 && s.urgentBytes+it.cost > limit {
			// The peer keeps sending frames that need a reply but doesn't read
			// the replies. Close rather than queue without bound. (Pausing
			// reads instead could deadlock two such peers.)
			s.beginCloseLocked(CodeRateLimited, fmt.Sprintf("peer is not reading: over %d bytes of control frames queued", limit), false)
			return nil, ErrClosed
		}
		s.urgentBytes += it.cost
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

// signalWriter wakes the attached path's writer. Call it with s.mu held.
func (s *Session) signalWriter() {
	if s.path == nil {
		return
	}
	select {
	case s.path.wake <- struct{}{}:
	default:
	}
}

func (s *Session) nextLocked() *outItem {
	if len(s.urgent) > 0 {
		it := s.urgent[0]
		s.urgent = s.urgent[1:]
		s.urgentBytes -= it.cost
		return it
	}
	// session-resume/1: a RECEIPT covering every received frame goes before
	// any WINDOW_UPDATE that returns credit for them (receipt before credit,
	// §5), and promptly otherwise.
	if r := s.resume; r != nil && r.dirty && s.state == stateOpen {
		if f := r.receiptLocked(false); f != nil {
			return &outItem{f: *f}
		}
	}
	if s.windowDirty && s.state == stateOpen {
		s.windowDirty = false
		var entries []windowEntry
		for id, c := range s.chans {
			if c.unreturned > 0 && c.state == chanOpen && !c.sentClose {
				inc := c.unreturned + s.growWindowLocked(c, c.unreturned)
				entries = append(entries, windowEntry{id, uint32(inc)})
				c.recvCredit += inc
				c.unreturned = 0
			}
		}
		if len(entries) > 0 {
			return &outItem{f: windowUpdateFrame(entries)}
		}
	}
	for n := len(s.rr); n > 0; n-- {
		id := s.rr[0]
		s.rr = s.rr[1:]
		if s.resume != nil && !s.resume.canSendLocked(id) {
			// At its unacknowledged-frame limit: wait for a RECEIPT (§5).
			s.rr = append(s.rr, id)
			continue
		}
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
	return nil
}

// writeLoop writes the frames of base path p until p is detached or the
// session ends.
func (s *Session) writeLoop(p *basePath) {
	defer func() {
		s.mu.Lock()
		close(p.closeWritten)
		s.mu.Unlock()
	}()
	for {
		var it *outItem
		retained := false
		s.mu.Lock()
		for it == nil {
			if s.path != p {
				s.mu.Unlock()
				return
			}
			if len(p.preamble) > 0 {
				it = &outItem{f: p.preamble[0]}
				p.preamble = p.preamble[1:]
				// Already counted and kept by the extension that attached p.
				retained = true
				break
			}
			if it = s.nextLocked(); it != nil {
				if s.hooks != nil {
					retained = s.hooks.writing(it)
				}
				break
			}
			if s.state == stateClosed {
				s.mu.Unlock()
				return
			}
			s.mu.Unlock()
			select {
			case <-p.wake:
			case <-p.stop:
			}
			s.mu.Lock()
		}
		s.mu.Unlock()

		err := p.conn.WriteFrame(it.f)
		if err != nil {
			s.mu.Lock()
			s.lostLocked(p, err)
			if s.path != p && s.state != stateClosed {
				// The path was lost and the session detached (now, or while
				// the write was in progress). The next path sends the frame:
				// from the extension's copy, or from the queue.
				if !retained {
					requeued := s.requeueLocked(it)
					s.mu.Unlock()
					if !requeued {
						it.finish(errChannelClosed(it.f.Channel))
					}
					return
				}
				s.mu.Unlock()
				it.finish(nil)
				return
			}
			info := s.closeInfo
			if s.state == stateOpen {
				info = CloseError{Code: CodeGoingAway, Reason: "transport write failed: " + err.Error(), Lost: true}
			}
			s.mu.Unlock()
			it.finish(err)
			s.mu.Lock()
			s.finishLocked(info)
			s.mu.Unlock()
			return
		}
		it.finish(nil)
		if it.isClose {
			_ = p.conn.CloseWrite()
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
			if p.lost {
				return 0, errPathLost
			}
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
		case s.state != stateOpen || s.path == nil:
			// Keepalive is suspended while detached (session-resume §6).
		case !s.keepaliveAt.IsZero():
			if last.Before(s.keepaliveAt) && now.Sub(s.keepaliveAt) >= s.pingTimeout {
				// The peer is gone; close the transport without waiting (core
				// §8.2). An extension that keeps the session detaches it
				// instead, without CLOSE 4008 (session-resume §6).
				if !s.lostLocked(s.path, errKeepaliveTimeout) {
					s.finishLocked(CloseError{Code: CodeKeepaliveTimeout, Reason: "peer stopped responding"})
				}
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

// growWindowLocked is receive-window auto-tuning. It is called as returned
// bytes of credit go out in a WINDOW_UPDATE, and returns extra credit to
// grant on top. A sender held back only by the window delivers one window
// per round trip; if a whole window was used in under two round trips, the
// window doubles, within MaxChannelWindow and the ConnectionWindow budget.
func (s *Session) growWindowLocked(c *Channel, returned int64) int64 {
	if s.opts.MaxChannelWindow <= c.recvWindow || !c.props.Reliable {
		return 0
	}
	// A measurement starts when a window's worth of credit is granted (channel
	// open, then each time a full window has been used), so the time includes
	// receiving the bytes, not only returning them.
	now := time.Now()
	c.tuneBytes += returned
	if c.tuneBytes < c.recvWindow {
		return 0
	}
	elapsed := now.Sub(c.tuneStart)
	c.tuneStart, c.tuneBytes = now, 0
	// The larger of SRTT and the latest sample: smoothing alone would take
	// several samples to notice that the path got slower.
	if s.stats.SRTT == 0 {
		s.probeRTTLocked()
		return 0
	}
	rtt := max(s.stats.SRTT, s.stats.RTT)
	if elapsed >= 2*rtt {
		// Not window-limited, or the RTT sample is stale (the path got slower).
		// Refresh it, at most once a second, so growth can resume.
		if now.Sub(s.rttProbedAt) > time.Second {
			s.probeRTTLocked()
		}
		return 0
	}
	extra := min(c.recvWindow, s.opts.MaxChannelWindow-c.recvWindow, s.opts.ConnectionWindow-s.windowTotal)
	if extra <= 0 {
		return 0
	}
	c.recvWindow += extra
	s.windowTotal += extra
	return extra
}

// probeRTTLocked measures the round-trip time once, for auto-tuning, when no
// probe has completed yet (keepalive only pings an idle connection).
func (s *Session) probeRTTLocked() {
	if s.rttProbing || s.state != stateOpen {
		return
	}
	s.rttProbing, s.rttProbedAt = true, time.Now()
	go func() {
		ctx, cancel := context.WithTimeout(s.ctx, s.pingTimeoutOrDefault())
		defer cancel()
		_, _ = s.ping(ctx, nil)
		s.mu.Lock()
		s.rttProbing = false
		s.mu.Unlock()
	}()
}

func (s *Session) pingTimeoutOrDefault() time.Duration {
	if s.pingTimeout > 0 {
		return s.pingTimeout
	}
	return DefaultPingTimeout
}
