package betfair

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
)

var errFakeLiveDisconnect = fmt.Errorf("fake betfair live disconnect")

type fakeLiveTransport struct {
	mu          sync.Mutex
	conns       []*fakeLiveConn
	connectErrs []error
	connects    int
	onConnect   func(int)
}

func (t *fakeLiveTransport) Connect(ctx context.Context) (LiveConn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.connects++
	if t.onConnect != nil {
		t.onConnect(t.connects)
	}
	if len(t.connectErrs) > 0 {
		err := t.connectErrs[0]
		t.connectErrs = t.connectErrs[1:]
		if err != nil {
			return nil, err
		}
	}
	if len(t.conns) == 0 {
		return nil, io.ErrUnexpectedEOF
	}
	conn := t.conns[0]
	t.conns = t.conns[1:]
	return conn, nil
}

func (t *fakeLiveTransport) connectCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.connects
}

type fakeLiveConn struct {
	reader io.Reader
	writes [][]byte
	closed bool
}

func newFakeLiveConn(messages ...string) *fakeLiveConn {
	var stream bytes.Buffer
	for _, message := range messages {
		stream.WriteString(message)
		stream.WriteString("\r\n")
	}
	return &fakeLiveConn{reader: bytes.NewReader(stream.Bytes())}
}

func newFakeDisconnectLiveConn(messages ...string) *fakeLiveConn {
	var stream bytes.Buffer
	for _, message := range messages {
		stream.WriteString(message)
		stream.WriteString("\r\n")
	}
	return &fakeLiveConn{reader: &errAfterReader{
		reader: bytes.NewReader(stream.Bytes()),
		err:    errFakeLiveDisconnect,
	}}
}

func (c *fakeLiveConn) Read(p []byte) (int, error) {
	return c.reader.Read(p)
}

func (c *fakeLiveConn) Write(ctx context.Context, payload []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.writes = append(c.writes, append([]byte(nil), payload...))
	return nil
}

func (c *fakeLiveConn) Close() error {
	c.closed = true
	return nil
}

type errAfterReader struct {
	reader *bytes.Reader
	err    error
}

func (r *errAfterReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if n > 0 {
		return n, nil
	}
	if err == io.EOF {
		return 0, r.err
	}
	return n, err
}

// closeToEOFLiveConn replays a fixed sequence of CRLF-delimited messages and
// then, once those messages are drained, blocks any further Read call until
// Close() is invoked, at which point Read returns io.EOF. The blocked channel
// fires when the connector has exhausted the pre-buffered messages and is
// blocked on the next read — the precise state the production race exercises
// (a stream-loop read that gets unblocked by liveCloseOnContext returning
// io.EOF). Using the blocked-on-read state as the synchronization point keeps
// the test deterministic regardless of how aggressively bufio.Reader prefetches
// from the underlying conn.
type closeToEOFLiveConn struct {
	mu        sync.Mutex
	pending   []byte
	blocked   chan struct{}
	closed    chan struct{}
	closeOnce sync.Once
	writes    [][]byte
}

func newCloseToEOFLiveConn(messages []string, blocked chan struct{}) *closeToEOFLiveConn {
	var buf bytes.Buffer
	for _, message := range messages {
		buf.WriteString(message)
		buf.WriteString("\r\n")
	}
	return &closeToEOFLiveConn{
		pending: buf.Bytes(),
		blocked: blocked,
		closed:  make(chan struct{}),
	}
}

func (c *closeToEOFLiveConn) Read(p []byte) (int, error) {
	c.mu.Lock()
	if len(c.pending) > 0 {
		n := copy(p, c.pending)
		c.pending = c.pending[n:]
		c.mu.Unlock()
		return n, nil
	}
	c.mu.Unlock()
	if c.blocked != nil {
		select {
		case <-c.blocked:
		default:
			close(c.blocked)
		}
	}
	<-c.closed
	return 0, io.EOF
}

func (c *closeToEOFLiveConn) Write(ctx context.Context, payload []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	c.writes = append(c.writes, append([]byte(nil), payload...))
	c.mu.Unlock()
	return nil
}

func (c *closeToEOFLiveConn) Close() error {
	c.closeOnce.Do(func() { close(c.closed) })
	return nil
}

// singleLiveTransport hands out a single LiveConn once and then errors. It is
// used to wire a closeToEOFLiveConn into the connector without the lifecycle
// expectations of fakeLiveTransport.
type singleLiveTransport struct {
	mu       sync.Mutex
	conn     LiveConn
	consumed bool
}

func (t *singleLiveTransport) Connect(ctx context.Context) (LiveConn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.consumed || t.conn == nil {
		return nil, io.ErrUnexpectedEOF
	}
	t.consumed = true
	return t.conn, nil
}
