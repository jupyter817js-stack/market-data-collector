package betfair

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"strings"
	"time"
)

type TLSLiveTransportOptions struct {
	Address      string
	ServerName   string
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	TLSConfig    *tls.Config
	DialContext  func(ctx context.Context, network, address string) (net.Conn, error)
}

type TLSLiveTransport struct {
	options TLSLiveTransportOptions
}

func NewTLSLiveTransport(options TLSLiveTransportOptions) *TLSLiveTransport {
	return &TLSLiveTransport{options: options}
}

func (t *TLSLiveTransport) Connect(ctx context.Context) (LiveConn, error) {
	if t == nil {
		return nil, fmt.Errorf("betfair live TLS transport: transport is nil")
	}
	address := strings.TrimSpace(t.options.Address)
	if address == "" {
		return nil, fmt.Errorf("betfair live TLS transport: address is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	dialCtx := ctx
	cancel := func() {}
	if t.options.DialTimeout > 0 {
		dialCtx, cancel = context.WithTimeout(ctx, t.options.DialTimeout)
	}
	defer cancel()

	rawConn, err := t.dial(dialCtx, address)
	if err != nil {
		return nil, fmt.Errorf("connect betfair live TLS transport to %s: %w", address, err)
	}

	tlsConn := tls.Client(rawConn, t.tlsConfig(address))
	if err := tlsConn.HandshakeContext(dialCtx); err != nil {
		_ = rawConn.Close()
		return nil, fmt.Errorf("handshake betfair live TLS transport to %s: %w", address, err)
	}

	return &TLSLiveConn{
		conn:         tlsConn,
		readTimeout:  t.options.ReadTimeout,
		writeTimeout: t.options.WriteTimeout,
	}, nil
}

func (t *TLSLiveTransport) dial(ctx context.Context, address string) (net.Conn, error) {
	if t.options.DialContext != nil {
		return t.options.DialContext(ctx, "tcp", address)
	}
	dialer := &net.Dialer{}
	if t.options.DialTimeout > 0 {
		dialer.Timeout = t.options.DialTimeout
	}
	return dialer.DialContext(ctx, "tcp", address)
}

func (t *TLSLiveTransport) tlsConfig(address string) *tls.Config {
	var config *tls.Config
	if t.options.TLSConfig != nil {
		config = t.options.TLSConfig.Clone()
	} else {
		config = &tls.Config{}
	}
	if config.MinVersion == 0 {
		config.MinVersion = tls.VersionTLS12
	}

	serverName := strings.TrimSpace(t.options.ServerName)
	if serverName == "" && strings.TrimSpace(config.ServerName) == "" {
		serverName = hostFromAddress(address)
	}
	if serverName != "" {
		config.ServerName = serverName
	}
	return config
}

func hostFromAddress(address string) string {
	host, _, err := net.SplitHostPort(address)
	if err == nil {
		return host
	}
	return address
}

type TLSLiveConn struct {
	conn         net.Conn
	readTimeout  time.Duration
	writeTimeout time.Duration
}

func (c *TLSLiveConn) Read(p []byte) (int, error) {
	if c == nil || c.conn == nil {
		return 0, io.ErrClosedPipe
	}
	if c.readTimeout > 0 {
		if err := c.conn.SetReadDeadline(time.Now().Add(c.readTimeout)); err != nil {
			return 0, err
		}
		defer c.conn.SetReadDeadline(time.Time{})
	}
	return c.conn.Read(p)
}

// Write sends the payload exactly as provided. Betfair live message builders in
// this package already return CRLF-delimited payloads, so the transport avoids
// mutating bytes and accidentally double-delimiting stream messages.
func (c *TLSLiveConn) Write(ctx context.Context, payload []byte) error {
	if c == nil || c.conn == nil {
		return io.ErrClosedPipe
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(payload) == 0 {
		return nil
	}

	deadline, hasDeadline := writeDeadline(ctx, c.writeTimeout)
	if hasDeadline {
		if err := c.conn.SetWriteDeadline(deadline); err != nil {
			return fmt.Errorf("set betfair live TLS write deadline: %w", err)
		}
	}

	stopCancelWatch := startWriteCancelWatch(ctx, c.conn)
	written := 0
	var err error
	for written < len(payload) {
		if err = ctx.Err(); err != nil {
			break
		}
		var n int
		n, err = c.conn.Write(payload[written:])
		if n > 0 {
			written += n
		}
		if err != nil {
			break
		}
		if n == 0 {
			err = io.ErrShortWrite
			break
		}
	}
	if stopCancelWatch != nil {
		stopCancelWatch()
	}
	if hasDeadline {
		_ = c.conn.SetWriteDeadline(time.Time{})
	}

	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return fmt.Errorf("write betfair live TLS payload: %w", err)
	}
	return nil
}

func (c *TLSLiveConn) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

func writeDeadline(ctx context.Context, timeout time.Duration) (time.Time, bool) {
	var deadline time.Time
	hasDeadline := false
	if timeout > 0 {
		deadline = time.Now().Add(timeout)
		hasDeadline = true
	}
	if ctxDeadline, ok := ctx.Deadline(); ok && (!hasDeadline || ctxDeadline.Before(deadline)) {
		deadline = ctxDeadline
		hasDeadline = true
	}
	return deadline, hasDeadline
}

func startWriteCancelWatch(ctx context.Context, conn net.Conn) func() {
	if ctx.Done() == nil {
		return nil
	}
	done := make(chan struct{})
	exited := make(chan struct{})
	go func() {
		defer close(exited)
		select {
		case <-ctx.Done():
			_ = conn.SetWriteDeadline(time.Now())
		case <-done:
		}
	}()
	return func() {
		close(done)
		<-exited
	}
}
