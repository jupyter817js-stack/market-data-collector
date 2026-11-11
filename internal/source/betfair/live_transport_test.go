package betfair

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"net"
	"strings"
	"testing"
	"time"
)

func TestTLSLiveTransportValidatesMissingAddress(t *testing.T) {
	transport := NewTLSLiveTransport(TLSLiveTransportOptions{})
	_, err := transport.Connect(context.Background())
	if err == nil || !strings.Contains(err.Error(), "address is required") {
		t.Fatalf("missing address error=%v want address validation", err)
	}
}

func TestTLSLiveTransportConnectRespectsContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	transport := NewTLSLiveTransport(TLSLiveTransportOptions{Address: "127.0.0.1:1"})
	_, err := transport.Connect(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled connect error=%v want context.Canceled", err)
	}
}

func TestTLSLiveTransportLocalReadWriteAndParser(t *testing.T) {
	received := make(chan []byte, 1)
	server := newLocalTLSLiveServer(t, func(conn net.Conn) {
		reader := bufio.NewReader(conn)
		line, err := reader.ReadBytes('\n')
		if err != nil {
			t.Errorf("read client line: %v", err)
			return
		}
		received <- append([]byte(nil), line...)
		_, _ = conn.Write([]byte(`{"op":"status","id":7,"statusCode":"SUCCESS"}` + "\r\n"))
	})
	defer server.close()

	transport := NewTLSLiveTransport(TLSLiveTransportOptions{
		Address:      server.address,
		ServerName:   "localhost",
		TLSConfig:    server.clientTLS,
		DialTimeout:  time.Second,
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
	})
	conn, err := transport.Connect(context.Background())
	if err != nil {
		t.Fatalf("connect local TLS transport: %v", err)
	}
	defer conn.Close()

	payload := []byte(`{"op":"heartbeat","id":7}` + "\r\n")
	if err := conn.Write(context.Background(), payload); err != nil {
		t.Fatalf("write TLS payload: %v", err)
	}
	select {
	case got := <-received:
		if !bytes.Equal(got, payload) {
			t.Fatalf("server received payload=%q want exact CRLF-delimited payload=%q", got, payload)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for server payload")
	}

	parser := NewParser(conn, DefaultMaxMessageSize)
	msg, err := parser.Next(context.Background())
	if err != nil {
		t.Fatalf("parse local TLS server message: %v", err)
	}
	if msg.Op != OperationStatus || msg.StatusCode != "SUCCESS" {
		t.Fatalf("parsed message=%+v want SUCCESS status", msg)
	}
}

func TestTLSLiveTransportWriteTimeoutAndSecretSafeError(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	conn := &TLSLiveConn{conn: client, writeTimeout: 20 * time.Millisecond}
	payload := bytes.Repeat([]byte(liveTestAppKey+" "+liveTestSession), 1024)
	err := conn.Write(context.Background(), payload)
	if err == nil {
		t.Fatal("write timeout error=nil want deterministic timeout")
	}
	assertNoLiveSecrets(t, err.Error())
	if !strings.Contains(err.Error(), "write betfair live TLS payload") {
		t.Fatalf("write timeout error=%v want TLS write context", err)
	}
}

func TestTLSLiveTransportWriteRespectsCanceledContext(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	conn := &TLSLiveConn{conn: client, writeTimeout: time.Second}
	err := conn.Write(ctx, []byte(`{"op":"heartbeat"}`+"\r\n"))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled write error=%v want context.Canceled", err)
	}
}

func TestLiveConnectorWithTLSLiveTransportLocalHappyPath(t *testing.T) {
	writes := make(chan []byte, 2)
	server := newLocalTLSLiveServer(t, func(conn net.Conn) {
		reader := bufio.NewReader(conn)
		auth, err := reader.ReadBytes('\n')
		if err != nil {
			t.Errorf("read auth request: %v", err)
			return
		}
		writes <- append([]byte(nil), auth...)
		_, _ = conn.Write([]byte(`{"op":"status","id":1,"statusCode":"SUCCESS","initialClk":"tls-initial"}` + "\r\n"))

		subscription, err := reader.ReadBytes('\n')
		if err != nil {
			t.Errorf("read subscription request: %v", err)
			return
		}
		writes <- append([]byte(nil), subscription...)
		_, _ = conn.Write([]byte(`{"op":"status","id":2,"statusCode":"SUCCESS","clk":"tls-sub"}` + "\r\n"))
		_, _ = conn.Write([]byte(liveMarketImageMessage("1.111", "event-1", "Home FC", 2.44, 55, 2.48, 65) + "\r\n"))
	})
	defer server.close()

	transport := NewTLSLiveTransport(TLSLiveTransportOptions{
		Address:      server.address,
		ServerName:   "localhost",
		TLSConfig:    server.clientTLS,
		DialTimeout:  time.Second,
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
	})
	connector := NewLiveConnector(transport, liveTestOptions())
	result, err := connector.Run(context.Background())
	if err != nil {
		t.Fatalf("run connector through local TLS transport: %v", err)
	}
	if result.State != LiveStateStopped || result.ConnectionAttempts != 1 {
		t.Fatalf("result=%+v want stopped after one local TLS connection", result)
	}
	if result.LatestClock != "tls-sub" || result.InitialClock != "tls-initial" {
		t.Fatalf("clocks=%q/%q want tls-sub/tls-initial", result.LatestClock, result.InitialClock)
	}
	back := requireLiveCandidate(t, result.Candidates, "1.111", 101, ExchangeSideBack)
	lay := requireLiveCandidate(t, result.Candidates, "1.111", 101, ExchangeSideLay)
	if back.PriceDecimal != 2.44 || lay.PriceDecimal != 2.48 {
		t.Fatalf("TLS connector candidates back=%+v lay=%+v", back, lay)
	}

	auth := readTLSLiveTestWrite(t, writes)
	subscription := readTLSLiveTestWrite(t, writes)
	if !bytes.HasSuffix(auth, []byte("\r\n")) || !bytes.HasSuffix(subscription, []byte("\r\n")) {
		t.Fatalf("TLS connector writes must remain CRLF-delimited auth=%q subscription=%q", auth, subscription)
	}
	redactedAuth := string(RedactLivePayload(auth))
	assertNoLiveSecrets(t, redactedAuth)
	if !strings.Contains(redactedAuth, redactedAppKey) || !strings.Contains(redactedAuth, redactedSessionToken) {
		t.Fatalf("redacted auth payload=%s", redactedAuth)
	}
}

func readTLSLiveTestWrite(t *testing.T, writes <-chan []byte) []byte {
	t.Helper()
	select {
	case write := <-writes:
		return write
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for TLS connector write")
		return nil
	}
}

type localTLSLiveServer struct {
	address   string
	clientTLS *tls.Config
	close     func()
}

func newLocalTLSLiveServer(t *testing.T, handler func(net.Conn)) localTLSLiveServer {
	t.Helper()
	certPEM, keyPEM := newLocalTLSLiveCertificate(t)
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatalf("load local TLS certificate: %v", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(certPEM) {
		t.Fatal("append local TLS certificate to pool failed")
	}
	listener, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	})
	if err != nil {
		t.Fatalf("start local TLS listener: %v", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				handler(conn)
			}()
		}
	}()
	return localTLSLiveServer{
		address: listener.Addr().String(),
		clientTLS: &tls.Config{
			RootCAs:    pool,
			ServerName: "localhost",
			MinVersion: tls.VersionTLS12,
		},
		close: func() {
			_ = listener.Close()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("timed out closing local TLS listener")
			}
		},
	}
}

func newLocalTLSLiveCertificate(t *testing.T) ([]byte, []byte) {
	t.Helper()
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate local TLS key: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName: "localhost",
		},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &privateKey.PublicKey, privateKey)
	if err != nil {
		t.Fatalf("create local TLS certificate: %v", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(privateKey)})
	return certPEM, keyPEM
}

var _ LiveTransport = (*TLSLiveTransport)(nil)
var _ LiveConn = (*TLSLiveConn)(nil)
var _ io.Reader = (*TLSLiveConn)(nil)
