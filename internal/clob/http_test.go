package clob

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestBootstrapL2ParsesResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/auth/api-key" {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			http.Error(w, "wrong path", 404)
			return
		}
		for _, h := range []string{"POLY_ADDRESS", "POLY_SIGNATURE", "POLY_TIMESTAMP", "POLY_NONCE"} {
			if r.Header.Get(h) == "" {
				t.Errorf("missing header %s", h)
			}
		}
		body, _ := io.ReadAll(r.Body)
		if string(body) != "{}" {
			t.Errorf("expected empty body {}; got %q", string(body))
		}
		_ = json.NewEncoder(w).Encode(map[string]string{
			"apiKey":     "k-test-001",
			"secret":     "c2VjcmV0LWZyb20tdGVzdA==",
			"passphrase": "pass-test",
		})
	}))
	defer srv.Close()

	c := NewClient(srv.URL, 5*time.Second)
	req, err := SignAuth(testPrivKey, DefaultAuthDomain())
	if err != nil {
		t.Fatalf("SignAuth: %v", err)
	}
	creds, err := c.BootstrapL2(context.Background(), req)
	if err != nil {
		t.Fatalf("BootstrapL2: %v", err)
	}
	if creds.APIKey != "k-test-001" || creds.APISecret == "" || creds.APIPassphrase != "pass-test" {
		t.Fatalf("unexpected creds %+v", creds)
	}
}

func TestBootstrapL2RejectsNon2xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"error":"signature invalid"}`, http.StatusUnauthorized)
	}))
	defer srv.Close()

	c := NewClient(srv.URL, 5*time.Second)
	req, _ := SignAuth(testPrivKey, DefaultAuthDomain())
	_, err := c.BootstrapL2(context.Background(), req)
	if err == nil {
		t.Fatalf("expected APIError on 401")
	}
	apiErr, ok := err.(*APIError)
	if !ok || apiErr.Status != http.StatusUnauthorized {
		t.Fatalf("wrong error type/status: %T %v", err, err)
	}
	if !strings.Contains(apiErr.Body, "signature invalid") {
		t.Fatalf("body not surfaced: %q", apiErr.Body)
	}
}

func TestBootstrapL2RejectsEmptyCreds(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{"apiKey": "", "secret": ""})
	}))
	defer srv.Close()

	c := NewClient(srv.URL, 5*time.Second)
	req, _ := SignAuth(testPrivKey, DefaultAuthDomain())
	_, err := c.BootstrapL2(context.Background(), req)
	if err == nil || !strings.Contains(err.Error(), "empty credentials") {
		t.Fatalf("expected empty-creds error, got %v", err)
	}
}

func TestBootstrapL2RefusesUnverifiableRequest(t *testing.T) {
	// Tamper the signature so VerifyAuthRoundTrip fails — BootstrapL2 must
	// short-circuit before any HTTP egress.
	c := NewClient("http://127.0.0.1:1", 5*time.Second) // unreachable; we should never call it
	req, _ := SignAuth(testPrivKey, DefaultAuthDomain())
	req.Signature = "0x" + strings.Repeat("00", 65)
	_, err := c.BootstrapL2(context.Background(), req)
	if err == nil || !strings.Contains(err.Error(), "refusing") {
		t.Fatalf("expected refuse-to-send error, got %v", err)
	}
}
