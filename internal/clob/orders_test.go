package clob

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestOrderStatusIsTerminal(t *testing.T) {
	cases := map[OrderStatus]bool{
		OrderStatusFilled:    true,
		OrderStatusUnmatched: true,
		OrderStatusCanceled:  true,
		OrderStatusLive:      false,
		OrderStatusMatched:   false,
		OrderStatusDelayed:   false,
		OrderStatusUnknown:   false,
	}
	for s, want := range cases {
		if got := s.IsTerminal(); got != want {
			t.Errorf("IsTerminal(%q)=%v want %v", s, got, want)
		}
	}
}

func TestGetOrderHappyPath(t *testing.T) {
	creds := L2Credentials{APIKey: "k", APISecret: testL2Secret64, APIPassphrase: "p"}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/data/order/0xabc" {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			http.Error(w, "wrong", 400)
			return
		}
		for _, h := range []string{"POLY_API_KEY", "POLY_SIGNATURE", "POLY_TIMESTAMP", "POLY_PASSPHRASE"} {
			if r.Header.Get(h) == "" {
				t.Errorf("missing header %s", h)
			}
		}
		_ = json.NewEncoder(w).Encode(OrderInfo{
			ID:           "0xabc",
			Status:       OrderStatusFilled,
			SizeMatched:  "20",
			OriginalSize: "20",
			AveragePrice: "0.49",
		})
	}))
	defer srv.Close()

	c := NewClient(srv.URL, 5*time.Second)
	info, err := c.GetOrder(context.Background(), "0xabc", creds)
	if err != nil {
		t.Fatalf("GetOrder: %v", err)
	}
	if info.ID != "0xabc" || info.Status != OrderStatusFilled {
		t.Fatalf("unexpected info: %+v", info)
	}
	if !info.Status.IsTerminal() {
		t.Fatalf("filled must be terminal")
	}
}

func TestGetOrderWrappedShape(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"order": map[string]any{
				"id":     "0xwrap",
				"status": "live",
			},
		})
	}))
	defer srv.Close()

	c := NewClient(srv.URL, 5*time.Second)
	creds := L2Credentials{APIKey: "k", APISecret: testL2Secret64, APIPassphrase: "p"}
	info, err := c.GetOrder(context.Background(), "0xwrap", creds)
	if err != nil {
		t.Fatalf("GetOrder wrapped: %v", err)
	}
	if info.ID != "0xwrap" || info.Status != OrderStatusLive {
		t.Fatalf("wrapped unwrap failed: %+v", info)
	}
}

func TestGetOrder404IsSentinel(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
	}))
	defer srv.Close()

	c := NewClient(srv.URL, 5*time.Second)
	creds := L2Credentials{APIKey: "k", APISecret: testL2Secret64, APIPassphrase: "p"}
	_, err := c.GetOrder(context.Background(), "0xmissing", creds)
	if !errors.Is(err, ErrOrderNotFound) {
		t.Fatalf("expected ErrOrderNotFound, got %v", err)
	}
}

func TestGetOrderEmptyIDRejected(t *testing.T) {
	c := NewClient("http://127.0.0.1:1", 5*time.Second)
	_, err := c.GetOrder(context.Background(), "  ", L2Credentials{APIKey: "k", APISecret: testL2Secret64})
	if err == nil || !strings.Contains(err.Error(), "empty order id") {
		t.Fatalf("expected empty-id error, got %v", err)
	}
}

func TestGetOrderNon2xxNon404Bubbles(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"error":"rate"}`, http.StatusTooManyRequests)
	}))
	defer srv.Close()

	c := NewClient(srv.URL, 5*time.Second)
	creds := L2Credentials{APIKey: "k", APISecret: testL2Secret64, APIPassphrase: "p"}
	_, err := c.GetOrder(context.Background(), "0xrate", creds)
	if err == nil {
		t.Fatalf("expected 429 error")
	}
	apiErr, ok := err.(*APIError)
	if !ok || apiErr.Status != http.StatusTooManyRequests {
		t.Fatalf("wrong wrap: %T %v", err, err)
	}
}
