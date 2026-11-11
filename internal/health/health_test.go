package health

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

type fakePinger struct {
	err error
}

func (p fakePinger) Ping(context.Context) error {
	return p.err
}

func TestHealthzReturnsOK(t *testing.T) {
	handler := NewHandler(fakePinger{}, time.Second)
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d want %d", rec.Code, http.StatusOK)
	}
}

func TestReadyzReturnsOKWhenPingSucceeds(t *testing.T) {
	handler := NewHandler(fakePinger{}, time.Second)
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d want %d", rec.Code, http.StatusOK)
	}
}

func TestReadyzReturnsUnavailableWhenPingFails(t *testing.T) {
	handler := NewHandler(fakePinger{err: errors.New("db down")}, time.Second)
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d want %d", rec.Code, http.StatusServiceUnavailable)
	}
}
