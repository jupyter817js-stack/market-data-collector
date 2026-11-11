package lsports

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestSubscribeRequiresPositiveFixtureID(t *testing.T) {
	c := NewClient("https://example.invalid", Credentials{PackageID: 4044, Username: "u", Password: "p"}, 1*time.Second)
	if _, err := c.Subscribe(context.Background(), 0); err == nil {
		t.Fatalf("expected error on fixtureID=0")
	}
	if _, err := c.Subscribe(context.Background(), -1); err == nil {
		t.Fatalf("expected error on fixtureID=-1")
	}
}

func TestSubscribeSendsSingleFixtureArray(t *testing.T) {
	var captured map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/Fixtures/Subscribe" {
			t.Fatalf("expected /Fixtures/Subscribe, got %s", r.URL.Path)
		}
		b, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(b, &captured); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		_, _ = w.Write([]byte(`{"Header":{"HttpStatusCode":200},"Body":{"Succeeded":true}}`))
	}))
	defer server.Close()

	c := NewClient(server.URL, Credentials{PackageID: 4044, Username: "u", Password: "p"}, 5*time.Second)
	if _, err := c.Subscribe(context.Background(), 12345); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	// fixtures must be a single-element array; if the slice were variadic
	// or larger we'd be at risk of accidental bulk-subscribe.
	arr, ok := captured["fixtures"].([]any)
	if !ok {
		t.Fatalf("fixtures field not array: %T", captured["fixtures"])
	}
	if len(arr) != 1 {
		t.Fatalf("expected single-element fixtures array, got len=%d", len(arr))
	}
	if int(arr[0].(float64)) != 12345 {
		t.Fatalf("fixture id: got %v want 12345", arr[0])
	}
}

func TestUnsubscribeSendsSingleFixtureArray(t *testing.T) {
	var captured map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/Fixtures/Unsubscribe" {
			t.Fatalf("expected /Fixtures/Unsubscribe, got %s", r.URL.Path)
		}
		b, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(b, &captured); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		_, _ = w.Write([]byte(`{"Header":{"HttpStatusCode":200},"Body":{"Succeeded":true}}`))
	}))
	defer server.Close()

	c := NewClient(server.URL, Credentials{PackageID: 4044, Username: "u", Password: "p"}, 5*time.Second)
	if _, err := c.Unsubscribe(context.Background(), 99); err != nil {
		t.Fatalf("Unsubscribe: %v", err)
	}
	arr := captured["fixtures"].([]any)
	if len(arr) != 1 || int(arr[0].(float64)) != 99 {
		t.Fatalf("expected [99], got %v", arr)
	}
}

func TestSubscribeSurfacesEnvelopeError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"Header":{"HttpStatusCode":404,"Errors":[{"Message":"fixture not found"}]},"Body":null}`))
	}))
	defer server.Close()

	c := NewClient(server.URL, Credentials{PackageID: 4044, Username: "u", Password: "p"}, 5*time.Second)
	_, err := c.Subscribe(context.Background(), 12345)
	if err == nil {
		t.Fatalf("expected envelope-level error, got nil")
	}
}
