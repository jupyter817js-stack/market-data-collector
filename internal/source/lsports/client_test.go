package lsports

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

func TestPostInjectsCredentialsLowerCamel(t *testing.T) {
	var captured map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("expected POST, got %s", r.Method)
		}
		if got := r.Header.Get("Content-Type"); got != "application/json" {
			t.Fatalf("expected Content-Type application/json, got %q", got)
		}
		b, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		if err := json.Unmarshal(b, &captured); err != nil {
			t.Fatalf("unmarshal body: %v: %s", err, b)
		}
		// Live response is PascalCase even though request is lowerCamel.
		_, _ = w.Write([]byte(`{"Header":{"HttpStatusCode":200},"Body":{}}`))
	}))
	defer server.Close()

	c := NewClient(server.URL, Credentials{PackageID: 4044, Username: "u", Password: "p"}, 5*time.Second)
	_, status, err := c.Post(context.Background(), "/Sports/Get", map[string]any{"languageId": 2})
	if err != nil {
		t.Fatalf("Post: %v", err)
	}
	if status != 200 {
		t.Fatalf("expected 200, got %d", status)
	}
	if got, want := int(captured["packageId"].(float64)), 4044; got != want {
		t.Fatalf("packageId: got %d, want %d", got, want)
	}
	if got := captured["userName"]; got != "u" {
		t.Fatalf("userName: got %v, want u", got)
	}
	if got := captured["password"]; got != "p" {
		t.Fatalf("password: got %v, want p", got)
	}
	if got := int(captured["languageId"].(float64)); got != 2 {
		t.Fatalf("languageId: got %d, want 2", got)
	}
	for _, bad := range []string{"PackageId", "UserName", "Password"} {
		if _, ok := captured[bad]; ok {
			t.Fatalf("body unexpectedly contained PascalCase key %q", bad)
		}
	}
}

func TestPostRedactsCredentialsInError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":"bad password sekret-pw"}`))
	}))
	defer server.Close()

	c := NewClient(server.URL, Credentials{PackageID: 4044, Username: "alice", Password: "sekret-pw"}, 5*time.Second)
	_, status, err := c.Post(context.Background(), "/Sports/Get", nil)
	if status != http.StatusUnauthorized {
		t.Fatalf("expected 401 status, got %d", status)
	}
	if err == nil {
		t.Fatalf("expected error on 401")
	}
	if strings.Contains(err.Error(), "sekret-pw") {
		t.Fatalf("error leaks password: %q", err)
	}
}

// TestGetSportsParsesEnvelope uses the actual PascalCase wire format observed
// from the live server on 2026-05-27: Header/Body/Sports/Id/Name.
func TestGetSportsParsesEnvelope(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/Sports/Get" {
			t.Fatalf("expected /Sports/Get, got %s", r.URL.Path)
		}
		_, _ = w.Write([]byte(`{
			"Header":{"HttpStatusCode":200,"Errors":null},
			"Body":{"Sports":[
				{"Id":6046,"Name":"Football"},
				{"Id":54094,"Name":"Tennis"},
				{"Id":154914,"Name":"Baseball"}
			]}
		}`))
	}))
	defer server.Close()

	c := NewClient(server.URL, Credentials{PackageID: 4044, Username: "u", Password: "p"}, 5*time.Second)
	sports, _, err := c.GetSports(context.Background())
	if err != nil {
		t.Fatalf("GetSports: %v", err)
	}
	if len(sports) != 3 {
		t.Fatalf("expected 3 sports, got %d", len(sports))
	}
	if sports[1].ID != 54094 || sports[1].Name != "Tennis" {
		t.Fatalf("unexpected second sport: %+v", sports[1])
	}
}
