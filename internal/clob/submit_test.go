package clob

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/your-org/pol-odds-collector/internal/executor"
)

func sampleSignedOrder(t *testing.T) SignedOrder {
	t.Helper()
	so, err := Sign(sampleOrderSpec(), DefaultDomain(), testPrivKey)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	return so
}

func TestBuildOrderBodyShape(t *testing.T) {
	signed := sampleSignedOrder(t)
	env, err := buildOrderBody(signed, "k-test", executor.OrderTypeFAK)
	if err != nil {
		t.Fatalf("buildOrderBody: %v", err)
	}
	if env.OrderType != "FAK" {
		t.Fatalf("orderType=%q want FAK", env.OrderType)
	}
	if env.Owner != "k-test" {
		t.Fatalf("owner=%q want k-test", env.Owner)
	}
	if env.Order.Side != "BUY" { // V2 wire: string side
		t.Fatalf("side=%q want BUY", env.Order.Side)
	}
	if env.Order.Salt != 99999 {
		t.Fatalf("salt=%d want 99999", env.Order.Salt)
	}
	if env.Order.MakerAmount != "20000000" || env.Order.TakerAmount != "40000000" {
		t.Fatalf("amounts wrong: maker=%s taker=%s", env.Order.MakerAmount, env.Order.TakerAmount)
	}
	if env.Order.Timestamp == "" {
		t.Fatalf("timestamp empty (v2 requires ms)")
	}
	if env.Order.Metadata != "0x"+strings.Repeat("0", 64) {
		t.Fatalf("metadata default zero32 expected, got %q", env.Order.Metadata)
	}
	if env.Order.Taker != "" {
		t.Fatalf("taker should be empty (omitempty), got %q", env.Order.Taker)
	}
	if env.Order.Signature != signed.Signature {
		t.Fatalf("signature drop")
	}
}

func TestBuildOrderBodyRejectsBadInputs(t *testing.T) {
	signed := sampleSignedOrder(t)
	if _, err := buildOrderBody(signed, "", executor.OrderTypeFAK); err == nil {
		t.Fatalf("expected error on empty owner")
	}
	signed.Signature = ""
	if _, err := buildOrderBody(signed, "k", executor.OrderTypeFAK); err == nil {
		t.Fatalf("expected error on empty signature")
	}
	signed = sampleSignedOrder(t)
	signed.Spec.Salt = nil
	if _, err := buildOrderBody(signed, "k", executor.OrderTypeFAK); err == nil {
		t.Fatalf("expected error on nil salt")
	}
	signed = sampleSignedOrder(t)
	signed.Spec.Side = "WAT"
	if _, err := buildOrderBody(signed, "k", executor.OrderTypeFAK); err == nil {
		t.Fatalf("expected error on unknown side")
	}
}

func TestSideEncodingSell(t *testing.T) {
	signed := sampleSignedOrder(t)
	signed.Spec.Side = executor.SideSell
	env, err := buildOrderBody(signed, "k", executor.OrderTypeFAK)
	if err != nil {
		t.Fatalf("buildOrderBody: %v", err)
	}
	if env.Order.Side != "SELL" {
		t.Fatalf("SELL side=%q want SELL", env.Order.Side)
	}
}

func TestSubmitOrderSuccessPath(t *testing.T) {
	signed := sampleSignedOrder(t)
	creds := L2Credentials{APIKey: "k-001", APISecret: testL2Secret64, APIPassphrase: "p-001"}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/order" {
			http.Error(w, "wrong path", 404)
			return
		}
		// Every required header present?
		for _, h := range []string{"POLY_ADDRESS", "POLY_API_KEY", "POLY_SIGNATURE", "POLY_TIMESTAMP", "POLY_PASSPHRASE"} {
			if r.Header.Get(h) == "" {
				t.Errorf("missing header %s", h)
			}
		}
		if r.Header.Get("POLY_API_KEY") != creds.APIKey {
			t.Errorf("POLY_API_KEY=%q want %q", r.Header.Get("POLY_API_KEY"), creds.APIKey)
		}
		// Verify HMAC matches what L2Headers would produce (V2 URL-safe base64).
		bodyBytes, _ := io.ReadAll(r.Body)
		secret, _ := base64.URLEncoding.DecodeString(creds.APISecret)
		mac := hmac.New(sha256.New, secret)
		mac.Write([]byte(r.Header.Get("POLY_TIMESTAMP") + "POST/order" + string(bodyBytes)))
		want := base64.URLEncoding.EncodeToString(mac.Sum(nil))
		if r.Header.Get("POLY_SIGNATURE") != want {
			t.Errorf("HMAC mismatch:\ngot:  %s\nwant: %s", r.Header.Get("POLY_SIGNATURE"), want)
		}
		// Body decodes back to orderEnvelope.
		var got orderEnvelope
		if err := json.Unmarshal(bodyBytes, &got); err != nil {
			t.Errorf("bad body json: %v", err)
		}
		if got.Order.Signature != signed.Signature {
			t.Errorf("signature didn't survive marshal: %s vs %s", got.Order.Signature, signed.Signature)
		}
		_ = json.NewEncoder(w).Encode(OrderResponse{
			Success: true, OrderID: "0xabcdef", Status: "matched",
			TransactionsHashes: []string{"0xtx1"},
		})
	}))
	defer srv.Close()

	c := NewClient(srv.URL, 5*time.Second)
	resp, err := c.SubmitOrder(context.Background(), signed, creds, executor.OrderTypeFAK)
	if err != nil {
		t.Fatalf("SubmitOrder: %v", err)
	}
	if !resp.Success || resp.OrderID != "0xabcdef" || resp.Status != "matched" {
		t.Fatalf("unexpected resp: %+v", resp)
	}
	if len(resp.TransactionsHashes) != 1 || resp.TransactionsHashes[0] != "0xtx1" {
		t.Fatalf("tx hashes lost: %v", resp.TransactionsHashes)
	}
}

func TestSubmitOrderSurfacesServerFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(OrderResponse{Success: false, ErrorMsg: "balance too low"})
	}))
	defer srv.Close()

	c := NewClient(srv.URL, 5*time.Second)
	signed := sampleSignedOrder(t)
	creds := L2Credentials{APIKey: "k", APISecret: testL2Secret64, APIPassphrase: "p"}
	resp, err := c.SubmitOrder(context.Background(), signed, creds, executor.OrderTypeFAK)
	if err == nil || !strings.Contains(err.Error(), "balance too low") {
		t.Fatalf("expected server-failure surface, got err=%v resp=%+v", err, resp)
	}
}

func TestSubmitOrderNon2xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"error":"rate limit"}`, http.StatusTooManyRequests)
	}))
	defer srv.Close()

	c := NewClient(srv.URL, 5*time.Second)
	signed := sampleSignedOrder(t)
	creds := L2Credentials{APIKey: "k", APISecret: testL2Secret64, APIPassphrase: "p"}
	_, err := c.SubmitOrder(context.Background(), signed, creds, executor.OrderTypeFAK)
	if err == nil {
		t.Fatalf("expected APIError on 429")
	}
	apiErr, ok := err.(*APIError)
	if !ok || apiErr.Status != http.StatusTooManyRequests {
		t.Fatalf("wrong error: %T %v", err, err)
	}
}

func TestBodyJSONDryRun(t *testing.T) {
	signed := sampleSignedOrder(t)
	b, err := BodyJSON(signed, "k-test", executor.OrderTypeFAK)
	if err != nil {
		t.Fatalf("BodyJSON: %v", err)
	}
	// Indented JSON should be human-readable
	if !strings.Contains(string(b), `"orderType": "FAK"`) {
		t.Fatalf("indented json missing orderType: %s", b)
	}
	var env orderEnvelope
	if err := json.Unmarshal(b, &env); err != nil {
		t.Fatalf("dry-run body must roundtrip: %v", err)
	}
	if env.Owner != "k-test" {
		t.Fatalf("owner roundtrip: %s", env.Owner)
	}
	// V2: salt is JSON NUMBER (no quotes). tokenId/makerAmount/takerAmount
	// stay as strings because they can exceed 2^53.
	if !strings.Contains(string(b), `"salt": 99999`) {
		t.Fatalf("salt should be JSON NUMBER 99999 in v2: %s", b)
	}
	if !strings.Contains(string(b), `"side": "BUY"`) {
		t.Fatalf("side should be string \"BUY\" in v2: %s", b)
	}
	if !strings.Contains(string(b), `"timestamp"`) {
		t.Fatalf("v2 body missing timestamp field: %s", b)
	}
	_ = big.NewInt(0) // keep math/big import alive even if other tests change
}
