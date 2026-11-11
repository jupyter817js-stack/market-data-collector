package clob

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"testing"
	"time"
)

const (
	testL2Key        = "k_test_abcdef"
	testL2Secret64   = "c2VjcmV0LXNhdWNlLWZvci10ZXN0aW5n" // base64 of "secret-sauce-for-testing" // gitleaks:allow -- deterministic test fixture, not a credential
	testL2Passphrase = "p_test_xyz"
)

func TestL2HeadersGoldenVector(t *testing.T) {
	creds := L2Credentials{
		APIKey:        testL2Key,
		APISecret:     testL2Secret64,
		APIPassphrase: testL2Passphrase,
	}
	ts := time.Unix(1_700_000_000, 0).UTC()
	method := "POST"
	path := "/order"
	body := `{"orderID":"abc"}`

	key, sign, tsStr, pass, err := L2Headers(creds, ts, method, path, body)
	if err != nil {
		t.Fatalf("L2Headers: %v", err)
	}
	if key != testL2Key {
		t.Fatalf("key=%q want %q", key, testL2Key)
	}
	if pass != testL2Passphrase {
		t.Fatalf("passphrase=%q want %q", pass, testL2Passphrase)
	}
	if tsStr != "1700000000" {
		t.Fatalf("ts=%q want 1700000000", tsStr)
	}
	// Recompute the expected sign independently using V2 URL-safe base64.
	secretBytes, _ := base64.URLEncoding.DecodeString(testL2Secret64)
	mac := hmac.New(sha256.New, secretBytes)
	mac.Write([]byte(tsStr + method + path + body))
	wantSign := base64.URLEncoding.EncodeToString(mac.Sum(nil))
	if sign != wantSign {
		t.Fatalf("sign drift (URL-safe):\ngot:  %s\nwant: %s", sign, wantSign)
	}
}

func TestL2HeadersRejectsMissingCreds(t *testing.T) {
	cases := []L2Credentials{
		{},
		{APIKey: "k"},                            // no secret
		{APISecret: testL2Secret64},              // no key
		{APIKey: " ", APISecret: testL2Secret64}, // whitespace key
		{APIKey: testL2Key, APISecret: "not-base64!!!#"}, // bad base64
	}
	for i, c := range cases {
		if _, _, _, _, err := L2Headers(c, time.Now(), "GET", "/x", ""); err == nil {
			t.Fatalf("case %d: expected error for creds %+v", i, c)
		}
	}
}

func TestL2HeadersRejectsBadMethodOrPath(t *testing.T) {
	creds := L2Credentials{APIKey: testL2Key, APISecret: testL2Secret64}
	if _, _, _, _, err := L2Headers(creds, time.Now(), "", "/x", ""); err == nil {
		t.Fatalf("expected error on empty method")
	}
	if _, _, _, _, err := L2Headers(creds, time.Now(), "GET", "no-slash", ""); err == nil {
		t.Fatalf("expected error on non-absolute path")
	}
}

func TestL2HeadersMethodIsUppercased(t *testing.T) {
	creds := L2Credentials{APIKey: testL2Key, APISecret: testL2Secret64}
	ts := time.Unix(1_700_000_000, 0).UTC()
	_, signLower, _, _, err := L2Headers(creds, ts, "post", "/order", "{}")
	if err != nil {
		t.Fatalf("lowercase method err: %v", err)
	}
	_, signUpper, _, _, err := L2Headers(creds, ts, "POST", "/order", "{}")
	if err != nil {
		t.Fatalf("uppercase method err: %v", err)
	}
	if signLower != signUpper {
		t.Fatalf("method case sensitivity bug: %s vs %s", signLower, signUpper)
	}
}
