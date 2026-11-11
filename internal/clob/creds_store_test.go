package clob

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSaveAndLoadCreds(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sub", "clob-creds.json")
	in := L2CredsFile{
		Address:    "0x7E5F4552091A69125d5DfCb7b8C2659029395Bdf",
		IssuedAt:   time.Now().UTC().Truncate(time.Second),
		BaseURL:    "https://clob.polymarket.com",
		APIKey:     "k-roundtrip",
		APISecret:  "c2VjcmV0LXJvdW5kdHJpcA==", // gitleaks:allow -- deterministic test fixture, not a credential
		Passphrase: "p-roundtrip",
	}
	if err := SaveCreds(path, in); err != nil {
		t.Fatalf("SaveCreds: %v", err)
	}
	st, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if !isWindows() && st.Mode().Perm() != 0o600 {
		t.Fatalf("expected mode 600, got %o", st.Mode().Perm())
	}
	out, err := LoadCreds(path)
	if err != nil {
		t.Fatalf("LoadCreds: %v", err)
	}
	if out.APIKey != in.APIKey || out.APISecret != in.APISecret || out.Passphrase != in.Passphrase {
		t.Fatalf("roundtrip mismatch: got %+v want %+v", out, in)
	}
	if out.Address != in.Address {
		t.Fatalf("address roundtrip mismatch: %s vs %s", out.Address, in.Address)
	}
	// And it should round-trip into L2Credentials cleanly.
	asRuntime := out.As()
	if asRuntime.APIKey != in.APIKey {
		t.Fatalf("As() drops APIKey")
	}
}

func TestSaveCredsRefusesEmpty(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x.json")
	cases := []L2CredsFile{
		{},
		{APIKey: "k"},
		{APISecret: "s"},
		{APIKey: " ", APISecret: "s"},
	}
	for i, c := range cases {
		if err := SaveCreds(path, c); err == nil {
			t.Fatalf("case %d: expected error for %+v", i, c)
		}
	}
}

func TestLoadCredsRejectsEmptyOnDisk(t *testing.T) {
	path := filepath.Join(t.TempDir(), "empty.json")
	if err := os.WriteFile(path, []byte(`{"api_key":"","api_secret":""}`), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	_, err := LoadCreds(path)
	if err == nil || !strings.Contains(err.Error(), "empty api_key") {
		t.Fatalf("expected empty-creds error, got %v", err)
	}
}

func TestLoadCredsRejectsBadJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.json")
	if err := os.WriteFile(path, []byte("not-json"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	_, err := LoadCreds(path)
	if err == nil || !strings.Contains(err.Error(), "decode") {
		t.Fatalf("expected decode error, got %v", err)
	}
}

func TestSaveCredsAtomicAcrossDirs(t *testing.T) {
	// Two writes to the same path don't leave .tmp clutter.
	path := filepath.Join(t.TempDir(), "creds.json")
	in1 := L2CredsFile{APIKey: "k1", APISecret: "s1", Address: "0x01"}
	in2 := L2CredsFile{APIKey: "k2", APISecret: "s2", Address: "0x02"}
	if err := SaveCreds(path, in1); err != nil {
		t.Fatalf("SaveCreds 1: %v", err)
	}
	if err := SaveCreds(path, in2); err != nil {
		t.Fatalf("SaveCreds 2: %v", err)
	}
	entries, _ := os.ReadDir(filepath.Dir(path))
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".clob-creds-") {
			t.Fatalf("leftover tmp file %s after two SaveCreds calls", e.Name())
		}
	}
	out, err := LoadCreds(path)
	if err != nil {
		t.Fatalf("LoadCreds: %v", err)
	}
	if out.APIKey != "k2" {
		t.Fatalf("second write didn't win: %s", out.APIKey)
	}
}
