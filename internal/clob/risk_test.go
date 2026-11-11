package clob

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// fakeStore implements ExposureStore for the risk tests.
type fakeStore struct {
	exposureByTokenSide map[string]float64
	totalExposure       float64
	ordersInHour        int
	hasRecent           map[string]bool
	err                 error // injected to verify error propagation
}

func key(token, side string) string { return token + "/" + side }
func keyMatchup(matchupID int64, token, side string) string {
	return fmt.Sprintf("%d/%s/%s", matchupID, token, side)
}
func fmtSize(f float64) string {
	return strings.TrimRight(strings.TrimRight(fmt.Sprintf("%.2f", f), "0"), ".")
}

// We don't want fmt imported in fakeStore globally; redeclare for tests via Sprintf.
// (Below uses a small helper to avoid the issue.)

func (f *fakeStore) ExposureForToken(ctx context.Context, t, s string, w time.Duration) (float64, error) {
	if f.err != nil {
		return 0, f.err
	}
	return f.exposureByTokenSide[key(t, s)], nil
}
func (f *fakeStore) TotalExposure(ctx context.Context, w time.Duration) (float64, error) {
	if f.err != nil {
		return 0, f.err
	}
	return f.totalExposure, nil
}
func (f *fakeStore) CountOrdersInWindow(ctx context.Context, w time.Duration) (int, error) {
	if f.err != nil {
		return 0, f.err
	}
	return f.ordersInHour, nil
}
func (f *fakeStore) HasRecentOrderFor(ctx context.Context, m int64, t, s string, w time.Duration) (bool, error) {
	if f.err != nil {
		return false, f.err
	}
	return f.hasRecent[keyMatchup(m, t, s)], nil
}

func defaultRiskConfig() RiskConfig {
	return RiskConfig{
		MaxPositionUSDC:      50,
		MaxTotalExposureUSDC: 200,
		MaxOrdersPerHour:     20,
		CooldownPerMatchup:   2 * time.Minute,
		ExposureWindow:       24 * time.Hour,
		MinSizeUSDC:          1,
		MaxSizeUSDCPerOrder:  100,
	}
}

func TestEvaluateAllowsCleanOrder(t *testing.T) {
	cfg := defaultRiskConfig()
	store := &fakeStore{}
	cand := OrderCandidate{MatchupID: 1, TokenID: "tk", Side: "BUY", SizeUSDC: 20}
	d, err := cfg.Evaluate(context.Background(), store, cand)
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	if !d.Allowed {
		t.Fatalf("expected allowed; got %+v", d)
	}
}

func TestKillSwitch(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "kill")
	if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	cfg := defaultRiskConfig()
	cfg.KillSwitchPath = path
	d, err := cfg.Evaluate(context.Background(), &fakeStore{}, OrderCandidate{TokenID: "tk", Side: "BUY", SizeUSDC: 20})
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	if d.Allowed || !d.NeverPropose || !strings.Contains(d.Reason, "kill switch") {
		t.Fatalf("expected kill switch refuse; got %+v", d)
	}
}

func TestSizeBounds(t *testing.T) {
	cfg := defaultRiskConfig()
	cfg.MinSizeUSDC = 5
	cfg.MaxSizeUSDCPerOrder = 50
	store := &fakeStore{}
	cases := []struct {
		name   string
		size   float64
		reason string
	}{
		{"tooSmall", 1, "MinSizeUSDC"},
		{"tooBig", 75, "MaxSizeUSDCPerOrder"},
		{"zero", 0, "<= 0"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d, _ := cfg.Evaluate(context.Background(), store, OrderCandidate{TokenID: "tk", Side: "BUY", SizeUSDC: tc.size})
			if d.Allowed || !strings.Contains(d.Reason, tc.reason) {
				t.Fatalf("got %+v want refuse with %q", d, tc.reason)
			}
		})
	}
}

func TestCooldown(t *testing.T) {
	cfg := defaultRiskConfig()
	store := &fakeStore{hasRecent: map[string]bool{keyMatchup(7, "tk", "BUY"): true}}
	d, _ := cfg.Evaluate(context.Background(), store, OrderCandidate{MatchupID: 7, TokenID: "tk", Side: "BUY", SizeUSDC: 20})
	if d.Allowed || !strings.Contains(d.Reason, "cooldown") {
		t.Fatalf("expected cooldown refuse; got %+v", d)
	}
	// Different matchup: should pass.
	d2, _ := cfg.Evaluate(context.Background(), store, OrderCandidate{MatchupID: 99, TokenID: "tk", Side: "BUY", SizeUSDC: 20})
	if !d2.Allowed {
		t.Fatalf("expected allowed for different matchup; got %+v", d2)
	}
}

func TestPerTokenCap(t *testing.T) {
	cfg := defaultRiskConfig()
	cfg.MaxPositionUSDC = 30
	store := &fakeStore{exposureByTokenSide: map[string]float64{key("tk", "BUY"): 20}}
	d, _ := cfg.Evaluate(context.Background(), store, OrderCandidate{TokenID: "tk", Side: "BUY", SizeUSDC: 20})
	if d.Allowed || !strings.Contains(d.Reason, "per-token cap") {
		t.Fatalf("expected per-token cap refuse; got %+v", d)
	}
	// Smaller size that fits exactly: should pass.
	d2, _ := cfg.Evaluate(context.Background(), store, OrderCandidate{TokenID: "tk", Side: "BUY", SizeUSDC: 10})
	if !d2.Allowed {
		t.Fatalf("expected allowed for fit-exactly size; got %+v", d2)
	}
}

func TestTotalExposureCap(t *testing.T) {
	cfg := defaultRiskConfig()
	cfg.MaxTotalExposureUSDC = 100
	store := &fakeStore{totalExposure: 95}
	d, _ := cfg.Evaluate(context.Background(), store, OrderCandidate{TokenID: "tk", Side: "BUY", SizeUSDC: 20})
	if d.Allowed || !strings.Contains(d.Reason, "total exposure") {
		t.Fatalf("expected total-exposure refuse; got %+v", d)
	}
}

func TestRateCap(t *testing.T) {
	cfg := defaultRiskConfig()
	cfg.MaxOrdersPerHour = 5
	store := &fakeStore{ordersInHour: 5}
	d, _ := cfg.Evaluate(context.Background(), store, OrderCandidate{TokenID: "tk", Side: "BUY", SizeUSDC: 20})
	if d.Allowed || !strings.Contains(d.Reason, "rate cap") {
		t.Fatalf("expected rate cap refuse; got %+v", d)
	}
}

func TestStoreErrorPropagates(t *testing.T) {
	cfg := defaultRiskConfig()
	store := &fakeStore{err: errors.New("db down")}
	_, err := cfg.Evaluate(context.Background(), store, OrderCandidate{MatchupID: 1, TokenID: "tk", Side: "BUY", SizeUSDC: 20})
	if err == nil {
		t.Fatalf("expected store error to surface")
	}
}

func TestValidateProducesWarnings(t *testing.T) {
	zero := RiskConfig{}
	warns := zero.Validate()
	for _, must := range []string{"MaxPositionUSDC", "MaxTotalExposureUSDC", "MaxOrdersPerHour", "CooldownPerMatchup", "KillSwitchPath", "MaxSizeUSDCPerOrder"} {
		found := false
		for _, w := range warns {
			if strings.Contains(w, must) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("expected zero-config warning to mention %s; warns=%v", must, warns)
		}
	}
}
