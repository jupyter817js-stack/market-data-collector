package clob

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"
)

// RiskConfig caps the always-on submitter. Every field is a hard guard;
// any limit hit refuses the order without partial filling. Zero values
// in numeric fields mean "no limit" (so a fresh-config operator gets
// "the gate is wide open" warnings, not "everything blocked" silence —
// see Validate).
type RiskConfig struct {
	// MaxPositionUSDC caps per-(token, side) live exposure.
	// Live = status in (planned, sent, filled), created within ExposureWindow.
	MaxPositionUSDC float64

	// MaxTotalExposureUSDC caps the sum across all live positions.
	MaxTotalExposureUSDC float64

	// MaxOrdersPerHour rate-caps the submitter. Even if every other gate
	// passes, ≥this many non-rejected/canceled rows in the last hour
	// blocks new ones.
	MaxOrdersPerHour int

	// CooldownPerMatchup is the min interval between two non-rejected
	// orders on the same (matchup, token, side). Defends against
	// signal-engine emitting the same BUY_CANDIDATE repeatedly per tick.
	CooldownPerMatchup time.Duration

	// ExposureWindow is the look-back used by MaxPosition / MaxTotalExposure.
	// Reasonable default: 24h.
	ExposureWindow time.Duration

	// KillSwitchPath, if non-empty AND file exists, blocks every order
	// with "kill switch engaged". Cheap defense: operator runs
	// `touch ./var/clob-kill-switch` to stop trading.
	KillSwitchPath string

	// MinSizeUSDC refuses tiny orders (defense against accidental tiny
	// signals; CLOB also has its own min-size on the contract).
	MinSizeUSDC float64

	// MaxSizeUSDCPerOrder caps the size of one individual order even
	// before MaxPosition is checked.
	MaxSizeUSDCPerOrder float64
}

// Validate returns warnings (not errors) for fields that are zero
// because the operator probably forgot, not because they intentionally
// disabled the cap. Caller decides whether to refuse to start.
func (c RiskConfig) Validate() []string {
	var warns []string
	if c.MaxPositionUSDC <= 0 {
		warns = append(warns, "MaxPositionUSDC=0 (per-token cap DISABLED)")
	}
	if c.MaxTotalExposureUSDC <= 0 {
		warns = append(warns, "MaxTotalExposureUSDC=0 (total exposure cap DISABLED)")
	}
	if c.MaxOrdersPerHour <= 0 {
		warns = append(warns, "MaxOrdersPerHour=0 (rate cap DISABLED)")
	}
	if c.CooldownPerMatchup <= 0 {
		warns = append(warns, "CooldownPerMatchup=0 (signal dedupe DISABLED — risk of spamming same matchup)")
	}
	if c.ExposureWindow <= 0 {
		warns = append(warns, "ExposureWindow=0 (look-back DISABLED — exposure sums everything ever recorded)")
	}
	if c.KillSwitchPath == "" {
		warns = append(warns, "KillSwitchPath='' (file-based kill switch DISABLED — must SIGTERM/kill the process to stop)")
	}
	if c.MaxSizeUSDCPerOrder <= 0 {
		warns = append(warns, "MaxSizeUSDCPerOrder=0 (per-order size cap DISABLED)")
	}
	return warns
}

// ExposureStore is the slice of ProposedOrderStore the risk gate needs.
// Lets tests inject a fake without pulling the whole pgx tower.
type ExposureStore interface {
	ExposureForToken(ctx context.Context, tokenID, side string, window time.Duration) (float64, error)
	TotalExposure(ctx context.Context, window time.Duration) (float64, error)
	CountOrdersInWindow(ctx context.Context, window time.Duration) (int, error)
	HasRecentOrderFor(ctx context.Context, matchupID int64, tokenID, side string, window time.Duration) (bool, error)
}

// OrderCandidate is the minimum information the risk gate needs to
// decide on one prospective order. Keeps the gate decoupled from
// executor.OrderSpec so the same code can vet historical / paper
// trades too.
type OrderCandidate struct {
	MatchupID int64
	TokenID   string
	Side      string // "BUY" / "SELL"
	SizeUSDC  float64
}

// Decision is the gate result. Allowed=true means submit; false means
// refuse with Reason. NeverPropose=true is an extra-hard nope used for
// kill-switch and dedupe so the submitter knows not to retry on the
// same tick.
type Decision struct {
	Allowed      bool
	Reason       string
	NeverPropose bool
}

// Evaluate runs all gates in deterministic order. Stops at the first
// failure (cheap defenses first; expensive DB queries last). The
// returned Reason includes the actual numbers so the operator can
// understand why an order was blocked from the log line alone.
func (c RiskConfig) Evaluate(ctx context.Context, store ExposureStore, cand OrderCandidate) (Decision, error) {
	// 1) Kill switch (filesystem) — cheapest, most important.
	if c.KillSwitchPath != "" {
		if _, err := os.Stat(c.KillSwitchPath); err == nil {
			return Decision{Reason: fmt.Sprintf("kill switch engaged (%s exists)", c.KillSwitchPath), NeverPropose: true}, nil
		}
	}
	if cand.SizeUSDC <= 0 {
		return Decision{Reason: fmt.Sprintf("size %.2f <= 0", cand.SizeUSDC), NeverPropose: true}, nil
	}
	// 2) Per-order size caps (in-memory, no DB).
	if c.MinSizeUSDC > 0 && cand.SizeUSDC < c.MinSizeUSDC {
		return Decision{Reason: fmt.Sprintf("size %.2f < MinSizeUSDC %.2f", cand.SizeUSDC, c.MinSizeUSDC), NeverPropose: true}, nil
	}
	if c.MaxSizeUSDCPerOrder > 0 && cand.SizeUSDC > c.MaxSizeUSDCPerOrder {
		return Decision{Reason: fmt.Sprintf("size %.2f > MaxSizeUSDCPerOrder %.2f", cand.SizeUSDC, c.MaxSizeUSDCPerOrder), NeverPropose: true}, nil
	}
	if strings.TrimSpace(cand.TokenID) == "" {
		return Decision{Reason: "empty token id", NeverPropose: true}, nil
	}
	// 3) DB-backed checks: cooldown, position cap, total exposure cap, rate cap.
	if c.CooldownPerMatchup > 0 {
		recent, err := store.HasRecentOrderFor(ctx, cand.MatchupID, cand.TokenID, cand.Side, c.CooldownPerMatchup)
		if err != nil {
			return Decision{}, err
		}
		if recent {
			return Decision{Reason: fmt.Sprintf("cooldown %s active for (matchup=%d, token=%s, side=%s)",
				c.CooldownPerMatchup, cand.MatchupID, truncMid(cand.TokenID, 12), cand.Side)}, nil
		}
	}
	window := c.ExposureWindow
	if window <= 0 {
		window = 100 * 365 * 24 * time.Hour // effectively forever
	}
	if c.MaxPositionUSDC > 0 {
		exp, err := store.ExposureForToken(ctx, cand.TokenID, cand.Side, window)
		if err != nil {
			return Decision{}, err
		}
		if exp+cand.SizeUSDC > c.MaxPositionUSDC {
			return Decision{Reason: fmt.Sprintf("per-token cap: %.2f current + %.2f new > %.2f max",
				exp, cand.SizeUSDC, c.MaxPositionUSDC)}, nil
		}
	}
	if c.MaxTotalExposureUSDC > 0 {
		exp, err := store.TotalExposure(ctx, window)
		if err != nil {
			return Decision{}, err
		}
		if exp+cand.SizeUSDC > c.MaxTotalExposureUSDC {
			return Decision{Reason: fmt.Sprintf("total exposure cap: %.2f current + %.2f new > %.2f max",
				exp, cand.SizeUSDC, c.MaxTotalExposureUSDC)}, nil
		}
	}
	if c.MaxOrdersPerHour > 0 {
		n, err := store.CountOrdersInWindow(ctx, time.Hour)
		if err != nil {
			return Decision{}, err
		}
		if n >= c.MaxOrdersPerHour {
			return Decision{Reason: fmt.Sprintf("rate cap: %d orders in last hour ≥ %d max",
				n, c.MaxOrdersPerHour)}, nil
		}
	}
	return Decision{Allowed: true}, nil
}

func truncMid(s string, head int) string {
	if len(s) <= head*2+1 {
		return s
	}
	return s[:head] + "…" + s[len(s)-head:]
}
