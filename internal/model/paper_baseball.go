package model

import (
	"encoding/json"
	"time"
)

// PaperBaseballSignal is one row of paper_baseball_signals — an audit
// record for a single LSPORTS score-change event we considered, plus the
// five-stage latency breakdown and the link to the signals(id) row we
// inserted into the existing signal pipeline (or skipped, with a reason).
type PaperBaseballSignal struct {
	LSportsFixtureID int64

	LSportsEventAt          time.Time
	LSportsSourcePublishAt  *time.Time
	ScoreBefore             string // e.g., "2-3"
	ScoreAfter              string // e.g., "2-4"
	ScoringSide             string // 'home' | 'away'
	HomeTeamName            string
	AwayTeamName            string

	TargetPolymarketMarketID    string
	TargetPolymarketConditionID string
	TargetPolymarketTokenID     string
	TargetOutcomeName           string
	TargetOutcomeSide           string

	PMBestAsk      *float64
	PMBestBid      *float64
	PMMidpoint     *float64
	PMSpread       *float64
	PMLiquidity    *float64
	PMSnapshotAt   time.Time

	SignalID         *int64
	SignalSkipReason string

	LSportsToDaemonMS            int
	DaemonToTokenLookupMS        int
	TokenLookupToPMSnapshotMS    int
	PMSnapshotToSignalInsertMS   int
	TotalSignalLatencyMS         int

	RawPayload json.RawMessage
}

// SyntheticBuySignal is the projection inserted into the existing signals
// table for an LSPORTS-driven paper trade. Pinnacle columns are filled
// with dummy values just to satisfy the signals_buy_candidates_fresh view
// filter (pinnacle_age_sec <= 15 AND state = 'BUY_CANDIDATE'). The
// JSONB edge_snapshot column carries the source attribution so a future
// V2 migration can demote this row to its own LSPORTS-source view cleanly.
type SyntheticBuySignal struct {
	// Identity
	MatchupID              int64  // we reuse this column for lsports_fixture_id
	PolymarketConditionID  string
	PolymarketTokenID      string
	OutcomeSide            string // 'yes' or 'no' (PM token side)
	OutcomeName            string // e.g., 'Cincinnati Reds'
	HomeName               string
	AwayName               string
	LeagueName             string // 'MLB'

	// PM book snapshot at signal time
	PMAsk       *float64
	PMBid       *float64
	PMMid       *float64
	PMSpread    *float64
	PMLiquidity *float64
	PMAgeSec    float64 // seconds since pm snapshot

	// Edge audit JSONB (source attribution + LSPORTS event details)
	EdgeSnapshot json.RawMessage
}
