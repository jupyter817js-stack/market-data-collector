package model

import (
	"encoding/json"
	"time"
)

const (
	SourceOddsAPI    = "the_odds_api"
	SourcePolymarket = "polymarket"
	SourceBetfair    = "betfair"
)

const (
	SourceRunStatusSuccess  = "success"
	SourceRunStatusError    = "error"
	SourceRunStatusCanceled = "canceled"
)

type FetchBatch struct {
	Source     string
	SportKey   string
	RequestURL string
	StatusCode int
	Headers    map[string]string
	Payload    json.RawMessage
	PayloadSHA string
	ReceivedAt time.Time
	Events     []EventOdds
}

type EventOdds struct {
	ID           string      `json:"id"`
	SportKey     string      `json:"sport_key"`
	SportTitle   string      `json:"sport_title"`
	CommenceTime time.Time   `json:"commence_time"`
	HomeTeam     string      `json:"home_team"`
	AwayTeam     string      `json:"away_team"`
	Bookmakers   []Bookmaker `json:"bookmakers"`
}

type Bookmaker struct {
	Key        string    `json:"key"`
	Title      string    `json:"title"`
	LastUpdate time.Time `json:"last_update"`
	Markets    []Market  `json:"markets"`
}

type Market struct {
	Key        string     `json:"key"`
	LastUpdate *time.Time `json:"last_update,omitempty"`
	Outcomes   []Outcome  `json:"outcomes"`
}

type Outcome struct {
	Name  string   `json:"name"`
	Price float64  `json:"price"`
	Point *float64 `json:"point,omitempty"`
}

type SaveStats struct {
	RawSnapshotsInserted int
	EventsSeen           int
	QuotesSeen           int
	QuotesInserted       int
	QuotesUnchanged      int
	EventsSkippedLive    int
}

type BetfairSaveStats struct {
	RawMessagesInserted int
	CandidatesSeen      int
	CandidatesInserted  int
	CandidatesUnchanged int
	CandidatesSkipped   int
}

const (
	BetfairLivePilotRunStatusRunning  = "running"
	BetfairLivePilotRunStatusSuccess  = "success"
	BetfairLivePilotRunStatusFailed   = "failed"
	BetfairLivePilotRunStatusCanceled = "canceled"
)

type BetfairLivePilotRunStart struct {
	RunLabel             string
	StreamID             string
	StartedAt            time.Time
	AddressRedacted      string
	ServerName           string
	MarketIDs            []string
	DurationLimitSeconds *int
	MaxMessages          *int
}

type BetfairLivePilotRunFinish struct {
	FinishedAt           time.Time
	Status               string
	StopReason           string
	FailureReason        string
	MessagesRead         int
	MarketChangeMessages int
	HeartbeatMessages    int
	StatusMessages       int
	RawMessagesSeen      int
	RawMessagesInserted  int
	CandidatesEmitted    int
	CandidatesInserted   int
	CandidatesUnchanged  int
	CandidatesSkipped    int
	MarketsSeen          int
	LastClock            string
	LastInitialClock     string
	LastMessageTime      *time.Time
	HeartbeatState       string
	ExitCode             int
	ElapsedMS            int64
}

type BetfairLivePilotRun struct {
	ID                   int64
	RunLabel             string
	StreamID             string
	StartedAt            time.Time
	FinishedAt           *time.Time
	Status               string
	StopReason           string
	FailureReason        string
	AddressRedacted      string
	ServerName           string
	MarketIDsCount       int
	MarketIDs            []string
	DurationLimitSeconds *int
	MaxMessages          *int
	MessagesRead         int
	MarketChangeMessages int
	HeartbeatMessages    int
	StatusMessages       int
	RawMessagesSeen      int
	RawMessagesInserted  int
	CandidatesEmitted    int
	CandidatesInserted   int
	CandidatesUnchanged  int
	CandidatesSkipped    int
	MarketsSeen          int
	LastClock            string
	LastInitialClock     string
	LastMessageTime      *time.Time
	HeartbeatState       string
	ExitCode             *int
	ElapsedMS            *int64
	CreatedAt            time.Time
	UpdatedAt            time.Time
}

type BetfairLivePilotReadinessCount struct {
	ReadinessState string
	RowCount       int64
}

type BetfairLivePilotPostflight struct {
	Run                   BetfairLivePilotRun
	LinkedRawMessageCount int64
	QuoteHistoryRows      int64
	LatestQuoteCount      int64
	MappedQuoteCount      int64
	ReadyRunnerBookCount  int64
	ReadyComparisonCount  int64
	ReadinessStateCounts  []BetfairLivePilotReadinessCount
	Warnings              []string
}

const (
	BetfairRuntimeRunStatusRunning  = "running"
	BetfairRuntimeRunStatusSuccess  = "success"
	BetfairRuntimeRunStatusFailed   = "failed"
	BetfairRuntimeRunStatusCanceled = "canceled"
)

type BetfairRuntimeRunStart struct {
	RunLabel             string
	StreamID             string
	RuntimeMode          string
	TransportMode        string
	SinkType             string
	StartedAt            time.Time
	MarketIDs            []string
	DurationLimitSeconds *int
	MaxMessages          *int
}

type BetfairRuntimeRunFinish struct {
	StreamID                string
	FinishedAt              time.Time
	Status                  string
	ReadinessState          string
	StoppedReason           string
	FailureReason           string
	MessagesRead            int
	MarketChangeMessages    int
	HeartbeatMessages       int
	StatusMessages          int
	RawMessagesSeen         int
	RawMessagesFlushed      int
	RawMessagesInserted     int
	CandidatesEmitted       int
	CandidateBatchesSeen    int
	CandidateBatchesFlushed int
	CandidatesInserted      int
	CandidatesUnchanged     int
	CandidatesSkipped       int
	FlushCount              int
	FlushErrors             int
	SinkErrors              int
	MarketsSeen             int
	ReconnectCount          int
	LastClock               string
	LastInitialClock        string
	LastMessageTime         *time.Time
	LastCandidateTime       *time.Time
	LastSinkFlushTime       *time.Time
	HeartbeatState          string
	ElapsedMS               int64
	ExitCode                int
}

type BetfairRuntimeRun struct {
	ID                      int64
	RunLabel                string
	StreamID                string
	RuntimeMode             string
	TransportMode           string
	SinkType                string
	StartedAt               time.Time
	FinishedAt              *time.Time
	Status                  string
	ReadinessState          string
	StoppedReason           string
	FailureReason           string
	MarketIDsCount          int
	MarketIDs               []string
	DurationLimitSeconds    *int
	MaxMessages             *int
	MessagesRead            int
	MarketChangeMessages    int
	HeartbeatMessages       int
	StatusMessages          int
	RawMessagesSeen         int
	RawMessagesFlushed      int
	RawMessagesInserted     int
	CandidatesEmitted       int
	CandidateBatchesSeen    int
	CandidateBatchesFlushed int
	CandidatesInserted      int
	CandidatesUnchanged     int
	CandidatesSkipped       int
	FlushCount              int
	FlushErrors             int
	SinkErrors              int
	MarketsSeen             int
	ReconnectCount          int
	LastClock               string
	LastInitialClock        string
	LastMessageTime         *time.Time
	LastCandidateTime       *time.Time
	LastSinkFlushTime       *time.Time
	HeartbeatState          string
	ElapsedMS               *int64
	ExitCode                *int
	CreatedAt               time.Time
	UpdatedAt               time.Time
}

type BetfairRuntimeReadinessCount struct {
	ReadinessState string
	RowCount       int64
}

type BetfairRuntimePostflight struct {
	Run                   BetfairRuntimeRun
	LinkedRawMessageCount int64
	QuoteHistoryRows      int64
	LatestQuoteCount      int64
	MappedQuoteCount      int64
	ReadyRunnerBookCount  int64
	ReadyComparisonCount  int64
	ReadinessStateCounts  []BetfairRuntimeReadinessCount
	Warnings              []string
}

type BetfairRuntimeVerificationReport struct {
	RuntimeRun     BetfairRuntimeVerificationRunSummary     `json:"runtime_run"`
	QuoteCandidate BetfairRuntimeQuoteCandidateSummary      `json:"quote_candidate"`
	MappedQuote    BetfairRuntimeMappedQuoteSummary         `json:"mapped_quote"`
	Readiness      BetfairRuntimeReadinessVerification      `json:"readiness"`
	Classification BetfairRuntimeVerificationClassification `json:"classification"`
}

type BetfairRuntimeVerificationRunSummary struct {
	RunID               int64      `json:"run_id"`
	RunLabel            string     `json:"run_label"`
	StartedAt           time.Time  `json:"started_at"`
	StoppedAt           *time.Time `json:"stopped_at,omitempty"`
	Status              string     `json:"status"`
	SinkType            string     `json:"sink_type"`
	MarketIDs           []string   `json:"market_ids"`
	RawMessagesInserted int        `json:"raw_messages_inserted"`
	CandidatesInserted  int        `json:"candidates_inserted"`
	SinkErrors          int        `json:"sink_errors"`
	FlushErrors         int        `json:"flush_errors"`
}

type BetfairRuntimeQuoteCandidateSummary struct {
	CandidateCount      int64    `json:"candidate_count"`
	DistinctMarketCount int64    `json:"distinct_market_count"`
	DistinctRunnerCount int64    `json:"distinct_runner_count"`
	HasBack             bool     `json:"has_back"`
	HasLay              bool     `json:"has_lay"`
	MinOdds             *float64 `json:"min_odds,omitempty"`
	MaxOdds             *float64 `json:"max_odds,omitempty"`
	MinAvailableSize    *float64 `json:"min_available_size,omitempty"`
	MaxAvailableSize    *float64 `json:"max_available_size,omitempty"`
}

type BetfairRuntimeMappedQuoteSummary struct {
	CanonicalEventIDs       []string         `json:"canonical_event_ids"`
	CanonicalMarketIDs      []string         `json:"canonical_market_ids"`
	RunnerAliasStatusCounts map[string]int64 `json:"runner_alias_status_counts"`
	MappedRunnersCount      int64            `json:"mapped_runners_count"`
	UnmappedRunnersCount    int64            `json:"unmapped_runners_count"`
}

type BetfairRuntimeReadinessVerification struct {
	ReadyRowCount        int64            `json:"ready_row_count"`
	WarningRowCount      int64            `json:"warning_row_count"`
	BlockedRowCount      int64            `json:"blocked_row_count"`
	ReadinessStateCounts map[string]int64 `json:"readiness_state_counts"`
	AnomalyReasonCounts  map[string]int64 `json:"anomaly_reason_counts"`
	StaleQuoteCount      int64            `json:"stale_quote_count"`
	LowConfidenceCount   int64            `json:"low_confidence_count"`
	ThinLiquidityCount   int64            `json:"thin_liquidity_count"`
}

type BetfairRuntimeVerificationClassification struct {
	IngestionPass    bool `json:"ingestion_pass"`
	MappingPass      bool `json:"mapping_pass"`
	ReadinessPass    bool `json:"readiness_pass"`
	ExecutionAllowed bool `json:"execution_allowed"`
}

type BetfairSportsbookCoverageAuditOptions struct {
	CanonicalEventID string
	BetfairMarketID  string
	Recent           bool
	Limit            int
}

type BetfairSportsbookCoverageAuditRow struct {
	CanonicalEventID                string     `json:"canonical_event_id"`
	SportKey                        string     `json:"sport_key"`
	League                          string     `json:"league"`
	HomeTeam                        string     `json:"home_team"`
	AwayTeam                        string     `json:"away_team"`
	CommenceTime                    time.Time  `json:"commence_time"`
	BetfairMarketID                 string     `json:"betfair_market_id,omitempty"`
	CanonicalMarketFamily           string     `json:"canonical_market_family,omitempty"`
	MappedBetfairQuoteCount         int64      `json:"mapped_betfair_quote_count"`
	MappedBetfairRunnerCount        int64      `json:"mapped_betfair_runner_count"`
	BetfairRunnerBookCount          int64      `json:"betfair_runner_book_count"`
	ReadyBetfairRunnerBookCount     int64      `json:"ready_betfair_runner_book_count"`
	SportsbookConsensusOutcomeCount int64      `json:"sportsbook_consensus_outcome_count"`
	ComparisonRowCount              int64      `json:"comparison_row_count"`
	ReadyComparisonRowCount         int64      `json:"ready_comparison_row_count"`
	ReadinessStates                 []string   `json:"readiness_states,omitempty"`
	LatestBetfairReceivedAt         *time.Time `json:"latest_betfair_received_at,omitempty"`
	LatestSportsbookReceivedAt      *time.Time `json:"latest_sportsbook_received_at,omitempty"`
	GapReason                       string     `json:"reason"`
	BetfairMapped                   bool       `json:"betfair_mapped"`
	BetfairReadyRunnerBooks         bool       `json:"betfair_ready_runner_books"`
	SportsbookConsensus             bool       `json:"sportsbook_consensus"`
	ComparisonReady                 bool       `json:"comparison_ready"`
}

type PolymarketComparisonReadinessOptions struct {
	CanonicalEventID   string
	BetfairMarketID    string
	PolymarketEventID  string
	PolymarketMarketID string
	SportKey           string
	Recent             bool
	Candidates         bool
	CommenceAfter      *time.Time
	CommenceBefore     *time.Time
	Limit              int
}

type PolymarketComparisonReadinessRow struct {
	CanonicalEventID                string     `json:"canonical_event_id"`
	SportKey                        string     `json:"sport_key"`
	League                          string     `json:"league"`
	HomeTeam                        string     `json:"home_team"`
	AwayTeam                        string     `json:"away_team"`
	CommenceTime                    time.Time  `json:"commence_time"`
	PolymarketAliasID               *int64     `json:"polymarket_alias_id,omitempty"`
	PolymarketEventID               string     `json:"polymarket_event_id,omitempty"`
	PolymarketMarketID              string     `json:"polymarket_market_id,omitempty"`
	PolymarketConditionID           string     `json:"polymarket_condition_id,omitempty"`
	PolymarketAliasStatus           string     `json:"polymarket_alias_status,omitempty"`
	PolymarketAliasConfidence       *float64   `json:"polymarket_alias_confidence,omitempty"`
	PolymarketAliasUpdatedAt        *time.Time `json:"polymarket_alias_updated_at,omitempty"`
	PolymarketQuestion              string     `json:"polymarket_question,omitempty"`
	PolymarketMarketActive          *bool      `json:"polymarket_market_active,omitempty"`
	PolymarketMarketClosed          *bool      `json:"polymarket_market_closed,omitempty"`
	PolymarketMarketArchived        *bool      `json:"polymarket_market_archived,omitempty"`
	PolymarketMarketEndTime         *time.Time `json:"polymarket_market_end_time,omitempty"`
	PolymarketTokenCount            int64      `json:"polymarket_token_count"`
	PolymarketPriceSnapshotCount    int64      `json:"polymarket_price_snapshot_count"`
	PolymarketLatestPriceTime       *time.Time `json:"polymarket_latest_price_time,omitempty"`
	HasBestBid                      bool       `json:"has_best_bid"`
	HasBestAsk                      bool       `json:"has_best_ask"`
	HasDepthOrLiquidity             bool       `json:"has_depth_or_liquidity"`
	PolymarketMaxLiquidity          *float64   `json:"polymarket_max_liquidity,omitempty"`
	PolymarketMinSpread             *float64   `json:"polymarket_min_spread,omitempty"`
	SportsbookConsensusOutcomeCount int64      `json:"sportsbook_consensus_outcome_count"`
	LatestSportsbookReceivedAt      *time.Time `json:"latest_sportsbook_received_at,omitempty"`
	BetfairMappedQuoteCount         int64      `json:"betfair_mapped_quote_count"`
	BetfairReadyRunnerBookCount     int64      `json:"betfair_ready_runner_book_count"`
	LatestBetfairReceivedAt         *time.Time `json:"latest_betfair_received_at,omitempty"`
	PolymarketEdgeRowCount          int64      `json:"polymarket_edge_row_count"`
	ReadyPolymarketEdgeRowCount     int64      `json:"ready_polymarket_edge_row_count"`
	ReadinessStates                 []string   `json:"readiness_states,omitempty"`
	PolymarketStaleState            string     `json:"polymarket_stale_state,omitempty"`
	ResolutionStatus                string     `json:"resolution_status,omitempty"`
	ResolutionVerified              bool       `json:"resolution_verified"`
	HasSportsbookConsensus          bool       `json:"has_sportsbook_consensus"`
	HasReadyBetfair                 bool       `json:"has_ready_betfair"`
	HasConfirmedPolymarketAlias     bool       `json:"has_confirmed_polymarket_alias"`
	HasPolymarketMarketMetadata     bool       `json:"has_polymarket_market_metadata"`
	HasPolymarketTokens             bool       `json:"has_polymarket_tokens"`
	HasBestBidAndAsk                bool       `json:"has_best_bid_and_ask"`
	GrossComparisonPossible         bool       `json:"gross_comparison_possible"`
	NetComparisonPossible           bool       `json:"net_comparison_possible"`
	GapReason                       string     `json:"gap_reason"`
	NextManualAction                string     `json:"next_manual_action"`
	ComparisonReady                 bool       `json:"comparison_ready"`
}

// PolymarketCandidateSearch defines the inputs to a read-only local
// Polymarket candidate lookup for a canonical event.
type PolymarketCandidateSearch struct {
	CanonicalEventID string
	Limit            int
}

// PolymarketCandidate is one local Polymarket market that may correspond to a
// canonical event. It is a diagnostic row only — the operator decides whether
// to confirm an alias via cmd/match-review.
type PolymarketCandidate struct {
	PolymarketEventID    string     `json:"polymarket_event_id,omitempty"`
	PolymarketEventSlug  string     `json:"polymarket_event_slug,omitempty"`
	PolymarketEventTitle string     `json:"polymarket_event_title,omitempty"`
	EventStartTime       *time.Time `json:"event_start_time,omitempty"`
	EventEndTime         *time.Time `json:"event_end_time,omitempty"`
	EventActive          *bool      `json:"event_active,omitempty"`
	EventClosed          *bool      `json:"event_closed,omitempty"`
	EventArchived        *bool      `json:"event_archived,omitempty"`
	PolymarketMarketID   string     `json:"polymarket_market_id,omitempty"`
	ConditionID          string     `json:"condition_id,omitempty"`
	MarketSlug           string     `json:"market_slug,omitempty"`
	MarketQuestion       string     `json:"market_question,omitempty"`
	MarketType           string     `json:"market_type,omitempty"`
	MarketActive         *bool      `json:"market_active,omitempty"`
	MarketClosed         *bool      `json:"market_closed,omitempty"`
	MarketArchived       *bool      `json:"market_archived,omitempty"`
	MarketEndTime        *time.Time `json:"market_end_time,omitempty"`
	TokenCount           int64      `json:"token_count"`
	LatestPriceTime      *time.Time `json:"latest_price_time,omitempty"`
	LatestBestBid        *float64   `json:"latest_best_bid,omitempty"`
	LatestBestAsk        *float64   `json:"latest_best_ask,omitempty"`
	LatestMidpoint       *float64   `json:"latest_midpoint,omitempty"`
	HasBestBidAndAsk     bool       `json:"has_best_bid_and_ask"`
	MatchedHomeToken     string     `json:"matched_home_token,omitempty"`
	MatchedAwayToken     string     `json:"matched_away_token,omitempty"`
	MatchedSource        string     `json:"matched_source,omitempty"`
	HasConfirmedAlias    bool       `json:"has_confirmed_alias_for_canonical_event"`
	HasUnresolvedReview  bool       `json:"has_unresolved_review"`
	HasAmbiguousReview   bool       `json:"has_ambiguous_review"`
}

// PolymarketCandidateReport is the full result of a candidate-search request.
type PolymarketCandidateReport struct {
	CanonicalEvent      CanonicalEvent        `json:"canonical_event"`
	HomeTokens          []string              `json:"home_search_tokens"`
	AwayTokens          []string              `json:"away_search_tokens"`
	Candidates          []PolymarketCandidate `json:"candidates"`
	HasConfirmedAlias   bool                  `json:"has_confirmed_alias_for_canonical_event"`
	UnresolvedReviewIDs []int64               `json:"unresolved_review_ids,omitempty"`
}

// PolymarketLatencyObservation is one row to be inserted into
// polymarket_latency_observations. Used by cmd/polymarket-latency-capture
// to record the PM CLOB orderbook state for a token paired with a watched
// LSPORTS fixture, at ms-resolution local time.
type PolymarketLatencyObservation struct {
	LSportsFixtureID       int64
	PolymarketMarketID     string
	PolymarketConditionID  string
	PolymarketTokenID      string
	ReceivedAt             *time.Time
	SourcePublishTime      *time.Time
	BestBid                *float64
	BestAsk                *float64
	Midpoint               *float64
	LastPrice              *float64
	Spread                 *float64
	Liquidity              *float64
	PriceChangedFromPrior  *bool
	RawPayload             json.RawMessage
}

// LSportsInplayObservation is one row to be inserted into
// lsports_inplay_observations. Most fields are optional; observation_kind
// and raw_payload are the minimum. received_at is set by the database
// default (clock_timestamp()) unless the caller supplies it.
type LSportsInplayObservation struct {
	LSportsFixtureID  int64
	Source            string // 'snapshot' | 'amqp_push'
	ObservationKind   string // 'livescore' | 'market' | 'settlement' | 'fixture_meta' | 'heartbeat' | 'keepalive' | 'other'
	ReceivedAt        *time.Time
	SourcePublishTime *time.Time
	HomeScore         *int
	AwayScore         *int
	CurrentPeriod     *int
	MatchStatus       *int
	IsInplay          *bool
	MarketID          *int
	BetName           string
	BetPrice          *float64
	BetLine           *float64
	RawPayload        json.RawMessage
	PayloadSHA256     string
	Notes             string
}

// LSPORTS TRADE360 in-play subscription ledger row.
//
// The trial cap (100 fixtures over 14 days, (C) refund-if-before-InProgress
// semantics) is enforced by gating Subscribe/Unsubscribe through this ledger
// and refusing any second-charge on a fixture that already has an open row.
type LSportsSubscription struct {
	ID                             int64
	LSportsFixtureID               int64
	LSportsSportID                 *int64
	LSportsLeagueID                *int64
	FixtureName                    string
	HomeParticipant                string
	AwayParticipant                string
	CommenceTime                   *time.Time
	Status                         string
	QuotaRemainingBeforeSubscribe  *int64
	QuotaRemainingAfterSubscribe   *int64
	QuotaRemainingAfterUnsubscribe *int64
	Operator                       string
	SubscribeReason                string
	UnsubscribeReason              string
	PolymarketCoverageVerified     bool
	PolymarketCoverageNotes        string
	PolymarketCanonicalEventID     string
	SubscribedAt                   time.Time
	InProgressAt                   *time.Time
	UnsubscribedAt                 *time.Time
	SettledAt                      *time.Time
	Notes                          string
	CreatedAt                      time.Time
	UpdatedAt                      time.Time
}

const (
	LSportsSubscriptionStatusPending             = "pending"
	LSportsSubscriptionStatusCommittedInProgress = "committed_inprogress"
	LSportsSubscriptionStatusRefunded            = "refunded"
	LSportsSubscriptionStatusUnsubscribedTooLate = "unsubscribed_too_late"
	LSportsSubscriptionStatusSettled             = "settled"
)

// LSportsSubscribeInput is the set of facts an operator must supply before
// any /Fixtures/Subscribe call. The store rejects empty operator/reason and
// any second open row for the same fixture.
type LSportsSubscribeInput struct {
	LSportsFixtureID              int64
	LSportsSportID                *int64
	LSportsLeagueID               *int64
	FixtureName                   string
	HomeParticipant               string
	AwayParticipant               string
	CommenceTime                  *time.Time
	Operator                      string
	SubscribeReason               string
	QuotaRemainingBeforeSubscribe int
	QuotaRemainingAfterSubscribe  int
	PolymarketCoverageVerified    bool
	PolymarketCoverageNotes       string
	PolymarketCanonicalEventID    string
}

type CanonicalEvent struct {
	ID           string
	SportKey     string
	League       string
	HomeTeam     string
	AwayTeam     string
	HomeTeamNorm string
	AwayTeamNorm string
	CommenceTime time.Time
	Status       string
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

type CanonicalEventCreateInput struct {
	SportKey     string
	League       string
	HomeTeam     string
	AwayTeam     string
	CommenceTime time.Time
	Status       string
}

type CanonicalEventCreateResult struct {
	Event   CanonicalEvent
	Created bool
}

type BetfairMapListOptions struct {
	Limit    int
	EventID  string
	MarketID string
	Status   string
}

type BetfairUnmappedEvent struct {
	BetfairEventID             string
	BetfairEventName           string
	QuoteHistoryRows           int64
	MarketCount                int64
	LatestReceivedAt           *time.Time
	LatestAliasStatus          string
	LatestAliasConfidenceScore *float64
	LatestAliasMatchReason     string
	LatestAliasUpdatedAt       *time.Time
}

type BetfairUnmappedMarket struct {
	BetfairMarketID            string
	BetfairEventID             string
	BetfairEventName           string
	BetfairMarketName          string
	BetfairMarketFamily        string
	QuoteHistoryRows           int64
	RunnerCount                int64
	LatestReceivedAt           *time.Time
	LatestAliasStatus          string
	LatestAliasMarketFamily    string
	LatestAliasConfidenceScore *float64
	LatestAliasMatchReason     string
	LatestAliasUpdatedAt       *time.Time
}

type BetfairUnmappedRunner struct {
	BetfairMarketID            string
	BetfairEventID             string
	BetfairEventName           string
	BetfairMarketName          string
	SelectionID                int64
	Handicap                   float64
	RunnerName                 string
	QuoteHistoryRows           int64
	ExchangeSideCount          int64
	LatestReceivedAt           *time.Time
	LatestAliasStatus          string
	LatestAliasOutcomeName     string
	LatestAliasOutcomeSide     string
	LatestAliasConfidenceScore *float64
	LatestAliasMatchReason     string
	LatestAliasUpdatedAt       *time.Time
}

type BetfairMappingConflict struct {
	IssueType           string
	Severity            string
	Reason              string
	BetfairEventID      string
	BetfairMarketID     string
	SelectionID         *int64
	Handicap            *float64
	CanonicalEventID    string
	CanonicalEventIDs   string
	CanonicalEventCount int64
	ObservedAt          *time.Time
	Details             json.RawMessage
}

type BetfairConfirmedEventAlias struct {
	AliasID          int64
	BetfairEventID   string
	BetfairEventName string
	CanonicalEventID string
	SportKey         string
	League           string
	HomeTeam         string
	AwayTeam         string
	CommenceTime     time.Time
	AliasStatus      string
	ConfidenceScore  *float64
	MatchReason      string
	Reviewer         string
	ReviewReason     string
	DecidedAt        *time.Time
	AliasUpdatedAt   time.Time
}

type BetfairConfirmedMarketAlias struct {
	AliasID               int64
	BetfairMarketID       string
	BetfairEventID        string
	CanonicalEventID      string
	SportKey              string
	League                string
	HomeTeam              string
	AwayTeam              string
	CommenceTime          time.Time
	BetfairMarketType     string
	CanonicalMarketFamily string
	Line                  *float64
	AliasStatus           string
	ConfidenceScore       *float64
	MatchReason           string
	Reviewer              string
	ReviewReason          string
	DecidedAt             *time.Time
	AliasUpdatedAt        time.Time
}

type BetfairConfirmedRunnerAlias struct {
	AliasID              int64
	BetfairMarketID      string
	SelectionID          int64
	Handicap             float64
	RunnerName           string
	CanonicalOutcomeName string
	CanonicalOutcomeSide string
	AliasStatus          string
	ConfidenceScore      *float64
	MatchReason          string
	Reviewer             string
	ReviewReason         string
	DecidedAt            *time.Time
	AliasUpdatedAt       time.Time
}

type BetfairEventAliasDecision struct {
	BetfairEventID   string
	BetfairEventName string
	CanonicalEventID string
	AliasStatus      string
	Reviewer         string
	ReviewReason     string
	MatchReason      string
	ConfidenceScore  *float64
}

type BetfairMarketAliasDecision struct {
	BetfairMarketID       string
	BetfairEventID        string
	CanonicalEventID      string
	BetfairMarketType     string
	CanonicalMarketFamily string
	Line                  *float64
	AliasStatus           string
	Reviewer              string
	ReviewReason          string
	MatchReason           string
	ConfidenceScore       *float64
}

type BetfairRunnerAliasDecision struct {
	BetfairMarketID      string
	SelectionID          int64
	Handicap             float64
	RunnerName           string
	CanonicalOutcomeName string
	CanonicalOutcomeSide string
	AliasStatus          string
	Reviewer             string
	ReviewReason         string
	MatchReason          string
	ConfidenceScore      *float64
}

type SourceRun struct {
	Source          string
	SportKey        string
	StartedAt       time.Time
	FinishedAt      time.Time
	Status          string
	StatusCode      *int
	RequestURL      string
	ResponseHeaders map[string]string
	ErrorMessage    string
	Stats           SaveStats
}

type BetfairRawStreamMessage struct {
	Source            string
	StreamID          string
	Op                string
	MarketID          string
	PublishTimeMS     *int64
	SourcePublishTime *time.Time
	ChangeType        string
	Clock             string
	InitialClock      string
	RawJSON           json.RawMessage
	PayloadSHA256     string
	ReceivedAt        time.Time
}

type BetfairExchangeQuoteCandidate struct {
	Source              string
	QuoteKey            string
	QuoteHash           string
	MarketID            string
	SelectionID         int64
	Handicap            float64
	RunnerName          string
	MarketName          string
	EventID             string
	EventName           string
	MarketFamily        string
	ExchangeSide        string
	PriceDecimal        float64
	AvailableSize       float64
	LastTradedPrice     *float64
	TradedVolume        *float64
	MarketStatus        string
	InPlay              *bool
	MarketTime          string
	SourcePublishTimeMS *int64
	SourcePublishTime   *time.Time
	ReducerUpdatedAt    *time.Time
	ReceivedAt          time.Time
	AgeMS               *int64
	StaleState          string
	ConfidenceScore     float64
	RawMetadata         json.RawMessage
}

type PolymarketEvent struct {
	PolymarketEventID string
	Slug              string
	Title             string
	Description       string
	Category          string
	StartTime         *time.Time
	EndTime           *time.Time
	Active            *bool
	Closed            *bool
	Archived          *bool
	GammaPayload      json.RawMessage
}

type PolymarketMarket struct {
	PolymarketMarketID string
	PolymarketEventID  string
	ConditionID        string
	Question           string
	Slug               string
	MarketType         string
	Active             *bool
	Closed             *bool
	Archived           *bool
	EndTime            *time.Time
	GammaPayload       json.RawMessage
	CLOBPayload        json.RawMessage
}

type PolymarketToken struct {
	TokenID            string
	ConditionID        string
	PolymarketMarketID string
	OutcomeName        string
	OutcomeIndex       *int
	Side               string
	CLOBPayload        json.RawMessage
}

type PolymarketPriceSnapshot struct {
	TokenID            string
	ConditionID        string
	PolymarketMarketID string
	BidPrice           *float64
	AskPrice           *float64
	MidPrice           *float64
	LastPrice          *float64
	BestBidSize        *float64
	BestAskSize        *float64
	BestBid            *float64
	BestAsk            *float64
	Midpoint           *float64
	Spread             *float64
	LastTradePrice     *float64
	Liquidity          *float64
	DepthSummary       json.RawMessage
	SourceTimestamp    *time.Time
	RawPayload         json.RawMessage
	Source             string
	ReceivedAt         time.Time
	CLOBPayload        json.RawMessage
}

type PolymarketSyncStats struct {
	EventsUpserted         int
	MarketsUpserted        int
	TokensUpserted         int
	PriceSnapshotsInserted int
	PriceSnapshotFailures  int
}

type PolymarketSyncRun struct {
	StartedAt     time.Time
	FinishedAt    time.Time
	SyncType      string
	Status        string
	PagesFetched  int
	EventsSeen    int
	MarketsSeen   int
	TokensSeen    int
	PricesFetched int
	ErrorsCount   int
	ErrorMessage  string
}

const (
	MatchDecisionApproved = "approved"
	MatchDecisionRejected = "rejected"
)

type PolymarketMatchReview struct {
	ReviewID                   int64
	PolymarketEventID          string
	PolymarketMarketID         string
	ConditionID                string
	Slug                       string
	Title                      string
	SportKey                   string
	League                     string
	RawStartTime               *time.Time
	MatchStatus                string
	ConfidenceScore            *float64
	MatchReason                string
	CandidateCanonicalEventIDs string
	CreatedAt                  time.Time
	UpdatedAt                  time.Time
}

type PolymarketConfirmedAlias struct {
	AliasID            int64
	PolymarketEventID  string
	PolymarketMarketID string
	ConditionID        string
	CanonicalEventID   string
	SportKey           string
	HomeTeam           string
	AwayTeam           string
	CommenceTime       time.Time
	MatchStatus        string
	ConfidenceScore    *float64
	MatchReason        string
	Reviewer           string
	ReviewReason       string
	DecisionSource     string
	DecidedAt          *time.Time
	AliasUpdatedAt     time.Time
}

type PolymarketMatchDecision struct {
	PolymarketEventID  string
	PolymarketMarketID string
	ConditionID        string
	CanonicalEventID   string
	DecisionStatus     string
	DecisionSource     string
	Reviewer           string
	ReviewReason       string
	ConfidenceScore    *float64
	MatchReason        string
}

// PolymarketTokenAliasDecision is the operator-supplied input for approving
// or rejecting a single Polymarket-token-to-canonical-outcome mapping. All
// confirmed decisions require exact token_id, exact canonical_event_id, an
// explicit canonical_outcome_name AND canonical_outcome_side, plus a non-empty
// reviewer and reason. The store enforces these invariants before any write.
type PolymarketTokenAliasDecision struct {
	TokenID              string
	ConditionID          string
	PolymarketMarketID   string
	CanonicalEventID     string
	CanonicalOutcomeName string
	CanonicalOutcomeSide string
	DecisionStatus       string
	DecisionSource       string
	Reviewer             string
	Reason               string
	ConfidenceScore      *float64
}

type PolymarketTokenAlias struct {
	AliasID               int64
	TokenID               string
	ConditionID           string
	PolymarketMarketID    string
	CanonicalEventID      string
	SportKey              string
	HomeTeam              string
	AwayTeam              string
	CommenceTime          time.Time
	CanonicalOutcomeName  string
	CanonicalOutcomeSide  string
	Status                string
	ConfidenceScore       *float64
	Reviewer              string
	Reason                string
	DecisionSource        string
	DecidedAt             time.Time
	UpdatedAt             time.Time
	PolymarketOutcomeName string
	PolymarketYesNoSide   string
}

type PolymarketTokenAliasListOptions struct {
	CanonicalEventID string
	TokenID          string
	Status           string
	Limit            int
}
