package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/your-org/pol-odds-collector/internal/model"
)

const (
	defaultCoverageAuditLimit = 25
	maxCoverageAuditLimit     = 200
)

func (s *PostgresStore) ListBetfairSportsbookCoverageAudit(ctx context.Context, opts model.BetfairSportsbookCoverageAuditOptions) ([]model.BetfairSportsbookCoverageAuditRow, error) {
	opts = normalizeCoverageAuditOptions(opts)

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("begin coverage audit read-only transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	rows, err := tx.QueryContext(ctx, `
        SELECT
            canonical_event_id::text,
            sport_key,
            league,
            home_team,
            away_team,
            commence_time,
            betfair_market_id,
            canonical_market_family,
            mapped_betfair_quote_count,
            mapped_betfair_runner_count,
            betfair_runner_book_count,
            ready_betfair_runner_book_count,
            sportsbook_consensus_outcome_count,
            comparison_row_count,
            ready_comparison_row_count,
            readiness_states,
            latest_betfair_received_at,
            latest_sportsbook_received_at,
            gap_reason,
            betfair_mapped,
            betfair_ready_runner_books,
            sportsbook_consensus,
            comparison_ready
        FROM sportsbook_betfair_comparison_coverage_audit
        WHERE ($1 = '' OR canonical_event_id = $1::uuid)
          AND ($2 = '' OR betfair_market_id = $2)
          AND (
              $3
              OR $1 <> ''
              OR $2 <> ''
          )
          AND (
              NOT $3
              OR betfair_mapped
              OR sportsbook_consensus
              OR $1 <> ''
              OR $2 <> ''
          )
        ORDER BY
            coalesce(latest_betfair_received_at, latest_sportsbook_received_at, commence_time) DESC NULLS LAST,
            commence_time DESC,
            canonical_event_id,
            betfair_market_id NULLS LAST
        LIMIT $4
    `, opts.CanonicalEventID, opts.BetfairMarketID, opts.Recent, opts.Limit)
	if err != nil {
		return nil, fmt.Errorf("query sportsbook/betfair coverage audit: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := []model.BetfairSportsbookCoverageAuditRow{}
	for rows.Next() {
		item, err := scanBetfairSportsbookCoverageAuditRow(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate sportsbook/betfair coverage audit: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit coverage audit read-only transaction: %w", err)
	}
	return out, nil
}

func normalizeCoverageAuditOptions(opts model.BetfairSportsbookCoverageAuditOptions) model.BetfairSportsbookCoverageAuditOptions {
	opts.CanonicalEventID = strings.TrimSpace(opts.CanonicalEventID)
	opts.BetfairMarketID = strings.TrimSpace(opts.BetfairMarketID)
	if opts.Limit <= 0 {
		opts.Limit = defaultCoverageAuditLimit
	}
	if opts.Limit > maxCoverageAuditLimit {
		opts.Limit = maxCoverageAuditLimit
	}
	return opts
}

func scanBetfairSportsbookCoverageAuditRow(row matchReviewScanner) (model.BetfairSportsbookCoverageAuditRow, error) {
	var item model.BetfairSportsbookCoverageAuditRow
	var marketID, marketFamily, readinessStates, reason sql.NullString
	var latestBetfair, latestSportsbook sql.NullTime
	if err := row.Scan(
		&item.CanonicalEventID,
		&item.SportKey,
		&item.League,
		&item.HomeTeam,
		&item.AwayTeam,
		&item.CommenceTime,
		&marketID,
		&marketFamily,
		&item.MappedBetfairQuoteCount,
		&item.MappedBetfairRunnerCount,
		&item.BetfairRunnerBookCount,
		&item.ReadyBetfairRunnerBookCount,
		&item.SportsbookConsensusOutcomeCount,
		&item.ComparisonRowCount,
		&item.ReadyComparisonRowCount,
		&readinessStates,
		&latestBetfair,
		&latestSportsbook,
		&reason,
		&item.BetfairMapped,
		&item.BetfairReadyRunnerBooks,
		&item.SportsbookConsensus,
		&item.ComparisonReady,
	); err != nil {
		return item, fmt.Errorf("scan sportsbook/betfair coverage audit row: %w", err)
	}
	item.BetfairMarketID = marketID.String
	item.CanonicalMarketFamily = marketFamily.String
	item.CommenceTime = item.CommenceTime.UTC()
	item.ReadinessStates = splitCoverageStateList(readinessStates.String)
	item.LatestBetfairReceivedAt = nullableTimePtr(latestBetfair)
	item.LatestSportsbookReceivedAt = nullableTimePtr(latestSportsbook)
	item.GapReason = reason.String
	return item, nil
}

func splitCoverageStateList(value string) []string {
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func (s *PostgresStore) ListPolymarketComparisonReadiness(ctx context.Context, opts model.PolymarketComparisonReadinessOptions) ([]model.PolymarketComparisonReadinessRow, error) {
	opts = normalizePolymarketComparisonReadinessOptions(opts)

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("begin polymarket comparison readiness read-only transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	rows, err := tx.QueryContext(ctx, `
        SELECT
            canonical_event_id::text,
            sport_key,
            league,
            home_team,
            away_team,
            commence_time,
            polymarket_alias_id,
            coalesce(polymarket_event_id, ''),
            coalesce(polymarket_market_id, ''),
            coalesce(polymarket_condition_id, ''),
            polymarket_alias_status,
            polymarket_alias_confidence,
            polymarket_alias_updated_at,
            polymarket_question,
            polymarket_market_active,
            polymarket_market_closed,
            polymarket_market_archived,
            polymarket_market_end_time,
            polymarket_token_count,
            polymarket_price_snapshot_count,
            latest_polymarket_price_time,
            has_best_bid,
            has_best_ask,
            has_depth_or_liquidity,
            polymarket_max_liquidity,
            polymarket_min_spread,
            sportsbook_consensus_outcome_count,
            latest_sportsbook_received_at,
            betfair_mapped_quote_count,
            betfair_ready_runner_book_count,
            latest_betfair_received_at,
            polymarket_edge_row_count,
            ready_polymarket_edge_row_count,
            readiness_states,
            polymarket_stale_state,
            resolution_status,
            resolution_verified,
            has_sportsbook_consensus,
            has_ready_betfair,
            has_confirmed_polymarket_alias,
            has_polymarket_market_metadata,
            has_polymarket_tokens,
            has_best_bid_and_ask,
            gross_comparison_possible,
            net_comparison_possible,
            gap_reason,
            next_manual_action,
            comparison_ready
        FROM polymarket_comparison_coverage_audit
        WHERE ($1 = '' OR canonical_event_id = $1::uuid)
          AND ($2 = '' OR polymarket_event_id = $2)
          AND ($3 = '' OR polymarket_market_id = $3)
          AND (
              $4 = ''
              OR canonical_event_id IN (
                  SELECT canonical_event_id
                  FROM sportsbook_betfair_comparison_coverage_audit
                  WHERE betfair_market_id = $4
              )
          )
          AND ($5 = '' OR sport_key = $5)
          AND ($6::timestamptz IS NULL OR commence_time >= $6::timestamptz)
          AND ($7::timestamptz IS NULL OR commence_time < $7::timestamptz)
          AND (
              $8
              OR $9
              OR $1 <> ''
              OR $2 <> ''
              OR $3 <> ''
              OR $4 <> ''
              OR $5 <> ''
              OR $6::timestamptz IS NOT NULL
              OR $7::timestamptz IS NOT NULL
          )
          AND (
              NOT $8
              OR has_confirmed_polymarket_alias
              OR has_sportsbook_consensus
              OR has_ready_betfair
              OR $1 <> ''
              OR $2 <> ''
              OR $3 <> ''
              OR $4 <> ''
              OR $5 <> ''
              OR $9
              OR $6::timestamptz IS NOT NULL
              OR $7::timestamptz IS NOT NULL
          )
        ORDER BY
            CASE WHEN $9 THEN 0 ELSE 1 END,
            CASE WHEN $9 THEN commence_time ELSE NULL END ASC NULLS LAST,
            coalesce(latest_polymarket_price_time, latest_sportsbook_received_at, latest_betfair_received_at, commence_time) DESC NULLS LAST,
            commence_time DESC,
            canonical_event_id,
            polymarket_market_id NULLS LAST
        LIMIT $10
    `, opts.CanonicalEventID, opts.PolymarketEventID, opts.PolymarketMarketID, opts.BetfairMarketID, opts.SportKey, nullableTime(opts.CommenceAfter), nullableTime(opts.CommenceBefore), opts.Recent, opts.Candidates, opts.Limit)
	if err != nil {
		return nil, fmt.Errorf("query polymarket comparison readiness: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := []model.PolymarketComparisonReadinessRow{}
	for rows.Next() {
		item, err := scanPolymarketComparisonReadinessRow(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate polymarket comparison readiness: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit polymarket comparison readiness read-only transaction: %w", err)
	}
	return out, nil
}

func normalizePolymarketComparisonReadinessOptions(opts model.PolymarketComparisonReadinessOptions) model.PolymarketComparisonReadinessOptions {
	opts.CanonicalEventID = strings.TrimSpace(opts.CanonicalEventID)
	opts.PolymarketEventID = strings.TrimSpace(opts.PolymarketEventID)
	opts.PolymarketMarketID = strings.TrimSpace(opts.PolymarketMarketID)
	opts.BetfairMarketID = strings.TrimSpace(opts.BetfairMarketID)
	opts.SportKey = strings.TrimSpace(opts.SportKey)
	if opts.Limit <= 0 {
		opts.Limit = defaultCoverageAuditLimit
	}
	if opts.Limit > maxCoverageAuditLimit {
		opts.Limit = maxCoverageAuditLimit
	}
	return opts
}

func scanPolymarketComparisonReadinessRow(row matchReviewScanner) (model.PolymarketComparisonReadinessRow, error) {
	var item model.PolymarketComparisonReadinessRow
	var aliasID sql.NullInt64
	var aliasStatus, question, readinessStates, staleState, resolutionStatus, gapReason, nextAction sql.NullString
	var aliasConfidence, maxLiquidity, minSpread sql.NullFloat64
	var aliasUpdatedAt, marketEndTime, latestPriceTime, latestSportsbook, latestBetfair sql.NullTime
	var marketActive, marketClosed, marketArchived sql.NullBool
	if err := row.Scan(
		&item.CanonicalEventID,
		&item.SportKey,
		&item.League,
		&item.HomeTeam,
		&item.AwayTeam,
		&item.CommenceTime,
		&aliasID,
		&item.PolymarketEventID,
		&item.PolymarketMarketID,
		&item.PolymarketConditionID,
		&aliasStatus,
		&aliasConfidence,
		&aliasUpdatedAt,
		&question,
		&marketActive,
		&marketClosed,
		&marketArchived,
		&marketEndTime,
		&item.PolymarketTokenCount,
		&item.PolymarketPriceSnapshotCount,
		&latestPriceTime,
		&item.HasBestBid,
		&item.HasBestAsk,
		&item.HasDepthOrLiquidity,
		&maxLiquidity,
		&minSpread,
		&item.SportsbookConsensusOutcomeCount,
		&latestSportsbook,
		&item.BetfairMappedQuoteCount,
		&item.BetfairReadyRunnerBookCount,
		&latestBetfair,
		&item.PolymarketEdgeRowCount,
		&item.ReadyPolymarketEdgeRowCount,
		&readinessStates,
		&staleState,
		&resolutionStatus,
		&item.ResolutionVerified,
		&item.HasSportsbookConsensus,
		&item.HasReadyBetfair,
		&item.HasConfirmedPolymarketAlias,
		&item.HasPolymarketMarketMetadata,
		&item.HasPolymarketTokens,
		&item.HasBestBidAndAsk,
		&item.GrossComparisonPossible,
		&item.NetComparisonPossible,
		&gapReason,
		&nextAction,
		&item.ComparisonReady,
	); err != nil {
		return item, fmt.Errorf("scan polymarket comparison readiness row: %w", err)
	}
	if aliasID.Valid {
		v := aliasID.Int64
		item.PolymarketAliasID = &v
	}
	item.PolymarketAliasStatus = aliasStatus.String
	if aliasConfidence.Valid {
		v := aliasConfidence.Float64
		item.PolymarketAliasConfidence = &v
	}
	item.PolymarketAliasUpdatedAt = nullableTimePtr(aliasUpdatedAt)
	item.PolymarketQuestion = question.String
	if marketActive.Valid {
		v := marketActive.Bool
		item.PolymarketMarketActive = &v
	}
	if marketClosed.Valid {
		v := marketClosed.Bool
		item.PolymarketMarketClosed = &v
	}
	if marketArchived.Valid {
		v := marketArchived.Bool
		item.PolymarketMarketArchived = &v
	}
	item.PolymarketMarketEndTime = nullableTimePtr(marketEndTime)
	item.PolymarketLatestPriceTime = nullableTimePtr(latestPriceTime)
	if maxLiquidity.Valid {
		v := maxLiquidity.Float64
		item.PolymarketMaxLiquidity = &v
	}
	if minSpread.Valid {
		v := minSpread.Float64
		item.PolymarketMinSpread = &v
	}
	item.LatestSportsbookReceivedAt = nullableTimePtr(latestSportsbook)
	item.LatestBetfairReceivedAt = nullableTimePtr(latestBetfair)
	item.ReadinessStates = splitCoverageStateList(readinessStates.String)
	item.PolymarketStaleState = staleState.String
	item.ResolutionStatus = resolutionStatus.String
	item.GapReason = gapReason.String
	item.NextManualAction = nextAction.String
	item.CommenceTime = item.CommenceTime.UTC()
	return item, nil
}
