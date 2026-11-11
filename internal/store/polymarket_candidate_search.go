package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/your-org/pol-odds-collector/internal/model"
	"github.com/your-org/pol-odds-collector/internal/normalize"
)

const (
	defaultPolymarketCandidateLimit = 25
	maxPolymarketCandidateLimit     = 100

	// minPolymarketSearchToken is the minimum length of a team-name token used
	// for ILIKE search, to avoid noisy two-letter false positives like "FC".
	minPolymarketSearchToken = 4
)

// SearchPolymarketCandidates returns local Polymarket markets that may
// correspond to the given canonical event. It is a read-only diagnostic; the
// command never writes aliases or canonical events.
//
// The matching is intentionally conservative: it fuzzes only on team-name
// tokens already present in the canonical event's home/away strings, and
// requires that BOTH a home-side token AND an away-side token appear in either
// the polymarket event title/slug or the polymarket market question/slug.
//
// Final alias decisions are an operator decision via cmd/match-review.
func (s *PostgresStore) SearchPolymarketCandidates(ctx context.Context, opts model.PolymarketCandidateSearch) (model.PolymarketCandidateReport, error) {
	canonicalID := strings.TrimSpace(opts.CanonicalEventID)
	if canonicalID == "" {
		return model.PolymarketCandidateReport{}, fmt.Errorf("canonical_event_id is required")
	}
	limit := opts.Limit
	if limit <= 0 {
		limit = defaultPolymarketCandidateLimit
	}
	if limit > maxPolymarketCandidateLimit {
		limit = maxPolymarketCandidateLimit
	}

	canonical, err := s.SelectCanonicalEventByID(ctx, canonicalID)
	if err != nil {
		return model.PolymarketCandidateReport{}, err
	}

	homeTokens := PolymarketSearchTokens(canonical.HomeTeam)
	awayTokens := PolymarketSearchTokens(canonical.AwayTeam)
	report := model.PolymarketCandidateReport{
		CanonicalEvent: canonical,
		HomeTokens:     homeTokens,
		AwayTokens:     awayTokens,
	}

	hasConfirmed, err := s.canonicalEventHasConfirmedPolymarketAlias(ctx, canonical.ID)
	if err != nil {
		return report, err
	}
	report.HasConfirmedAlias = hasConfirmed

	unresolvedIDs, err := s.unresolvedPolymarketReviewIDsForCanonical(ctx, canonical.ID)
	if err != nil {
		return report, err
	}
	report.UnresolvedReviewIDs = unresolvedIDs

	if len(homeTokens) == 0 || len(awayTokens) == 0 {
		// Without distinctive tokens we cannot run a safe fuzzy search; return
		// the empty candidate set with the canonical context so the operator
		// understands why.
		return report, nil
	}

	candidates, err := s.queryPolymarketCandidates(ctx, canonical, homeTokens, awayTokens, limit)
	if err != nil {
		return report, err
	}
	report.Candidates = candidates
	return report, nil
}

// PolymarketSearchTokens extracts ILIKE-safe team-name tokens from a canonical
// team string. It returns the lowercase distinctive tokens (last word, plus
// any non-trivial leading words). Tokens shorter than minPolymarketSearchToken
// are dropped. Exported so the CLI can echo the search inputs in JSON output.
func PolymarketSearchTokens(team string) []string {
	team = strings.TrimSpace(team)
	if team == "" {
		return nil
	}
	normalized := normalize.EventTeamNorm(team)
	if normalized == "" {
		return nil
	}
	parts := strings.Fields(normalized)
	if len(parts) == 0 {
		return nil
	}
	seen := map[string]struct{}{}
	out := []string{}
	addToken := func(t string) {
		t = strings.TrimSpace(t)
		if len(t) < minPolymarketSearchToken {
			return
		}
		if _, ok := seen[t]; ok {
			return
		}
		seen[t] = struct{}{}
		out = append(out, t)
	}
	// Prefer the LAST token (mascot/club name is usually the most distinctive,
	// e.g. "cleveland cavaliers" -> "cavaliers", "manchester united" -> "united").
	addToken(parts[len(parts)-1])
	// Add other long tokens for fallback (city/region match).
	for i, part := range parts {
		if i == len(parts)-1 {
			continue
		}
		addToken(part)
	}
	return out
}

func (s *PostgresStore) queryPolymarketCandidates(
	ctx context.Context,
	canonical model.CanonicalEvent,
	homeTokens, awayTokens []string,
	limit int,
) ([]model.PolymarketCandidate, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("begin polymarket candidate read-only transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	args := []any{}
	addArg := func(v any) string {
		args = append(args, v)
		return fmt.Sprintf("$%d", len(args))
	}
	homeAny := buildAnyArrayPlaceholder(homeTokens, addArg)
	awayAny := buildAnyArrayPlaceholder(awayTokens, addArg)
	limitPlaceholder := addArg(limit)

	// Match if BOTH a home-side token and an away-side token appear in the same
	// candidate field (event title/slug OR market question/slug). The text is
	// lowercased once on the database side so we do not re-lowercase every row.
	query := fmt.Sprintf(`
        WITH token_summary AS (
            SELECT condition_id, count(*) AS token_count
            FROM polymarket_tokens
            GROUP BY condition_id
        ),
        latest_prices AS (
            SELECT DISTINCT ON (pps.polymarket_market_id)
                pps.polymarket_market_id,
                pps.received_at,
                COALESCE(pps.best_bid, pps.bid_price) AS best_bid,
                COALESCE(pps.best_ask, pps.ask_price) AS best_ask,
                COALESCE(pps.midpoint, pps.mid_price) AS midpoint
            FROM polymarket_price_snapshots pps
            WHERE pps.polymarket_market_id IS NOT NULL
            ORDER BY pps.polymarket_market_id, pps.received_at DESC, pps.id DESC
        ),
        scored AS (
            SELECT
                pe.polymarket_event_id,
                pe.slug AS event_slug,
                pe.title AS event_title,
                pe.start_time,
                pe.end_time,
                pe.active AS event_active,
                pe.closed AS event_closed,
                pe.archived AS event_archived,
                pm.polymarket_market_id,
                pm.condition_id,
                pm.slug AS market_slug,
                pm.question AS market_question,
                pm.market_type,
                pm.active AS market_active,
                pm.closed AS market_closed,
                pm.archived AS market_archived,
                pm.end_time AS market_end_time,
                COALESCE(ts.token_count, 0) AS token_count,
                lp.received_at AS latest_price_time,
                lp.best_bid,
                lp.best_ask,
                lp.midpoint,
                lower(coalesce(pe.title, '') || ' ' || coalesce(pe.slug, '') || ' ' || coalesce(pm.question, '') || ' ' || coalesce(pm.slug, '')) AS haystack
            FROM polymarket_markets pm
            LEFT JOIN polymarket_events pe
              ON pe.polymarket_event_id = pm.polymarket_event_id
            LEFT JOIN token_summary ts
              ON ts.condition_id = pm.condition_id
            LEFT JOIN latest_prices lp
              ON lp.polymarket_market_id = pm.polymarket_market_id
        )
        SELECT
            polymarket_event_id,
            event_slug,
            event_title,
            start_time,
            end_time,
            event_active,
            event_closed,
            event_archived,
            polymarket_market_id,
            condition_id,
            market_slug,
            market_question,
            market_type,
            market_active,
            market_closed,
            market_archived,
            market_end_time,
            token_count,
            latest_price_time,
            best_bid,
            best_ask,
            midpoint,
            (SELECT t FROM unnest(%s::text[]) t WHERE haystack LIKE '%%' || t || '%%' LIMIT 1) AS matched_home_token,
            (SELECT t FROM unnest(%s::text[]) t WHERE haystack LIKE '%%' || t || '%%' LIMIT 1) AS matched_away_token
        FROM scored
        WHERE
            EXISTS (SELECT 1 FROM unnest(%s::text[]) t WHERE haystack LIKE '%%' || t || '%%')
            AND EXISTS (SELECT 1 FROM unnest(%s::text[]) t WHERE haystack LIKE '%%' || t || '%%')
        ORDER BY
            (event_active IS DISTINCT FROM false) DESC,
            (market_active IS DISTINCT FROM false) DESC,
            (event_closed IS DISTINCT FROM true) DESC,
            (market_closed IS DISTINCT FROM true) DESC,
            COALESCE(market_end_time, end_time) DESC NULLS LAST,
            polymarket_market_id
        LIMIT %s
    `, homeAny, awayAny, homeAny, awayAny, limitPlaceholder)

	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("search polymarket candidates: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := []model.PolymarketCandidate{}
	for rows.Next() {
		candidate, err := scanPolymarketCandidate(rows)
		if err != nil {
			return nil, err
		}
		// Per-row hasUnresolved/hasAmbiguous flag is informational; populate
		// from the per-canonical pre-fetched lists.
		out = append(out, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate polymarket candidates: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit polymarket candidate read-only transaction: %w", err)
	}

	if len(out) > 0 {
		unresolvedSet, err := s.polymarketEventIDsWithReviews(ctx, "no_match")
		if err == nil {
			ambiguousSet, _ := s.polymarketEventIDsWithReviews(ctx, "ambiguous")
			confirmedSet, _ := s.polymarketEventIDsWithConfirmedAlias()
			for i := range out {
				if _, ok := unresolvedSet[out[i].PolymarketEventID]; ok {
					out[i].HasUnresolvedReview = true
				}
				if _, ok := ambiguousSet[out[i].PolymarketEventID]; ok {
					out[i].HasAmbiguousReview = true
				}
				if _, ok := confirmedSet[out[i].PolymarketEventID]; ok {
					out[i].HasConfirmedAlias = true
				}
				switch {
				case out[i].MatchedHomeToken != "" && out[i].MatchedAwayToken != "":
					out[i].MatchedSource = "both_team_tokens"
				case out[i].MatchedHomeToken != "" || out[i].MatchedAwayToken != "":
					out[i].MatchedSource = "single_team_token"
				}
				out[i].HasBestBidAndAsk = out[i].LatestBestBid != nil && out[i].LatestBestAsk != nil
			}
		}
	}
	_ = canonical
	return out, nil
}

func buildAnyArrayPlaceholder(tokens []string, addArg func(any) string) string {
	if len(tokens) == 0 {
		return "ARRAY[]::text[]"
	}
	parts := make([]string, len(tokens))
	for i, t := range tokens {
		parts[i] = addArg(t)
	}
	return "ARRAY[" + strings.Join(parts, ",") + "]"
}

func scanPolymarketCandidate(row matchReviewScanner) (model.PolymarketCandidate, error) {
	var c model.PolymarketCandidate
	var eventID, eventSlug, eventTitle, marketID, conditionID, marketSlug, marketQuestion, marketType sql.NullString
	var matchedHome, matchedAway sql.NullString
	var startTime, endTime, marketEndTime, latestPriceTime sql.NullTime
	var eventActive, eventClosed, eventArchived, marketActive, marketClosed, marketArchived sql.NullBool
	var bestBid, bestAsk, midpoint sql.NullFloat64
	if err := row.Scan(
		&eventID,
		&eventSlug,
		&eventTitle,
		&startTime,
		&endTime,
		&eventActive,
		&eventClosed,
		&eventArchived,
		&marketID,
		&conditionID,
		&marketSlug,
		&marketQuestion,
		&marketType,
		&marketActive,
		&marketClosed,
		&marketArchived,
		&marketEndTime,
		&c.TokenCount,
		&latestPriceTime,
		&bestBid,
		&bestAsk,
		&midpoint,
		&matchedHome,
		&matchedAway,
	); err != nil {
		return c, fmt.Errorf("scan polymarket candidate: %w", err)
	}
	c.PolymarketEventID = eventID.String
	c.PolymarketEventSlug = eventSlug.String
	c.PolymarketEventTitle = eventTitle.String
	c.EventStartTime = nullableTimePtr(startTime)
	c.EventEndTime = nullableTimePtr(endTime)
	c.EventActive = nullBoolPtr(eventActive)
	c.EventClosed = nullBoolPtr(eventClosed)
	c.EventArchived = nullBoolPtr(eventArchived)
	c.PolymarketMarketID = marketID.String
	c.ConditionID = conditionID.String
	c.MarketSlug = marketSlug.String
	c.MarketQuestion = marketQuestion.String
	c.MarketType = marketType.String
	c.MarketActive = nullBoolPtr(marketActive)
	c.MarketClosed = nullBoolPtr(marketClosed)
	c.MarketArchived = nullBoolPtr(marketArchived)
	c.MarketEndTime = nullableTimePtr(marketEndTime)
	c.LatestPriceTime = nullableTimePtr(latestPriceTime)
	c.LatestBestBid = nullFloatPtr(bestBid)
	c.LatestBestAsk = nullFloatPtr(bestAsk)
	c.LatestMidpoint = nullFloatPtr(midpoint)
	c.MatchedHomeToken = matchedHome.String
	c.MatchedAwayToken = matchedAway.String
	return c, nil
}

func nullBoolPtr(v sql.NullBool) *bool {
	if !v.Valid {
		return nil
	}
	out := v.Bool
	return &out
}

func nullFloatPtr(v sql.NullFloat64) *float64 {
	if !v.Valid {
		return nil
	}
	out := v.Float64
	return &out
}

func (s *PostgresStore) canonicalEventHasConfirmedPolymarketAlias(ctx context.Context, canonicalEventID string) (bool, error) {
	var has bool
	err := s.db.QueryRowContext(ctx, `
        SELECT EXISTS (
            SELECT 1
            FROM polymarket_event_aliases
            WHERE canonical_event_id = $1::uuid
              AND lower(match_status) = 'confirmed'
        )
    `, canonicalEventID).Scan(&has)
	if err != nil {
		return false, fmt.Errorf("check confirmed polymarket alias: %w", err)
	}
	return has, nil
}

func (s *PostgresStore) unresolvedPolymarketReviewIDsForCanonical(ctx context.Context, canonicalEventID string) ([]int64, error) {
	// candidate_canonical_event_ids is a uuid[] column on event_match_reviews
	// (see migrations/005_event_matching.sql); using ILIKE here would error with
	// "operator does not exist: uuid[] ~~* text". Use proper array membership
	// (= ANY) and coalesce the array so a NULL value never raises an error.
	rows, err := s.db.QueryContext(ctx, `
        SELECT review_id
        FROM unresolved_polymarket_event_matches
        WHERE $1::uuid = ANY (COALESCE(candidate_canonical_event_ids, ARRAY[]::uuid[]))
        UNION ALL
        SELECT review_id
        FROM ambiguous_polymarket_event_matches
        WHERE $1::uuid = ANY (COALESCE(candidate_canonical_event_ids, ARRAY[]::uuid[]))
    `, canonicalEventID)
	if err != nil {
		return nil, fmt.Errorf("list polymarket reviews for canonical event: %w", err)
	}
	defer func() { _ = rows.Close() }()
	out := []int64{}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

func (s *PostgresStore) polymarketEventIDsWithReviews(ctx context.Context, status string) (map[string]struct{}, error) {
	view := "unresolved_polymarket_event_matches"
	if status == "ambiguous" {
		view = "ambiguous_polymarket_event_matches"
	}
	rows, err := s.db.QueryContext(ctx, `SELECT DISTINCT polymarket_event_id FROM `+view)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := map[string]struct{}{}
	for rows.Next() {
		var id sql.NullString
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		if id.String != "" {
			out[id.String] = struct{}{}
		}
	}
	return out, rows.Err()
}

func (s *PostgresStore) polymarketEventIDsWithConfirmedAlias() (map[string]struct{}, error) {
	rows, err := s.db.Query(`SELECT DISTINCT polymarket_event_id FROM confirmed_polymarket_event_aliases`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := map[string]struct{}{}
	for rows.Next() {
		var id sql.NullString
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		if id.String != "" {
			out[id.String] = struct{}{}
		}
	}
	return out, rows.Err()
}
