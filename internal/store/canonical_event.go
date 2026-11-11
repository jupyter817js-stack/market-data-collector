package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/your-org/pol-odds-collector/internal/model"
	"github.com/your-org/pol-odds-collector/internal/normalize"
)

const selectCanonicalEventByIdentitySQL = `
        SELECT
            id::text,
            sport_key,
            league,
            home_team,
            away_team,
            home_team_norm,
            away_team_norm,
            commence_time,
            status,
            created_at,
            updated_at
        FROM canonical_events
        WHERE sport_key = $1
          AND home_team_norm = $2
          AND away_team_norm = $3
          AND commence_time = $4
    `

func (s *PostgresStore) CreateReviewedCanonicalEvent(ctx context.Context, input model.CanonicalEventCreateInput) (model.CanonicalEventCreateResult, error) {
	input, err := normalizeCanonicalEventCreateInput(input)
	if err != nil {
		return model.CanonicalEventCreateResult{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.CanonicalEventCreateResult{}, err
	}
	defer func() { _ = tx.Rollback() }()

	homeNorm := normalize.EventTeamNorm(input.HomeTeam)
	awayNorm := normalize.EventTeamNorm(input.AwayTeam)
	inserted, err := scanCanonicalEvent(tx.QueryRowContext(ctx, `
        INSERT INTO canonical_events (
            sport_key,
            league,
            home_team,
            away_team,
            home_team_norm,
            away_team_norm,
            commence_time,
            status,
            updated_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, now())
        ON CONFLICT (sport_key, home_team_norm, away_team_norm, commence_time)
        DO NOTHING
        RETURNING
            id::text,
            sport_key,
            league,
            home_team,
            away_team,
            home_team_norm,
            away_team_norm,
            commence_time,
            status,
            created_at,
            updated_at
    `, input.SportKey, input.League, input.HomeTeam, input.AwayTeam, homeNorm, awayNorm, input.CommenceTime, input.Status))
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return model.CanonicalEventCreateResult{}, fmt.Errorf("insert reviewed canonical event: %w", err)
	}
	if err == nil {
		if err := tx.Commit(); err != nil {
			return model.CanonicalEventCreateResult{}, err
		}
		return model.CanonicalEventCreateResult{Event: inserted, Created: true}, nil
	}

	existing, err := scanCanonicalEvent(tx.QueryRowContext(ctx, selectCanonicalEventByIdentitySQL,
		input.SportKey,
		homeNorm,
		awayNorm,
		input.CommenceTime,
	))
	if err != nil {
		return model.CanonicalEventCreateResult{}, fmt.Errorf("select existing canonical event: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return model.CanonicalEventCreateResult{}, err
	}
	return model.CanonicalEventCreateResult{Event: existing, Created: false}, nil
}

func normalizeCanonicalEventCreateInput(input model.CanonicalEventCreateInput) (model.CanonicalEventCreateInput, error) {
	input.SportKey = strings.TrimSpace(input.SportKey)
	input.League = strings.TrimSpace(input.League)
	input.HomeTeam = strings.TrimSpace(input.HomeTeam)
	input.AwayTeam = strings.TrimSpace(input.AwayTeam)
	input.Status = strings.TrimSpace(input.Status)
	if input.Status == "" {
		input.Status = "scheduled"
	}
	if input.SportKey == "" {
		return input, fmt.Errorf("sport_key is required")
	}
	if input.HomeTeam == "" {
		return input, fmt.Errorf("home_team is required")
	}
	if input.AwayTeam == "" {
		return input, fmt.Errorf("away_team is required")
	}
	if input.HomeTeam == input.AwayTeam {
		return input, fmt.Errorf("home_team and away_team must differ")
	}
	if input.CommenceTime.IsZero() {
		return input, fmt.Errorf("commence_time is required")
	}
	if normalize.EventTeamNorm(input.HomeTeam) == "" || normalize.EventTeamNorm(input.AwayTeam) == "" {
		return input, fmt.Errorf("home_team and away_team must normalize to non-empty values")
	}
	if input.League == "" {
		input.League = input.SportKey
	}
	return input, nil
}

// SelectCanonicalEventByID returns the canonical event row for the given UUID.
// It is a read-only helper used by Phase 12D candidate-search diagnostics.
func (s *PostgresStore) SelectCanonicalEventByID(ctx context.Context, canonicalEventID string) (model.CanonicalEvent, error) {
	canonicalEventID = strings.TrimSpace(canonicalEventID)
	if canonicalEventID == "" {
		return model.CanonicalEvent{}, fmt.Errorf("canonical_event_id is required")
	}
	row := s.db.QueryRowContext(ctx, `
        SELECT
            id::text,
            sport_key,
            league,
            home_team,
            away_team,
            home_team_norm,
            away_team_norm,
            commence_time,
            status,
            created_at,
            updated_at
        FROM canonical_events
        WHERE id = $1::uuid
    `, canonicalEventID)
	event, err := scanCanonicalEvent(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return model.CanonicalEvent{}, fmt.Errorf("canonical event %s not found", canonicalEventID)
		}
		return model.CanonicalEvent{}, fmt.Errorf("select canonical event %s: %w", canonicalEventID, err)
	}
	return event, nil
}

func scanCanonicalEvent(row interface {
	Scan(dest ...any) error
}) (model.CanonicalEvent, error) {
	var event model.CanonicalEvent
	err := row.Scan(
		&event.ID,
		&event.SportKey,
		&event.League,
		&event.HomeTeam,
		&event.AwayTeam,
		&event.HomeTeamNorm,
		&event.AwayTeamNorm,
		&event.CommenceTime,
		&event.Status,
		&event.CreatedAt,
		&event.UpdatedAt,
	)
	return event, err
}
