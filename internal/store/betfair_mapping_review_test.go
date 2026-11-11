package store

import (
	"strings"
	"testing"

	"github.com/your-org/pol-odds-collector/internal/model"
)

func TestBetfairMapReviewValidation(t *testing.T) {
	confidence := 1.0
	validEvent := model.BetfairEventAliasDecision{
		BetfairEventID:   "event-1",
		CanonicalEventID: "00000000-0000-0000-0000-000000000000",
		Reviewer:         "analyst",
		ReviewReason:     "verified",
		ConfidenceScore:  &confidence,
	}
	if err := validateBetfairEventApproval(normalizeBetfairEventDecision(validEvent)); err != nil {
		t.Fatalf("valid event approval rejected: %v", err)
	}

	tests := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "missing reviewer",
			err: validateBetfairEventApproval(normalizeBetfairEventDecision(model.BetfairEventAliasDecision{
				BetfairEventID:   "event-1",
				CanonicalEventID: "00000000-0000-0000-0000-000000000000",
				ReviewReason:     "verified",
			})),
			want: "reviewer",
		},
		{
			name: "invalid market family",
			err: validateBetfairMarketApproval(normalizeBetfairMarketDecision(model.BetfairMarketAliasDecision{
				BetfairMarketID:       "1.234",
				BetfairEventID:        "event-1",
				CanonicalEventID:      "00000000-0000-0000-0000-000000000000",
				CanonicalMarketFamily: "moneyline",
				Reviewer:              "analyst",
				ReviewReason:          "verified",
			})),
			want: "canonical market family",
		},
		{
			name: "invalid outcome side",
			err: validateBetfairRunnerApproval(normalizeBetfairRunnerDecision(model.BetfairRunnerAliasDecision{
				BetfairMarketID:      "1.234",
				SelectionID:          101,
				CanonicalOutcomeName: "Home FC",
				CanonicalOutcomeSide: "winner",
				Reviewer:             "analyst",
				ReviewReason:         "verified",
			})),
			want: "canonical outcome side",
		},
		{
			name: "invalid confidence",
			err: validateBetfairEventApproval(normalizeBetfairEventDecision(model.BetfairEventAliasDecision{
				BetfairEventID:   "event-1",
				CanonicalEventID: "00000000-0000-0000-0000-000000000000",
				Reviewer:         "analyst",
				ReviewReason:     "verified",
				ConfidenceScore:  floatPtr(101),
			})),
			want: "confidence",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.err == nil || !strings.Contains(tc.err.Error(), tc.want) {
				t.Fatalf("error=%v want containing %q", tc.err, tc.want)
			}
		})
	}
}

func TestBetfairMapReviewSQLUsesMappingTables(t *testing.T) {
	for _, sql := range []string{insertBetfairEventAliasSQL, insertBetfairMarketAliasSQL, insertBetfairRunnerAliasSQL} {
		if !strings.Contains(sql, "alias_status") || !strings.Contains(sql, "reviewer") || !strings.Contains(sql, "review_reason") {
			t.Fatalf("approval SQL must preserve review metadata:\n%s", sql)
		}
	}
	for _, sql := range []string{insertBetfairEventRejectionSQL, insertBetfairMarketRejectionSQL, insertBetfairRunnerRejectionSQL} {
		if !strings.Contains(sql, "'rejected'") {
			t.Fatalf("rejection SQL must mark alias rejected:\n%s", sql)
		}
		if strings.Contains(sql, "canonical_event_id") || strings.Contains(sql, "canonical_outcome_name") {
			t.Fatalf("rejection SQL must not create confirmed mapping identity:\n%s", sql)
		}
	}
}

func floatPtr(v float64) *float64 {
	return &v
}
