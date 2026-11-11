package main

import (
	"encoding/json"
	"testing"

	"github.com/your-org/pol-odds-collector/internal/model"
)

// TestExtractLivescoreFacetsAMQPShape — AMQP push variant with Events wrapper.
// This is the shape we observed from a live LSPORTS capture 2026-05-28
// (FixtureId 19033944 Football). Position 1=home, Position 2=away.
func TestExtractLivescoreFacetsAMQPShape(t *testing.T) {
	raw := json.RawMessage(`{
		"Header": {"Type":2},
		"Body": {
			"Events": [{
				"FixtureId": 19033944,
				"Livescore": {
					"Scoreboard": {
						"Status": 3,
						"CurrentPeriod": 10,
						"Results": [
							{"Position":"1","Value":"1"},
							{"Position":"2","Value":"0"}
						]
					}
				}
			}]
		}
	}`)
	var obs model.LSportsInplayObservation
	extractLivescoreFacets(raw, &obs)
	if obs.HomeScore == nil || *obs.HomeScore != 1 {
		t.Fatalf("home score: got %v want 1", obs.HomeScore)
	}
	if obs.AwayScore == nil || *obs.AwayScore != 0 {
		t.Fatalf("away score: got %v want 0", obs.AwayScore)
	}
	if obs.CurrentPeriod == nil || *obs.CurrentPeriod != 10 {
		t.Fatalf("period: got %v want 10", obs.CurrentPeriod)
	}
	if obs.MatchStatus == nil || *obs.MatchStatus != 3 {
		t.Fatalf("status: got %v want 3", obs.MatchStatus)
	}
}

// TestExtractLivescoreFacetsSnapshotArrayShape — Snapshot REST variant where
// Body is a direct array (not wrapped in Events).
func TestExtractLivescoreFacetsSnapshotArrayShape(t *testing.T) {
	raw := json.RawMessage(`{
		"Header": {"Type":36},
		"Body": [{
			"FixtureId": 19031398,
			"Livescore": {
				"Scoreboard": {
					"Status": 2,
					"CurrentPeriod": 1,
					"Results": [
						{"Position":"1","Value":"3"},
						{"Position":"2","Value":"4"}
					]
				}
			}
		}]
	}`)
	var obs model.LSportsInplayObservation
	extractLivescoreFacets(raw, &obs)
	if obs.HomeScore == nil || *obs.HomeScore != 3 {
		t.Fatalf("home score: got %v want 3", obs.HomeScore)
	}
	if obs.AwayScore == nil || *obs.AwayScore != 4 {
		t.Fatalf("away score: got %v want 4", obs.AwayScore)
	}
	if obs.CurrentPeriod == nil || *obs.CurrentPeriod != 1 {
		t.Fatalf("period: got %v want 1", obs.CurrentPeriod)
	}
}

// TestExtractLivescoreFacetsEmptyBody — empty Body array must be a no-op.
// This is the current pre-match state for our subscriptions.
func TestExtractLivescoreFacetsEmptyBody(t *testing.T) {
	raw := json.RawMessage(`{"Header":{"Type":36},"Body":[]}`)
	var obs model.LSportsInplayObservation
	extractLivescoreFacets(raw, &obs)
	if obs.HomeScore != nil || obs.AwayScore != nil || obs.CurrentPeriod != nil {
		t.Fatalf("expected all nil for empty Body; got home=%v away=%v period=%v",
			obs.HomeScore, obs.AwayScore, obs.CurrentPeriod)
	}
}

// TestExtractLivescoreFacetsScoreboardOnlyHomeSide — when only one side
// has a Position result we should still populate that side and leave the
// other nil rather than crash.
func TestExtractLivescoreFacetsPartialResults(t *testing.T) {
	raw := json.RawMessage(`{
		"Body": [{
			"FixtureId": 19031398,
			"Livescore": {
				"Scoreboard": {
					"Status": 2,
					"CurrentPeriod": 1,
					"Results": [
						{"Position":"1","Value":"5"}
					]
				}
			}
		}]
	}`)
	var obs model.LSportsInplayObservation
	extractLivescoreFacets(raw, &obs)
	if obs.HomeScore == nil || *obs.HomeScore != 5 {
		t.Fatalf("home score: got %v want 5", obs.HomeScore)
	}
	if obs.AwayScore != nil {
		t.Fatalf("away score should be nil; got %v", obs.AwayScore)
	}
}

// TestExtractLivescoreFacetsGarbageBodyIsNoOp — Body containing a number
// or string (LSPORTS error response) must be tolerated silently.
func TestExtractLivescoreFacetsGarbageBodyIsNoOp(t *testing.T) {
	for _, raw := range []json.RawMessage{
		json.RawMessage(`{"Header":{"Type":36},"Body":42}`),
		json.RawMessage(`{"Header":{"Type":36},"Body":"error"}`),
		json.RawMessage(`{"Header":{"Type":36},"Body":null}`),
	} {
		var obs model.LSportsInplayObservation
		extractLivescoreFacets(raw, &obs)
		if obs.HomeScore != nil || obs.AwayScore != nil {
			t.Fatalf("expected nil scores for garbage body %s", string(raw))
		}
	}
}
