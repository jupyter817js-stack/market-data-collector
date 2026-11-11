package match

import (
	"strings"
	"testing"
	"time"
)

func TestEventToCanonicalExactTeamStartMatch(t *testing.T) {
	start := mustTime("2026-06-01T00:00:00Z")
	query := testEvent("poly-1", "basketball_nba", "NBA", "Boston Celtics", "Los Angeles Lakers", start)
	candidates := []Event{
		testCandidate("canon-1", "basketball_nba", "NBA", "Boston Celtics", "Los Angeles Lakers", start),
	}

	result := MatchEvent(query, candidates, Options{})

	if result.Status != StatusMatched {
		t.Fatalf("Status=%s want %s reason=%q", result.Status, StatusMatched, result.Reason)
	}
	if result.Match == nil || result.Match.Event.ID != "canon-1" {
		t.Fatalf("Match=%+v want canon-1", result.Match)
	}
	if result.ConfidenceScore < 95 {
		t.Fatalf("ConfidenceScore=%v want >=95", result.ConfidenceScore)
	}
}

func TestEventToCanonicalSmallStartTimeDifferenceWithinTolerance(t *testing.T) {
	start := mustTime("2026-06-01T00:00:00Z")
	query := testEvent("poly-1", "basketball_nba", "NBA", "Boston Celtics", "Los Angeles Lakers", start.Add(4*time.Minute))
	candidates := []Event{
		testCandidate("canon-1", "basketball_nba", "NBA", "Boston Celtics", "Los Angeles Lakers", start),
	}

	result := MatchEvent(query, candidates, Options{StartTimeTolerance: 10 * time.Minute})

	if result.Status != StatusMatched {
		t.Fatalf("Status=%s want %s reason=%q", result.Status, StatusMatched, result.Reason)
	}
	if result.Match == nil || result.Match.Event.ID != "canon-1" {
		t.Fatalf("Match=%+v want canon-1", result.Match)
	}
	if result.ConfidenceScore >= 95 || result.ConfidenceScore < 90 {
		t.Fatalf("ConfidenceScore=%v want [90,95)", result.ConfidenceScore)
	}
}

func TestEventToCanonicalReversedHomeAwayIsNotMatched(t *testing.T) {
	start := mustTime("2026-06-01T00:00:00Z")
	query := testEvent("poly-1", "basketball_nba", "NBA", "Boston Celtics", "Los Angeles Lakers", start)
	candidates := []Event{
		testCandidate("canon-1", "basketball_nba", "NBA", "Los Angeles Lakers", "Boston Celtics", start),
	}

	result := MatchEvent(query, candidates, Options{})

	if result.Status != StatusNoMatch {
		t.Fatalf("Status=%s want %s", result.Status, StatusNoMatch)
	}
	if !strings.Contains(result.Reason, "home/away reversed") {
		t.Fatalf("Reason=%q want home/away reversed", result.Reason)
	}
}

func TestEventToCanonicalDifferentParticipantsIsNotMatched(t *testing.T) {
	start := mustTime("2026-06-01T00:00:00Z")
	query := testEvent("poly-1", "basketball_nba", "NBA", "Boston Celtics", "Los Angeles Lakers", start)
	candidates := []Event{
		testCandidate("canon-1", "basketball_nba", "NBA", "Boston Celtics", "Golden State Warriors", start),
	}

	result := MatchEvent(query, candidates, Options{})

	if result.Status != StatusNoMatch {
		t.Fatalf("Status=%s want %s", result.Status, StatusNoMatch)
	}
	if !strings.Contains(result.Reason, "participant mismatch") {
		t.Fatalf("Reason=%q want participant mismatch", result.Reason)
	}
}

func TestEventToCanonicalAmbiguousCandidateSet(t *testing.T) {
	start := mustTime("2026-06-01T00:00:00Z")
	query := testEvent("poly-1", "basketball_nba", "NBA", "Boston Celtics", "Los Angeles Lakers", start)
	candidates := []Event{
		testCandidate("canon-1", "basketball_nba", "NBA", "Boston Celtics", "Los Angeles Lakers", start),
		testCandidate("canon-2", "basketball_nba", "NBA", "Boston Celtics", "Los Angeles Lakers", start.Add(2*time.Minute)),
	}

	result := MatchEvent(query, candidates, Options{StartTimeTolerance: 10 * time.Minute})

	if result.Status != StatusAmbiguous {
		t.Fatalf("Status=%s want %s reason=%q", result.Status, StatusAmbiguous, result.Reason)
	}
	if result.Match != nil {
		t.Fatalf("Match=%+v want nil for ambiguous result", result.Match)
	}
	if len(result.Candidates) != 2 {
		t.Fatalf("len(Candidates)=%d want 2", len(result.Candidates))
	}
}

func testEvent(id, sport, league, home, away string, start time.Time) Event {
	return Event{
		Source:    "polymarket",
		SourceID:  id,
		SportKey:  sport,
		League:    league,
		HomeTeam:  home,
		AwayTeam:  away,
		StartTime: start,
	}
}

func testCandidate(id, sport, league, home, away string, start time.Time) Event {
	e := testEvent(id, sport, league, home, away, start)
	e.ID = id
	e.Source = "canonical"
	return e
}

func mustTime(raw string) time.Time {
	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		panic(err)
	}
	return t
}
