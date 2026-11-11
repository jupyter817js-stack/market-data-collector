package match

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/your-org/pol-odds-collector/internal/normalize"
)

type Status string

const (
	StatusMatched   Status = "matched"
	StatusNoMatch   Status = "no_match"
	StatusAmbiguous Status = "ambiguous"
)

type Event struct {
	ID           string
	Source       string
	SourceID     string
	SportKey     string
	League       string
	Title        string
	HomeTeam     string
	AwayTeam     string
	Participants []string
	StartTime    time.Time
}

type Options struct {
	StartTimeTolerance time.Duration
	MinConfidence      float64
}

type CandidateResult struct {
	Event           Event
	ConfidenceScore float64
	Reason          string
}

type Result struct {
	Status          Status
	ConfidenceScore float64
	Reason          string
	Match           *CandidateResult
	Candidates      []CandidateResult
}

func MatchEvent(query Event, candidates []Event, opts Options) Result {
	opts = opts.withDefaults()
	if query.StartTime.IsZero() {
		return noMatch("query event has no start time")
	}

	matches := make([]CandidateResult, 0, len(candidates))
	rejections := map[string]int{}
	for _, candidate := range candidates {
		result, ok := scoreCandidate(query, candidate, opts)
		if ok {
			matches = append(matches, result)
			continue
		}
		rejections[result.Reason]++
	}

	if len(matches) == 0 {
		return noMatch(mostCommonReason(rejections, "no compatible candidate"))
	}

	sort.SliceStable(matches, func(i, j int) bool {
		if matches[i].ConfidenceScore == matches[j].ConfidenceScore {
			return matches[i].Event.ID < matches[j].Event.ID
		}
		return matches[i].ConfidenceScore > matches[j].ConfidenceScore
	})

	if len(matches) > 1 {
		return Result{
			Status:          StatusAmbiguous,
			ConfidenceScore: matches[0].ConfidenceScore,
			Reason:          fmt.Sprintf("%d compatible candidates", len(matches)),
			Candidates:      matches,
		}
	}

	match := matches[0]
	if match.ConfidenceScore < opts.MinConfidence {
		return noMatch("best candidate below confidence threshold")
	}
	return Result{
		Status:          StatusMatched,
		ConfidenceScore: match.ConfidenceScore,
		Reason:          match.Reason,
		Match:           &match,
		Candidates:      matches,
	}
}

func EventToCanonical(query Event, candidates []Event, opts Options) Result {
	return MatchEvent(query, candidates, opts)
}

func (o Options) withDefaults() Options {
	if o.StartTimeTolerance <= 0 {
		o.StartTimeTolerance = 15 * time.Minute
	}
	if o.MinConfidence <= 0 {
		o.MinConfidence = 85
	}
	return o
}

func scoreCandidate(query, candidate Event, opts Options) (CandidateResult, bool) {
	if candidate.StartTime.IsZero() {
		return rejected(candidate, "candidate has no start time")
	}
	if ok, reason := compatibleSportLeague(query, candidate); !ok {
		return rejected(candidate, reason)
	}

	diff := absDuration(query.StartTime.Sub(candidate.StartTime))
	if diff > opts.StartTimeTolerance {
		return rejected(candidate, "start time outside tolerance")
	}

	participantMode, reason, ok := compatibleParticipants(query, candidate)
	if !ok {
		return rejected(candidate, reason)
	}

	score := 95.0
	if opts.StartTimeTolerance > 0 {
		score -= math.Min(5, 5*float64(diff)/float64(opts.StartTimeTolerance))
	}
	if participantMode == "participants" {
		score -= 3
	}
	if leagueNorm(query.League) == "" || leagueNorm(candidate.League) == "" {
		score -= 2
	}
	if score < 0 {
		score = 0
	}

	return CandidateResult{
		Event:           candidate,
		ConfidenceScore: roundScore(score),
		Reason:          reason,
	}, true
}

func compatibleSportLeague(query, candidate Event) (bool, string) {
	querySport := keyNorm(query.SportKey)
	candidateSport := keyNorm(candidate.SportKey)
	if querySport != "" && candidateSport != "" && querySport != candidateSport {
		return false, "sport mismatch"
	}

	queryLeague := leagueNorm(query.League)
	candidateLeague := leagueNorm(candidate.League)
	if queryLeague != "" && candidateLeague != "" && queryLeague != candidateLeague {
		return false, "league mismatch"
	}
	return true, ""
}

func compatibleParticipants(query, candidate Event) (string, string, bool) {
	queryHasSides := hasHomeAway(query)
	candidateHasSides := hasHomeAway(candidate)
	if queryHasSides && candidateHasSides {
		qHome, qAway := teamNorm(query.HomeTeam), teamNorm(query.AwayTeam)
		cHome, cAway := teamNorm(candidate.HomeTeam), teamNorm(candidate.AwayTeam)
		switch {
		case qHome == cHome && qAway == cAway:
			return "home_away", "home/away participants match", true
		case qHome == cAway && qAway == cHome:
			return "", "home/away reversed", false
		default:
			return "", "participant mismatch", false
		}
	}

	queryParticipants := participantSet(query)
	candidateParticipants := participantSet(candidate)
	if len(queryParticipants) < 2 || len(candidateParticipants) < 2 {
		return "", "insufficient participant data", false
	}
	if sameStringSet(queryParticipants, candidateParticipants) {
		return "participants", "participant set matches without home/away certainty", true
	}
	return "", "participant mismatch", false
}

func noMatch(reason string) Result {
	return Result{
		Status: StatusNoMatch,
		Reason: reason,
	}
}

func rejected(candidate Event, reason string) (CandidateResult, bool) {
	return CandidateResult{Event: candidate, Reason: reason}, false
}

func hasHomeAway(e Event) bool {
	return teamNorm(e.HomeTeam) != "" && teamNorm(e.AwayTeam) != ""
}

func participantSet(e Event) []string {
	raw := e.Participants
	if len(raw) == 0 {
		raw = []string{e.HomeTeam, e.AwayTeam}
	}

	seen := map[string]struct{}{}
	out := make([]string, 0, len(raw))
	for _, participant := range raw {
		n := teamNorm(participant)
		if n == "" {
			continue
		}
		if _, ok := seen[n]; ok {
			continue
		}
		seen[n] = struct{}{}
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

func sameStringSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func mostCommonReason(counts map[string]int, fallback string) string {
	bestReason := fallback
	bestCount := 0
	for reason, count := range counts {
		if count > bestCount || (count == bestCount && reason < bestReason) {
			bestReason = reason
			bestCount = count
		}
	}
	return bestReason
}

func keyNorm(s string) string {
	return strings.ToLower(strings.TrimSpace(s))
}

func leagueNorm(s string) string {
	return normalize.EventTeamNorm(s)
}

func teamNorm(s string) string {
	return normalize.EventTeamNorm(s)
}

func absDuration(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}

func roundScore(score float64) float64 {
	return math.Round(score*100) / 100
}
