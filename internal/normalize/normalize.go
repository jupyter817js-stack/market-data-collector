package normalize

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/your-org/pol-odds-collector/internal/model"
)

var nonWord = regexp.MustCompile(`[^a-z0-9]+`)

type FreshnessPolicy struct {
	FreshAfter time.Duration
	AgingAfter time.Duration
}

type Quote struct {
	Source           string
	SourceEventID    string
	SportKey         string
	League           string
	HomeTeam         string
	AwayTeam         string
	HomeTeamNorm     string
	AwayTeamNorm     string
	CommenceTime     time.Time
	BookmakerKey     string
	BookmakerTitle   string
	MarketKey        string
	OutcomeName      string
	OutcomeSide      string
	Line             *float64
	PriceDecimal     float64
	SourceLastUpdate *time.Time
	ReceivedAt       time.Time
	InPlay           bool
	AgeMS            *int64
	StaleState       string
	ConfidenceScore  float64
	QuoteKey         string
	QuoteHash        string
}

func EventTeamNorm(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	s = nonWord.ReplaceAllString(s, " ")
	return strings.Join(strings.Fields(s), " ")
}

func EventToQuotes(event model.EventOdds, source string, receivedAt time.Time, policy FreshnessPolicy) []Quote {
	homeNorm := EventTeamNorm(event.HomeTeam)
	awayNorm := EventTeamNorm(event.AwayTeam)
	inPlay := event.CommenceTime.Before(receivedAt)

	quotes := make([]Quote, 0)
	for _, bm := range event.Bookmakers {
		bmLastUpdate := optionalTime(bm.LastUpdate)
		for _, market := range bm.Markets {
			sourceLastUpdate := bmLastUpdate
			if market.LastUpdate != nil && !market.LastUpdate.IsZero() {
				sourceLastUpdate = market.LastUpdate
			}
			staleState, ageMS := StaleState(sourceLastUpdate, receivedAt, policy)
			confidence := ConfidenceScore(staleState, sourceLastUpdate != nil)

			for _, outcome := range market.Outcomes {
				side := OutcomeSide(outcome.Name, event.HomeTeam, event.AwayTeam, market.Key)
				q := Quote{
					Source:           source,
					SourceEventID:    event.ID,
					SportKey:         event.SportKey,
					League:           event.SportTitle,
					HomeTeam:         event.HomeTeam,
					AwayTeam:         event.AwayTeam,
					HomeTeamNorm:     homeNorm,
					AwayTeamNorm:     awayNorm,
					CommenceTime:     event.CommenceTime,
					BookmakerKey:     bm.Key,
					BookmakerTitle:   bm.Title,
					MarketKey:        market.Key,
					OutcomeName:      outcome.Name,
					OutcomeSide:      side,
					Line:             outcome.Point,
					PriceDecimal:     outcome.Price,
					SourceLastUpdate: sourceLastUpdate,
					ReceivedAt:       receivedAt,
					InPlay:           inPlay,
					AgeMS:            ageMS,
					StaleState:       staleState,
					ConfidenceScore:  confidence,
				}
				q.QuoteKey = QuoteKey(q)
				q.QuoteHash = QuoteHash(q)
				quotes = append(quotes, q)
			}
		}
	}
	return quotes
}

func optionalTime(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	u := t.UTC()
	return &u
}

func OutcomeSide(outcomeName, homeTeam, awayTeam, marketKey string) string {
	name := EventTeamNorm(outcomeName)
	switch marketKey {
	case "totals":
		switch name {
		case "over":
			return "over"
		case "under":
			return "under"
		}
	}

	switch name {
	case EventTeamNorm(homeTeam):
		return "home"
	case EventTeamNorm(awayTeam):
		return "away"
	case "draw", "tie":
		return "draw"
	default:
		return "other"
	}
}

func StaleState(sourceLastUpdate *time.Time, receivedAt time.Time, policy FreshnessPolicy) (string, *int64) {
	if sourceLastUpdate == nil {
		return "unknown", nil
	}
	age := receivedAt.Sub(sourceLastUpdate.UTC())
	if age < 0 {
		age = 0
	}
	ageMS := age.Milliseconds()
	switch {
	case age <= policy.FreshAfter:
		return "fresh", &ageMS
	case age <= policy.AgingAfter:
		return "aging", &ageMS
	default:
		return "stale", &ageMS
	}
}

func ConfidenceScore(staleState string, hasTimestamp bool) float64 {
	score := 70.0 // v0 base score for broad aggregator data.
	if !hasTimestamp {
		score -= 15
	}
	switch staleState {
	case "fresh":
		// no-op
	case "aging":
		score -= 10
	case "stale":
		score -= 25
	default:
		score -= 20
	}
	if score < 0 {
		return 0
	}
	return score
}

func QuoteKey(q Quote) string {
	parts := []string{
		q.Source,
		q.SourceEventID,
		q.BookmakerKey,
		q.MarketKey,
		q.OutcomeSide,
		EventTeamNorm(q.OutcomeName),
	}
	return hashString(strings.Join(parts, "|"))
}

func QuoteHash(q Quote) string {
	line := ""
	if q.Line != nil {
		line = fmt.Sprintf("%.4f", *q.Line)
	}
	raw := fmt.Sprintf("%s|%s|%.6f|%s|%t", q.QuoteKey, line, q.PriceDecimal, q.StaleState, q.InPlay)
	return hashString(raw)
}

func hashString(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}
