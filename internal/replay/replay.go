package replay

import (
	"encoding/json"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/your-org/pol-odds-collector/internal/model"
	"github.com/your-org/pol-odds-collector/internal/normalize"
)

const (
	HistoryMatch       = "history_match"
	HistoryHashDiff    = "history_hash_diff"
	MissingFromHistory = "missing_from_history"
	MissingFromLatest  = "missing_from_latest"
	LatestMatch        = "latest_match"
	LatestHashDiff     = "latest_hash_diff"
)

type RawSnapshot struct {
	ID         int64
	Source     string
	RequestURL string
	ReceivedAt time.Time
	StatusCode int
	Payload    json.RawMessage
}

type SnapshotFilter struct {
	ID       int64
	Source   string
	From     *time.Time
	To       *time.Time
	SportKey string
	Limit    int
}

type StoredQuote struct {
	QuoteKey  string
	QuoteHash string
}

type QuoteReport struct {
	QuoteKey          string
	QuoteHash         string
	SourceEventID     string
	BookmakerKey      string
	MarketKey         string
	OutcomeName       string
	OutcomeSide       string
	StoredHistoryHash string
	LatestQuoteHash   string
	HistoryStatus     string
	LatestStatus      string
}

type SnapshotReport struct {
	SnapshotID            int64
	Source                string
	SportKey              string
	RequestURL            string
	StatusCode            int
	ReceivedAt            time.Time
	EventCount            int
	NormalizedQuoteCount  int
	Quotes                []QuoteReport
	StoredHistoryOnly     []StoredQuote
	HistoryMatches        int
	HistoryHashDiffs      int
	MissingFromHistory    int
	StoredHistoryOnlyCnt  int
	LatestMatches         int
	LatestHashDiffs       int
	MissingFromLatest     int
	SkippedBySportFilter  bool
	DecodeOrReplayWarning string
}

func DecodeOddsAPIEvents(payload json.RawMessage) ([]model.EventOdds, error) {
	var events []model.EventOdds
	if err := json.Unmarshal(payload, &events); err != nil {
		return nil, fmt.Errorf("decode odds api snapshot payload: %w", err)
	}
	return events, nil
}

func ReplayOddsAPISnapshot(snapshot RawSnapshot, policy normalize.FreshnessPolicy, sportKeyFilter string, storedHistory map[string]string, latest map[string]string) (SnapshotReport, error) {
	report := SnapshotReport{
		SnapshotID: snapshot.ID,
		Source:     snapshot.Source,
		RequestURL: snapshot.RequestURL,
		StatusCode: snapshot.StatusCode,
		ReceivedAt: snapshot.ReceivedAt.UTC(),
	}

	events, err := DecodeOddsAPIEvents(snapshot.Payload)
	if err != nil {
		report.DecodeOrReplayWarning = err.Error()
		return report, err
	}

	report.SportKey = inferSportKey(snapshot.RequestURL, events)
	events = filterEventsBySport(events, sportKeyFilter)
	if strings.TrimSpace(sportKeyFilter) != "" {
		report.SportKey = strings.TrimSpace(sportKeyFilter)
	}
	if sportKeyFilter != "" && len(events) == 0 {
		report.SkippedBySportFilter = true
		return report, nil
	}
	report.EventCount = len(events)

	quotes := make([]normalize.Quote, 0)
	for _, event := range events {
		eventQuotes := normalize.EventToQuotes(event, snapshot.Source, snapshot.ReceivedAt.UTC(), policy)
		quotes = append(quotes, eventQuotes...)
	}
	sort.SliceStable(quotes, func(i, j int) bool {
		if quotes[i].QuoteKey == quotes[j].QuoteKey {
			return quotes[i].QuoteHash < quotes[j].QuoteHash
		}
		return quotes[i].QuoteKey < quotes[j].QuoteKey
	})

	report.NormalizedQuoteCount = len(quotes)
	seenHistory := map[string]struct{}{}
	for _, quote := range quotes {
		qr := QuoteReport{
			QuoteKey:      quote.QuoteKey,
			QuoteHash:     quote.QuoteHash,
			SourceEventID: quote.SourceEventID,
			BookmakerKey:  quote.BookmakerKey,
			MarketKey:     quote.MarketKey,
			OutcomeName:   quote.OutcomeName,
			OutcomeSide:   quote.OutcomeSide,
		}

		if storedHash, ok := storedHistory[quote.QuoteKey]; ok {
			seenHistory[quote.QuoteKey] = struct{}{}
			qr.StoredHistoryHash = storedHash
			if storedHash == quote.QuoteHash {
				qr.HistoryStatus = HistoryMatch
				report.HistoryMatches++
			} else {
				qr.HistoryStatus = HistoryHashDiff
				report.HistoryHashDiffs++
			}
		} else {
			qr.HistoryStatus = MissingFromHistory
			report.MissingFromHistory++
		}

		if latestHash, ok := latest[quote.QuoteKey]; ok {
			qr.LatestQuoteHash = latestHash
			if latestHash == quote.QuoteHash {
				qr.LatestStatus = LatestMatch
				report.LatestMatches++
			} else {
				qr.LatestStatus = LatestHashDiff
				report.LatestHashDiffs++
			}
		} else {
			qr.LatestStatus = MissingFromLatest
			report.MissingFromLatest++
		}

		report.Quotes = append(report.Quotes, qr)
	}

	for key, hash := range storedHistory {
		if _, ok := seenHistory[key]; ok {
			continue
		}
		report.StoredHistoryOnly = append(report.StoredHistoryOnly, StoredQuote{
			QuoteKey:  key,
			QuoteHash: hash,
		})
	}
	sort.SliceStable(report.StoredHistoryOnly, func(i, j int) bool {
		return report.StoredHistoryOnly[i].QuoteKey < report.StoredHistoryOnly[j].QuoteKey
	})
	report.StoredHistoryOnlyCnt = len(report.StoredHistoryOnly)
	return report, nil
}

func QuoteKeys(report SnapshotReport) []string {
	keys := make([]string, 0, len(report.Quotes))
	for _, quote := range report.Quotes {
		keys = append(keys, quote.QuoteKey)
	}
	return keys
}

func inferSportKey(requestURL string, events []model.EventOdds) string {
	if len(events) > 0 && strings.TrimSpace(events[0].SportKey) != "" {
		return strings.TrimSpace(events[0].SportKey)
	}

	parsed, err := url.Parse(requestURL)
	if err != nil {
		return ""
	}
	parts := strings.Split(strings.Trim(parsed.Path, "/"), "/")
	for i := 0; i+1 < len(parts); i++ {
		if parts[i] == "sports" {
			return parts[i+1]
		}
	}
	return ""
}

func filterEventsBySport(events []model.EventOdds, sportKey string) []model.EventOdds {
	sportKey = strings.TrimSpace(sportKey)
	if sportKey == "" {
		return events
	}
	out := make([]model.EventOdds, 0, len(events))
	for _, event := range events {
		if event.SportKey == sportKey {
			out = append(out, event)
		}
	}
	return out
}
