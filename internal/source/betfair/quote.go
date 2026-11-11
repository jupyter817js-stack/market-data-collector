package betfair

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

const SourceBetfair = "betfair"

type ExchangeSide string

const (
	ExchangeSideBack ExchangeSide = "back"
	ExchangeSideLay  ExchangeSide = "lay"
)

const (
	QuoteFreshnessFresh   = "fresh"
	QuoteFreshnessAging   = "aging"
	QuoteFreshnessStale   = "stale"
	QuoteFreshnessUnknown = "unknown"
)

type QuoteEmitOptions struct {
	Now            time.Time
	FreshAfter     time.Duration
	AgingAfter     time.Duration
	BaseConfidence float64
}

type QuoteCandidate struct {
	Source              string
	MarketID            string
	SelectionID         int64
	Handicap            float64
	RunnerName          string
	MarketName          string
	EventID             string
	EventName           string
	MarketFamily        string
	MarketStatus        string
	InPlay              *bool
	MarketTime          string
	SourcePublishTimeMS *int64
	SourcePublishTime   *time.Time
	ReducerUpdatedAt    time.Time
	ExchangeSide        ExchangeSide
	PriceDecimal        float64
	AvailableSize       float64
	LastTradedPrice     *float64
	TradedVolume        *float64
	StaleState          string
	AgeMS               *int64
	ConfidenceScore     float64
	QuoteKey            string
	QuoteHash           string
	RawMarket           json.RawMessage
	RawRunner           json.RawMessage
}

func EmitAllQuoteCandidates(cache *MarketCache, opts QuoteEmitOptions) ([]QuoteCandidate, error) {
	if cache == nil {
		return nil, fmt.Errorf("betfair market cache is nil")
	}
	options, err := normalizeQuoteEmitOptions(opts)
	if err != nil {
		return nil, err
	}

	var candidates []QuoteCandidate
	for _, snapshot := range cache.SnapshotAll() {
		emitted, err := emitQuoteCandidates(snapshot, options)
		if err != nil {
			return nil, err
		}
		candidates = append(candidates, emitted...)
	}
	sortQuoteCandidates(candidates)
	return candidates, nil
}

func EmitQuoteCandidates(snapshot MarketSnapshot, opts QuoteEmitOptions) ([]QuoteCandidate, error) {
	options, err := normalizeQuoteEmitOptions(opts)
	if err != nil {
		return nil, err
	}
	return emitQuoteCandidates(snapshot, options)
}

func QuoteCandidateKey(candidate QuoteCandidate) string {
	raw := strings.Join([]string{
		SourceBetfair,
		candidate.MarketFamily,
		candidate.MarketID,
		fmt.Sprintf("%d", candidate.SelectionID),
		formatQuoteFloat(candidate.Handicap, 4),
		string(candidate.ExchangeSide),
	}, "|")
	return quoteHashString(raw)
}

func QuoteCandidateHash(candidate QuoteCandidate) string {
	inPlay := "unknown"
	if candidate.InPlay != nil {
		inPlay = fmt.Sprintf("%t", *candidate.InPlay)
	}
	publishTime := ""
	if candidate.SourcePublishTime != nil {
		publishTime = candidate.SourcePublishTime.UTC().Format(time.RFC3339Nano)
	}
	raw := strings.Join([]string{
		candidate.QuoteKey,
		formatQuoteFloat(candidate.PriceDecimal, 6),
		formatQuoteFloat(candidate.AvailableSize, 6),
		strings.ToUpper(strings.TrimSpace(candidate.MarketStatus)),
		inPlay,
		publishTime,
	}, "|")
	return quoteHashString(raw)
}

func InferMarketFamily(marketName string) string {
	name := strings.ToLower(strings.TrimSpace(marketName))
	switch {
	case name == "":
		return ""
	case strings.Contains(name, "match odds"):
		return "match_odds"
	case strings.Contains(name, "moneyline"):
		return "moneyline"
	case strings.Contains(name, "total") || strings.Contains(name, "over/under"):
		return "totals"
	default:
		return ""
	}
}

func emitQuoteCandidates(snapshot MarketSnapshot, opts QuoteEmitOptions) ([]QuoteCandidate, error) {
	if strings.TrimSpace(snapshot.MarketID) == "" {
		return nil, fmt.Errorf("emit betfair quote candidates: market id is required")
	}

	staleState, ageMS := quoteFreshness(snapshot.SourcePublishTime, opts.Now, opts)
	sourcePublishTime := optionalQuoteTime(snapshot.SourcePublishTime)
	base := quoteCandidateBase{
		snapshot:          snapshot,
		marketFamily:      InferMarketFamily(snapshot.Name),
		staleState:        staleState,
		ageMS:             ageMS,
		sourcePublishTime: sourcePublishTime,
	}

	candidates := make([]QuoteCandidate, 0, len(snapshot.Runners)*2)
	for _, runner := range snapshot.Runners {
		if runner.SelectionID == 0 {
			continue
		}
		if runner.BestBackPrice != nil && runner.BestBackSize != nil {
			if candidate, ok := buildQuoteCandidate(base, runner, ExchangeSideBack, *runner.BestBackPrice, *runner.BestBackSize, opts); ok {
				candidates = append(candidates, candidate)
			}
		}
		if runner.BestLayPrice != nil && runner.BestLaySize != nil {
			if candidate, ok := buildQuoteCandidate(base, runner, ExchangeSideLay, *runner.BestLayPrice, *runner.BestLaySize, opts); ok {
				candidates = append(candidates, candidate)
			}
		}
	}
	sortQuoteCandidates(candidates)
	return candidates, nil
}

type quoteCandidateBase struct {
	snapshot          MarketSnapshot
	marketFamily      string
	staleState        string
	ageMS             *int64
	sourcePublishTime *time.Time
}

func buildQuoteCandidate(base quoteCandidateBase, runner RunnerSnapshot, side ExchangeSide, price, size float64, opts QuoteEmitOptions) (QuoteCandidate, bool) {
	if !validExchangeQuote(side, price, size) {
		return QuoteCandidate{}, false
	}
	candidate := QuoteCandidate{
		Source:              SourceBetfair,
		MarketID:            base.snapshot.MarketID,
		SelectionID:         runner.SelectionID,
		Handicap:            runner.Handicap,
		RunnerName:          runner.Name,
		MarketName:          base.snapshot.Name,
		EventID:             base.snapshot.EventID,
		EventName:           base.snapshot.EventName,
		MarketFamily:        base.marketFamily,
		MarketStatus:        base.snapshot.Status,
		InPlay:              cloneBoolPtr(base.snapshot.InPlay),
		MarketTime:          base.snapshot.MarketTime,
		SourcePublishTimeMS: cloneInt64Ptr(base.snapshot.SourcePublishTimeMS),
		SourcePublishTime:   cloneTimePtr(base.sourcePublishTime),
		ReducerUpdatedAt:    base.snapshot.LocalUpdatedAt,
		ExchangeSide:        side,
		PriceDecimal:        price,
		AvailableSize:       size,
		LastTradedPrice:     cloneFloatPtr(runner.LastTradedPrice),
		TradedVolume:        cloneFloatPtr(runner.TradedVolume),
		StaleState:          base.staleState,
		AgeMS:               cloneInt64Ptr(base.ageMS),
		RawMarket:           copyRaw(base.snapshot.Raw),
		RawRunner:           copyRaw(runner.Raw),
	}
	candidate.ConfidenceScore = quoteConfidence(base.snapshot, runner, candidate, opts.BaseConfidence)
	candidate.QuoteKey = QuoteCandidateKey(candidate)
	candidate.QuoteHash = QuoteCandidateHash(candidate)
	return candidate, true
}

func validExchangeQuote(side ExchangeSide, price, size float64) bool {
	if side != ExchangeSideBack && side != ExchangeSideLay {
		return false
	}
	if math.IsNaN(price) || math.IsInf(price, 0) || price <= 1 {
		return false
	}
	if math.IsNaN(size) || math.IsInf(size, 0) || size <= 0 {
		return false
	}
	return true
}

func normalizeQuoteEmitOptions(opts QuoteEmitOptions) (QuoteEmitOptions, error) {
	if opts.FreshAfter <= 0 {
		opts.FreshAfter = 2 * time.Minute
	}
	if opts.AgingAfter <= 0 {
		opts.AgingAfter = 5 * time.Minute
	}
	if opts.AgingAfter < opts.FreshAfter {
		return QuoteEmitOptions{}, fmt.Errorf("betfair quote emit aging threshold must be >= fresh threshold")
	}
	if opts.Now.IsZero() {
		opts.Now = time.Now().UTC()
	} else {
		opts.Now = opts.Now.UTC()
	}
	if opts.BaseConfidence <= 0 {
		opts.BaseConfidence = 80
	}
	if opts.BaseConfidence > 100 {
		opts.BaseConfidence = 100
	}
	return opts, nil
}

func quoteFreshness(sourcePublishTime, now time.Time, opts QuoteEmitOptions) (string, *int64) {
	if sourcePublishTime.IsZero() {
		return QuoteFreshnessUnknown, nil
	}
	age := now.Sub(sourcePublishTime.UTC())
	if age < 0 {
		age = 0
	}
	ageMS := age.Milliseconds()
	switch {
	case age <= opts.FreshAfter:
		return QuoteFreshnessFresh, &ageMS
	case age <= opts.AgingAfter:
		return QuoteFreshnessAging, &ageMS
	default:
		return QuoteFreshnessStale, &ageMS
	}
}

func quoteConfidence(snapshot MarketSnapshot, runner RunnerSnapshot, candidate QuoteCandidate, base float64) float64 {
	score := base
	if candidate.SourcePublishTime == nil {
		score -= 20
	}
	if marketDefinitionMissing(snapshot) {
		score -= 10
	}
	if strings.TrimSpace(runner.Name) == "" {
		score -= 10
	}
	switch candidate.StaleState {
	case QuoteFreshnessFresh:
	case QuoteFreshnessAging:
		score -= 10
	case QuoteFreshnessStale:
		score -= 25
	default:
		score -= 20
	}
	switch normalizedMarketStatus(snapshot.Status) {
	case "", "OPEN", "ACTIVE":
	case "SUSPENDED":
		score -= 25
	case "CLOSED", "INACTIVE", "VOID", "SETTLED":
		score -= 35
	default:
		score -= 15
	}
	if score < 0 {
		return 0
	}
	if score > 100 {
		return 100
	}
	return score
}

func marketDefinitionMissing(snapshot MarketSnapshot) bool {
	return strings.TrimSpace(snapshot.Status) == "" &&
		strings.TrimSpace(snapshot.Name) == "" &&
		strings.TrimSpace(snapshot.EventID) == "" &&
		strings.TrimSpace(snapshot.MarketTime) == ""
}

func normalizedMarketStatus(status string) string {
	return strings.ToUpper(strings.TrimSpace(status))
}

func optionalQuoteTime(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	out := t.UTC()
	return &out
}

func cloneTimePtr(v *time.Time) *time.Time {
	if v == nil {
		return nil
	}
	out := v.UTC()
	return &out
}

func sortQuoteCandidates(candidates []QuoteCandidate) {
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].MarketID != candidates[j].MarketID {
			return candidates[i].MarketID < candidates[j].MarketID
		}
		if candidates[i].SelectionID != candidates[j].SelectionID {
			return candidates[i].SelectionID < candidates[j].SelectionID
		}
		if candidates[i].Handicap != candidates[j].Handicap {
			return candidates[i].Handicap < candidates[j].Handicap
		}
		if candidates[i].ExchangeSide != candidates[j].ExchangeSide {
			return candidates[i].ExchangeSide < candidates[j].ExchangeSide
		}
		return candidates[i].QuoteKey < candidates[j].QuoteKey
	})
}

func formatQuoteFloat(v float64, precision int) string {
	if v == 0 {
		v = 0
	}
	return fmt.Sprintf("%.*f", precision, v)
}

func quoteHashString(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}
