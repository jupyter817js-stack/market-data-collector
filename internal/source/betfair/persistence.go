package betfair

import (
	"encoding/json"
	"time"

	"github.com/your-org/pol-odds-collector/internal/model"
)

func (m Message) RawStreamMessage(streamID string, receivedAt time.Time) model.BetfairRawStreamMessage {
	var publishTime *time.Time
	if m.PublishTimeMS != nil {
		ts := time.UnixMilli(*m.PublishTimeMS).UTC()
		publishTime = &ts
	}
	marketID := ""
	if len(m.MarketChanges) == 1 {
		marketID = m.MarketChanges[0].ID
	}
	return model.BetfairRawStreamMessage{
		Source:            model.SourceBetfair,
		StreamID:          streamID,
		Op:                string(m.Op),
		MarketID:          marketID,
		PublishTimeMS:     cloneInt64Ptr(m.PublishTimeMS),
		SourcePublishTime: publishTime,
		ChangeType:        m.ChangeType,
		Clock:             m.Clock,
		InitialClock:      m.InitialClock,
		RawJSON:           copyRaw(m.Raw),
		ReceivedAt:        receivedAt,
	}
}

func (c QuoteCandidate) PersistenceModel(receivedAt time.Time) model.BetfairExchangeQuoteCandidate {
	var reducerUpdatedAt *time.Time
	if !c.ReducerUpdatedAt.IsZero() {
		ts := c.ReducerUpdatedAt.UTC()
		reducerUpdatedAt = &ts
	}
	return model.BetfairExchangeQuoteCandidate{
		Source:              model.SourceBetfair,
		QuoteKey:            c.QuoteKey,
		QuoteHash:           c.QuoteHash,
		MarketID:            c.MarketID,
		SelectionID:         c.SelectionID,
		Handicap:            c.Handicap,
		RunnerName:          c.RunnerName,
		MarketName:          c.MarketName,
		EventID:             c.EventID,
		EventName:           c.EventName,
		MarketFamily:        c.MarketFamily,
		ExchangeSide:        string(c.ExchangeSide),
		PriceDecimal:        c.PriceDecimal,
		AvailableSize:       c.AvailableSize,
		LastTradedPrice:     cloneFloatPtr(c.LastTradedPrice),
		TradedVolume:        cloneFloatPtr(c.TradedVolume),
		MarketStatus:        c.MarketStatus,
		InPlay:              cloneBoolPtr(c.InPlay),
		MarketTime:          c.MarketTime,
		SourcePublishTimeMS: cloneInt64Ptr(c.SourcePublishTimeMS),
		SourcePublishTime:   cloneTimePtr(c.SourcePublishTime),
		ReducerUpdatedAt:    reducerUpdatedAt,
		ReceivedAt:          receivedAt,
		AgeMS:               cloneInt64Ptr(c.AgeMS),
		StaleState:          c.StaleState,
		ConfidenceScore:     c.ConfidenceScore,
		RawMetadata:         quoteCandidateRawMetadata(c),
	}
}

func quoteCandidateRawMetadata(candidate QuoteCandidate) json.RawMessage {
	metadata := map[string]json.RawMessage{}
	if len(candidate.RawMarket) > 0 {
		metadata["raw_market"] = copyRaw(candidate.RawMarket)
	}
	if len(candidate.RawRunner) > 0 {
		metadata["raw_runner"] = copyRaw(candidate.RawRunner)
	}
	if len(metadata) == 0 {
		return nil
	}
	raw, err := json.Marshal(metadata)
	if err != nil {
		return nil
	}
	return raw
}
