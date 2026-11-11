package betfair

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"
)

type MarketCache struct {
	markets map[string]*marketState
	now     func() time.Time
}

type CacheOption func(*MarketCache)

func WithCacheClock(now func() time.Time) CacheOption {
	return func(c *MarketCache) {
		c.now = now
	}
}

func NewMarketCache(options ...CacheOption) *MarketCache {
	cache := &MarketCache{
		markets: map[string]*marketState{},
		now:     func() time.Time { return time.Now().UTC() },
	}
	for _, option := range options {
		option(cache)
	}
	if cache.now == nil {
		cache.now = func() time.Time { return time.Now().UTC() }
	}
	return cache
}

type MarketSnapshot struct {
	MarketID            string
	ChangeType          string
	Status              string
	EventID             string
	EventName           string
	EventTypeID         string
	Name                string
	InPlay              *bool
	MarketTime          string
	SourcePublishTimeMS *int64
	SourcePublishTime   time.Time
	LocalUpdatedAt      time.Time
	Runners             []RunnerSnapshot
	UnknownFields       map[string]json.RawMessage
	Raw                 json.RawMessage
}

type RunnerSnapshot struct {
	SelectionID         int64
	Handicap            float64
	Name                string
	Status              string
	LastTradedPrice     *float64
	TradedVolume        *float64
	BestAvailableToBack []LevelPriceSize
	BestAvailableToLay  []LevelPriceSize
	AvailableToBack     []PriceSize
	AvailableToLay      []PriceSize
	Traded              []PriceSize
	BestBackPrice       *float64
	BestBackSize        *float64
	BestLayPrice        *float64
	BestLaySize         *float64
	UnknownFields       map[string]json.RawMessage
	Raw                 json.RawMessage
}

type BestPricesSnapshot struct {
	MarketID            string
	SelectionID         int64
	Handicap            float64
	BestBackPrice       *float64
	BestBackSize        *float64
	BestLayPrice        *float64
	BestLaySize         *float64
	LastTradedPrice     *float64
	TradedVolume        *float64
	MarketStatus        string
	InPlay              *bool
	SourcePublishTimeMS *int64
	SourcePublishTime   time.Time
	LocalUpdatedAt      time.Time
}

type LevelPriceSize struct {
	Level int
	Price float64
	Size  float64
}

type PriceSize struct {
	Price float64
	Size  float64
}

type marketState struct {
	id                  string
	changeType          string
	status              string
	eventID             string
	eventName           string
	eventTypeID         string
	name                string
	inPlay              *bool
	marketTime          string
	sourcePublishTimeMS *int64
	sourcePublishTime   time.Time
	localUpdatedAt      time.Time
	runners             map[runnerKey]*runnerState
	unknownFields       map[string]json.RawMessage
	raw                 json.RawMessage
}

type runnerState struct {
	selectionID         int64
	handicap            float64
	name                string
	status              string
	lastTradedPrice     *float64
	tradedVolume        *float64
	bestAvailableToBack map[int]PriceSize
	bestAvailableToLay  map[int]PriceSize
	availableToBack     map[float64]PriceSize
	availableToLay      map[float64]PriceSize
	traded              map[float64]PriceSize
	unknownFields       map[string]json.RawMessage
	raw                 json.RawMessage
}

type runnerKey struct {
	selectionID int64
	handicap    float64
}

func (c *MarketCache) ApplyMessage(msg Message) error {
	if c == nil {
		return fmt.Errorf("betfair market cache is nil")
	}
	if msg.Op != OperationMarketChange {
		return nil
	}
	updatedAt := c.now().UTC()
	if updatedAt.IsZero() {
		updatedAt = time.Now().UTC()
	}
	for i, change := range msg.MarketChanges {
		if change.ID == "" {
			return fmt.Errorf("apply betfair market change %d: missing market id", i)
		}
		if err := c.applyMarketChange(msg, change, updatedAt); err != nil {
			return fmt.Errorf("apply betfair market change %q: %w", change.ID, err)
		}
	}
	return nil
}

func (c *MarketCache) SnapshotMarket(marketID string) (MarketSnapshot, bool) {
	if c == nil {
		return MarketSnapshot{}, false
	}
	market, ok := c.markets[marketID]
	if !ok {
		return MarketSnapshot{}, false
	}
	return snapshotMarket(market), true
}

func (c *MarketCache) SnapshotAll() []MarketSnapshot {
	if c == nil {
		return nil
	}
	ids := make([]string, 0, len(c.markets))
	for id := range c.markets {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	snapshots := make([]MarketSnapshot, 0, len(ids))
	for _, id := range ids {
		snapshots = append(snapshots, snapshotMarket(c.markets[id]))
	}
	return snapshots
}

func (c *MarketCache) BestPrices(marketID string, selectionID int64, handicap float64) (BestPricesSnapshot, bool) {
	if c == nil {
		return BestPricesSnapshot{}, false
	}
	market, ok := c.markets[marketID]
	if !ok {
		return BestPricesSnapshot{}, false
	}
	runner, ok := market.runners[runnerKey{selectionID: selectionID, handicap: handicap}]
	if !ok {
		return BestPricesSnapshot{}, false
	}
	bestBackPrice, bestBackSize := bestBack(runner)
	bestLayPrice, bestLaySize := bestLay(runner)
	return BestPricesSnapshot{
		MarketID:            market.id,
		SelectionID:         runner.selectionID,
		Handicap:            runner.handicap,
		BestBackPrice:       bestBackPrice,
		BestBackSize:        bestBackSize,
		BestLayPrice:        bestLayPrice,
		BestLaySize:         bestLaySize,
		LastTradedPrice:     cloneFloatPtr(runner.lastTradedPrice),
		TradedVolume:        cloneFloatPtr(runner.tradedVolume),
		MarketStatus:        market.status,
		InPlay:              cloneBoolPtr(market.inPlay),
		SourcePublishTimeMS: cloneInt64Ptr(market.sourcePublishTimeMS),
		SourcePublishTime:   market.sourcePublishTime,
		LocalUpdatedAt:      market.localUpdatedAt,
	}, true
}

func (c *MarketCache) applyMarketChange(msg Message, change MarketChange, updatedAt time.Time) error {
	fullMarketImage := isImageChange(msg.ChangeType) || boolPtrValue(change.Image)
	if fullMarketImage {
		c.markets[change.ID] = newMarketState(change.ID)
	}
	market := c.markets[change.ID]
	if market == nil {
		market = newMarketState(change.ID)
		c.markets[change.ID] = market
	}
	if msg.ChangeType != "" {
		market.changeType = msg.ChangeType
	}
	if msg.PublishTimeMS != nil {
		market.sourcePublishTimeMS = cloneInt64Ptr(msg.PublishTimeMS)
		market.sourcePublishTime = time.UnixMilli(*msg.PublishTimeMS).UTC()
	}
	market.localUpdatedAt = updatedAt
	market.unknownFields = mergeUnknownFields(market.unknownFields, change.UnknownFields)
	if len(change.Raw) > 0 {
		market.raw = copyRaw(change.Raw)
	}
	if change.MarketDefinition != nil {
		applyMarketDefinition(market, *change.MarketDefinition)
	}
	for i, change := range change.RunnerChanges {
		if err := applyRunnerChange(market, change); err != nil {
			return fmt.Errorf("runner change %d: %w", i, err)
		}
	}
	return nil
}

func newMarketState(marketID string) *marketState {
	return &marketState{
		id:      marketID,
		runners: map[runnerKey]*runnerState{},
	}
}

func applyMarketDefinition(market *marketState, definition MarketDefinition) {
	if definition.Status != "" {
		market.status = definition.Status
	}
	if definition.EventID != "" {
		market.eventID = definition.EventID
	}
	if definition.EventName != "" {
		market.eventName = definition.EventName
	}
	if definition.EventTypeID != "" {
		market.eventTypeID = definition.EventTypeID
	}
	if definition.Name != "" {
		market.name = definition.Name
	}
	if definition.InPlay != nil {
		market.inPlay = cloneBoolPtr(definition.InPlay)
	}
	if definition.MarketTime != "" {
		market.marketTime = definition.MarketTime
	}
	market.unknownFields = mergeUnknownFields(market.unknownFields, definition.UnknownFields)
	for _, definition := range definition.Runners {
		applyRunnerDefinition(market, definition)
	}
}

func applyRunnerDefinition(market *marketState, definition RunnerDefinition) {
	if definition.ID == 0 {
		return
	}
	handicap := 0.0
	if definition.Handicap != nil {
		handicap = *definition.Handicap
	}
	key := runnerKey{selectionID: definition.ID, handicap: handicap}
	runner := market.runners[key]
	if runner == nil {
		runner = newRunnerState(definition.ID, handicap)
		market.runners[key] = runner
	}
	if definition.Name != "" {
		runner.name = definition.Name
	}
	if definition.Status != "" {
		runner.status = definition.Status
	}
	runner.unknownFields = mergeUnknownFields(runner.unknownFields, definition.UnknownFields)
	if len(definition.Raw) > 0 {
		runner.raw = copyRaw(definition.Raw)
	}
}

func applyRunnerChange(market *marketState, change RunnerChange) error {
	if change.SelectionID == 0 {
		return fmt.Errorf("missing runner selection id")
	}
	handicap := 0.0
	if change.Handicap != nil {
		handicap = *change.Handicap
	}
	key := runnerKey{selectionID: change.SelectionID, handicap: handicap}
	if boolPtrValue(change.Image) {
		market.runners[key] = newRunnerState(change.SelectionID, handicap)
	}
	runner := market.runners[key]
	if runner == nil {
		runner = newRunnerState(change.SelectionID, handicap)
		market.runners[key] = runner
	}
	if change.LastTradedPrice != nil {
		runner.lastTradedPrice = cloneFloatPtr(change.LastTradedPrice)
	}
	if change.TradedVolume != nil {
		runner.tradedVolume = cloneFloatPtr(change.TradedVolume)
	}
	runner.bestAvailableToBack = applyLevelLadder(runner.bestAvailableToBack, change.BestAvailableToBack)
	runner.bestAvailableToLay = applyLevelLadder(runner.bestAvailableToLay, change.BestAvailableToLay)
	runner.availableToBack = applyPriceLadder(runner.availableToBack, change.AvailableToBack)
	runner.availableToLay = applyPriceLadder(runner.availableToLay, change.AvailableToLay)
	runner.traded = applyPriceLadder(runner.traded, change.Traded)
	runner.unknownFields = mergeUnknownFields(runner.unknownFields, change.UnknownFields)
	if len(change.Raw) > 0 {
		runner.raw = copyRaw(change.Raw)
	}
	return nil
}

func newRunnerState(selectionID int64, handicap float64) *runnerState {
	return &runnerState{
		selectionID: selectionID,
		handicap:    handicap,
	}
}

func applyLevelLadder(ladder map[int]PriceSize, changes []LevelLadderChange) map[int]PriceSize {
	if len(changes) == 0 {
		return ladder
	}
	if ladder == nil {
		ladder = map[int]PriceSize{}
	}
	for _, change := range changes {
		if change.Size == 0 {
			delete(ladder, change.Level)
			continue
		}
		ladder[change.Level] = PriceSize{Price: change.Price, Size: change.Size}
	}
	return ladder
}

func applyPriceLadder(ladder map[float64]PriceSize, changes []PriceLadderChange) map[float64]PriceSize {
	if len(changes) == 0 {
		return ladder
	}
	if ladder == nil {
		ladder = map[float64]PriceSize{}
	}
	for _, change := range changes {
		if change.Size == 0 {
			delete(ladder, change.Price)
			continue
		}
		ladder[change.Price] = PriceSize{Price: change.Price, Size: change.Size}
	}
	return ladder
}

func snapshotMarket(market *marketState) MarketSnapshot {
	keys := make([]runnerKey, 0, len(market.runners))
	for key := range market.runners {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].selectionID != keys[j].selectionID {
			return keys[i].selectionID < keys[j].selectionID
		}
		return keys[i].handicap < keys[j].handicap
	})
	runners := make([]RunnerSnapshot, 0, len(keys))
	for _, key := range keys {
		runners = append(runners, snapshotRunner(market.runners[key]))
	}
	return MarketSnapshot{
		MarketID:            market.id,
		ChangeType:          market.changeType,
		Status:              market.status,
		EventID:             market.eventID,
		EventName:           market.eventName,
		EventTypeID:         market.eventTypeID,
		Name:                market.name,
		InPlay:              cloneBoolPtr(market.inPlay),
		MarketTime:          market.marketTime,
		SourcePublishTimeMS: cloneInt64Ptr(market.sourcePublishTimeMS),
		SourcePublishTime:   market.sourcePublishTime,
		LocalUpdatedAt:      market.localUpdatedAt,
		Runners:             runners,
		UnknownFields:       cloneRawMap(market.unknownFields),
		Raw:                 copyRaw(market.raw),
	}
}

func snapshotRunner(runner *runnerState) RunnerSnapshot {
	bestBackPrice, bestBackSize := bestBack(runner)
	bestLayPrice, bestLaySize := bestLay(runner)
	return RunnerSnapshot{
		SelectionID:         runner.selectionID,
		Handicap:            runner.handicap,
		Name:                runner.name,
		Status:              runner.status,
		LastTradedPrice:     cloneFloatPtr(runner.lastTradedPrice),
		TradedVolume:        cloneFloatPtr(runner.tradedVolume),
		BestAvailableToBack: snapshotLevelLadder(runner.bestAvailableToBack),
		BestAvailableToLay:  snapshotLevelLadder(runner.bestAvailableToLay),
		AvailableToBack:     snapshotPriceLadder(runner.availableToBack, false),
		AvailableToLay:      snapshotPriceLadder(runner.availableToLay, true),
		Traded:              snapshotPriceLadder(runner.traded, true),
		BestBackPrice:       bestBackPrice,
		BestBackSize:        bestBackSize,
		BestLayPrice:        bestLayPrice,
		BestLaySize:         bestLaySize,
		UnknownFields:       cloneRawMap(runner.unknownFields),
		Raw:                 copyRaw(runner.raw),
	}
}

func snapshotLevelLadder(ladder map[int]PriceSize) []LevelPriceSize {
	if len(ladder) == 0 {
		return nil
	}
	levels := make([]int, 0, len(ladder))
	for level := range ladder {
		levels = append(levels, level)
	}
	sort.Ints(levels)
	out := make([]LevelPriceSize, 0, len(levels))
	for _, level := range levels {
		value := ladder[level]
		out = append(out, LevelPriceSize{Level: level, Price: value.Price, Size: value.Size})
	}
	return out
}

func snapshotPriceLadder(ladder map[float64]PriceSize, ascending bool) []PriceSize {
	if len(ladder) == 0 {
		return nil
	}
	prices := make([]float64, 0, len(ladder))
	for price := range ladder {
		prices = append(prices, price)
	}
	sort.Float64s(prices)
	if !ascending {
		for i, j := 0, len(prices)-1; i < j; i, j = i+1, j-1 {
			prices[i], prices[j] = prices[j], prices[i]
		}
	}
	out := make([]PriceSize, 0, len(prices))
	for _, price := range prices {
		out = append(out, ladder[price])
	}
	return out
}

func bestBack(runner *runnerState) (*float64, *float64) {
	if len(runner.bestAvailableToBack) > 0 {
		level := minLevel(runner.bestAvailableToBack)
		value := runner.bestAvailableToBack[level]
		return floatPtr(value.Price), floatPtr(value.Size)
	}
	if len(runner.availableToBack) > 0 {
		price := maxPrice(runner.availableToBack)
		value := runner.availableToBack[price]
		return floatPtr(value.Price), floatPtr(value.Size)
	}
	return nil, nil
}

func bestLay(runner *runnerState) (*float64, *float64) {
	if len(runner.bestAvailableToLay) > 0 {
		level := minLevel(runner.bestAvailableToLay)
		value := runner.bestAvailableToLay[level]
		return floatPtr(value.Price), floatPtr(value.Size)
	}
	if len(runner.availableToLay) > 0 {
		price := minPrice(runner.availableToLay)
		value := runner.availableToLay[price]
		return floatPtr(value.Price), floatPtr(value.Size)
	}
	return nil, nil
}

func minLevel(ladder map[int]PriceSize) int {
	var min int
	first := true
	for level := range ladder {
		if first || level < min {
			min = level
			first = false
		}
	}
	return min
}

func maxPrice(ladder map[float64]PriceSize) float64 {
	var max float64
	first := true
	for price := range ladder {
		if first || price > max {
			max = price
			first = false
		}
	}
	return max
}

func minPrice(ladder map[float64]PriceSize) float64 {
	var min float64
	first := true
	for price := range ladder {
		if first || price < min {
			min = price
			first = false
		}
	}
	return min
}

func isImageChange(changeType string) bool {
	switch changeType {
	case "SUB_IMAGE", "RESUB_IMAGE":
		return true
	default:
		return false
	}
}

func boolPtrValue(v *bool) bool {
	return v != nil && *v
}

func floatPtr(v float64) *float64 {
	return &v
}

func cloneFloatPtr(v *float64) *float64 {
	if v == nil {
		return nil
	}
	out := *v
	return &out
}

func cloneBoolPtr(v *bool) *bool {
	if v == nil {
		return nil
	}
	out := *v
	return &out
}

func cloneInt64Ptr(v *int64) *int64 {
	if v == nil {
		return nil
	}
	out := *v
	return &out
}

func mergeUnknownFields(dst, src map[string]json.RawMessage) map[string]json.RawMessage {
	if len(src) == 0 {
		return dst
	}
	if dst == nil {
		dst = map[string]json.RawMessage{}
	}
	for key, value := range src {
		dst[key] = copyRaw(value)
	}
	return dst
}

func cloneRawMap(in map[string]json.RawMessage) map[string]json.RawMessage {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]json.RawMessage, len(in))
	for key, value := range in {
		out[key] = copyRaw(value)
	}
	return out
}
