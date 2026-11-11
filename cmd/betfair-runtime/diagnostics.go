package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	"github.com/your-org/pol-odds-collector/internal/source/betfair"
)

const (
	runtimeSkipMissingMarketID       = "missing_market_id"
	runtimeSkipMissingSelectionID    = "missing_selection_id"
	runtimeSkipMissingBackPrice      = "missing_back_price"
	runtimeSkipMissingLayPrice       = "missing_lay_price"
	runtimeSkipInvalidPrice          = "invalid_price"
	runtimeSkipInvalidSize           = "invalid_size"
	runtimeSkipMissingRunnerMetadata = "missing_runner_metadata"
	runtimeSkipMarketClosedSuspended = "market_closed_or_suspended"
)

const (
	runtimeDiagnosticWarningNoMarketPayload             = "no_market_payload_received"
	runtimeDiagnosticWarningSubscriptionNoMarketPayload = "subscription_matched_no_market_payload"
)

type RuntimeStreamDiagnostics struct {
	RequestedMarketDataFields           []string                        `json:"requested_market_data_fields,omitempty"`
	RequestedLadderLevels               int                             `json:"requested_ladder_levels"`
	RequestedMarketIDsCount             int                             `json:"requested_market_ids_count"`
	RequestedMarketIDHashes             []string                        `json:"requested_market_id_hashes,omitempty"`
	RequestedMarketFilterHasMarketIDs   bool                            `json:"requested_market_filter_has_market_ids"`
	RequestedMarketFilterMarketIDsCount int                             `json:"requested_market_filter_market_ids_count"`
	RequestedMarketFilterBroadScope     bool                            `json:"requested_market_filter_broad_scope"`
	SubscriptionRequestID               int                             `json:"subscription_request_id,omitempty"`
	ParsedMessages                      int                             `json:"parsed_messages"`
	StatusMessages                      int                             `json:"status_messages"`
	StatusMessagesSeen                  int                             `json:"status_messages_seen"`
	StatusSuccessCount                  int                             `json:"status_success_count"`
	StatusFailureCount                  int                             `json:"status_failure_count"`
	StatusErrorCodes                    []string                        `json:"status_error_codes,omitempty"`
	StatusIDsSeen                       []int                           `json:"status_ids_seen,omitempty"`
	ConnectionClosedCount               int                             `json:"connection_closed_count"`
	ConnectionIDPresent                 bool                            `json:"connection_id_present"`
	AuthenticationStatusCode            string                          `json:"authentication_status_code,omitempty"`
	AuthenticationErrorCode             string                          `json:"authentication_error_code,omitempty"`
	AuthenticationID                    int                             `json:"authentication_id,omitempty"`
	SubscriptionStatusCode              string                          `json:"subscription_status_code,omitempty"`
	SubscriptionErrorCode               string                          `json:"subscription_error_code,omitempty"`
	SubscriptionID                      int                             `json:"subscription_id,omitempty"`
	MarketChangeMessages                int                             `json:"market_change_messages"`
	MCMTotal                            int                             `json:"mcm_total"`
	MCMWithMC                           int                             `json:"mcm_with_mc"`
	MCMWithoutMC                        int                             `json:"mcm_without_mc"`
	MCMServerHeartbeatOrControl         int                             `json:"mcm_server_heartbeat_or_control"`
	MCMWithCTHeartbeat                  int                             `json:"mcm_with_ct_heartbeat"`
	MCMWithCTSubImage                   int                             `json:"mcm_with_ct_sub_image"`
	MCMWithCTResubImage                 int                             `json:"mcm_with_ct_resub_image"`
	MCMWithCTUpdate                     int                             `json:"mcm_with_ct_update"`
	MCMWithUnknownCT                    int                             `json:"mcm_with_unknown_ct"`
	MarketChangeMessagesWithMC          int                             `json:"market_change_messages_with_mc"`
	MarketChangeMessagesWithoutMC       int                             `json:"market_change_messages_without_mc"`
	MarketIDsSeen                       []string                        `json:"market_ids_seen,omitempty"`
	MarketIDsSeenCount                  int                             `json:"market_ids_seen_count"`
	RunnerChangesSeen                   int                             `json:"runner_changes_seen"`
	RunnerChangesWithSelectionID        int                             `json:"runner_changes_with_selection_id"`
	RunnerChangesWithBATB               int                             `json:"runner_changes_with_batb"`
	RunnerChangesWithBATL               int                             `json:"runner_changes_with_batl"`
	RunnerChangesWithATB                int                             `json:"runner_changes_with_atb"`
	RunnerChangesWithATL                int                             `json:"runner_changes_with_atl"`
	RunnerChangesWithLTP                int                             `json:"runner_changes_with_ltp"`
	RunnerChangesWithTradedVolume       int                             `json:"runner_changes_with_traded_volume"`
	CacheApplySuccess                   int                             `json:"cache_apply_success"`
	CacheApplyErrors                    int                             `json:"cache_apply_errors"`
	MarketsInCacheAfterApply            int                             `json:"markets_in_cache_after_apply"`
	QuoteEmitAttempts                   int                             `json:"quote_emit_attempts"`
	QuoteEmitErrors                     int                             `json:"quote_emit_errors"`
	QuoteCandidatesSkipped              int                             `json:"quote_candidates_skipped"`
	QuoteCandidatesSkippedReasonCounts  map[string]int                  `json:"quote_candidates_skipped_reason_counts,omitempty"`
	Warnings                            []string                        `json:"warnings,omitempty"`
	StatusSamples                       []RuntimeStreamDiagnosticSample `json:"status_samples,omitempty"`
	Samples                             []RuntimeStreamDiagnosticSample `json:"samples,omitempty"`
}

type RuntimeStreamDiagnosticSample struct {
	Op                  string   `json:"op"`
	ID                  *int     `json:"id,omitempty"`
	StatusCode          string   `json:"status_code,omitempty"`
	ErrorCode           string   `json:"error_code,omitempty"`
	ConnectionClosed    *bool    `json:"connection_closed,omitempty"`
	ConnectionIDPresent bool     `json:"connection_id_present,omitempty"`
	ChangeType          string   `json:"ct,omitempty"`
	MarketID            string   `json:"market_id,omitempty"`
	HasMarketDefinition bool     `json:"has_market_definition"`
	RunnerCount         int      `json:"runner_count"`
	RunnerSelectionIDs  []int64  `json:"runner_selection_ids,omitempty"`
	LadderFieldsPresent []string `json:"ladder_fields_present,omitempty"`
}

type runtimeStreamDiagnosticCollector struct {
	enabled     bool
	sampleLimit int
	diagnostics RuntimeStreamDiagnostics
	marketIDs   map[string]bool
	statusIDs   map[int]bool
	errorCodes  map[string]bool
}

func newRuntimeStreamDiagnosticCollector(enabled bool, sampleLimit int, requestedFields []string, requestedLadderLevels int) *runtimeStreamDiagnosticCollector {
	if sampleLimit < 0 {
		sampleLimit = 0
	}
	if sampleLimit > maxRuntimeDiagnosticSampleLimit {
		sampleLimit = maxRuntimeDiagnosticSampleLimit
	}
	return &runtimeStreamDiagnosticCollector{
		enabled:     enabled,
		sampleLimit: sampleLimit,
		diagnostics: RuntimeStreamDiagnostics{
			RequestedMarketDataFields:          append([]string(nil), requestedFields...),
			RequestedLadderLevels:              requestedLadderLevels,
			QuoteCandidatesSkippedReasonCounts: map[string]int{},
		},
		marketIDs:  map[string]bool{},
		statusIDs:  map[int]bool{},
		errorCodes: map[string]bool{},
	}
}

func (c *runtimeStreamDiagnosticCollector) observeMessage(msg betfair.Message) {
	if c == nil || !c.enabled {
		return
	}
	c.diagnostics.ParsedMessages++
	if msg.Op == betfair.OperationStatus {
		c.observeStatus(msg)
	}
	if strings.TrimSpace(msg.ConnectionID) != "" {
		c.diagnostics.ConnectionIDPresent = true
	}
	if msg.Op != betfair.OperationMarketChange {
		return
	}

	c.diagnostics.MarketChangeMessages++
	c.diagnostics.MCMTotal++
	c.observeMCMChangeType(msg)
	if len(msg.MarketChanges) == 0 {
		c.diagnostics.MarketChangeMessagesWithoutMC++
		c.diagnostics.MCMWithoutMC++
		c.diagnostics.MCMServerHeartbeatOrControl++
		c.appendSample(runtimeDiagnosticSampleFromMessage(msg))
		return
	}
	c.diagnostics.MarketChangeMessagesWithMC++
	c.diagnostics.MCMWithMC++

	for _, change := range msg.MarketChanges {
		if strings.TrimSpace(change.ID) != "" {
			c.marketIDs[strings.TrimSpace(change.ID)] = true
		}
		c.appendSample(runtimeDiagnosticSampleFromMarketChange(msg, change))
		for _, runner := range change.RunnerChanges {
			c.observeRunnerChange(change, runner)
		}
	}
}

func (c *runtimeStreamDiagnosticCollector) observeStatusPhase(phase string, msg betfair.Message) {
	if c == nil || !c.enabled || msg.Op != betfair.OperationStatus {
		return
	}
	switch strings.ToLower(strings.TrimSpace(phase)) {
	case "authentication":
		c.diagnostics.AuthenticationStatusCode = strings.TrimSpace(msg.StatusCode)
		c.diagnostics.AuthenticationErrorCode = strings.TrimSpace(msg.ErrorCode)
		if msg.ID != nil {
			c.diagnostics.AuthenticationID = *msg.ID
		}
	case "subscription":
		c.diagnostics.SubscriptionStatusCode = strings.TrimSpace(msg.StatusCode)
		c.diagnostics.SubscriptionErrorCode = strings.TrimSpace(msg.ErrorCode)
		if msg.ID != nil {
			c.diagnostics.SubscriptionID = *msg.ID
		}
	}
}

func (c *runtimeStreamDiagnosticCollector) observeSubscription(requestID int, req betfair.MarketSubscriptionRequest) {
	if c == nil || !c.enabled {
		return
	}
	marketIDs := cleanRuntimeMarketIDs(req.MarketIDs)
	c.diagnostics.SubscriptionRequestID = requestID
	c.diagnostics.RequestedMarketIDsCount = len(marketIDs)
	c.diagnostics.RequestedMarketFilterHasMarketIDs = len(marketIDs) > 0
	c.diagnostics.RequestedMarketFilterMarketIDsCount = len(marketIDs)
	c.diagnostics.RequestedMarketFilterBroadScope = len(marketIDs) == 0 || len(cleanRuntimeMarketIDs(req.MarketTypes)) > 0
	c.diagnostics.RequestedMarketIDHashes = make([]string, 0, len(marketIDs))
	for _, marketID := range marketIDs {
		c.diagnostics.RequestedMarketIDHashes = append(c.diagnostics.RequestedMarketIDHashes, runtimeDiagnosticHashMarketID(marketID))
	}
	sort.Strings(c.diagnostics.RequestedMarketIDHashes)
	if len(req.Fields) > 0 {
		c.diagnostics.RequestedMarketDataFields = append([]string(nil), req.Fields...)
	}
	if req.LadderLevels > 0 {
		c.diagnostics.RequestedLadderLevels = req.LadderLevels
	}
}

func (c *runtimeStreamDiagnosticCollector) observeCacheApply(msg betfair.Message, err error) {
	if c == nil || !c.enabled || msg.Op != betfair.OperationMarketChange || len(msg.MarketChanges) == 0 {
		return
	}
	if err != nil {
		c.diagnostics.CacheApplyErrors++
		return
	}
	c.diagnostics.CacheApplySuccess++
}

func (c *runtimeStreamDiagnosticCollector) applyToStatus(status *RuntimeStatus, result betfair.LiveRunResult) {
	if c == nil || !c.enabled || status == nil {
		return
	}
	diagnostics := c.snapshot()
	diagnostics.MarketsInCacheAfterApply = result.MarketsSeen
	diagnostics.QuoteEmitAttempts = result.MarketsSeen
	diagnostics.QuoteEmitErrors = 0
	status.StreamDiagnostics = diagnostics
}

func (c *runtimeStreamDiagnosticCollector) snapshot() *RuntimeStreamDiagnostics {
	diagnostics := c.diagnostics
	diagnostics.MarketIDsSeen = make([]string, 0, len(c.marketIDs))
	for marketID := range c.marketIDs {
		diagnostics.MarketIDsSeen = append(diagnostics.MarketIDsSeen, marketID)
	}
	sort.Strings(diagnostics.MarketIDsSeen)
	diagnostics.MarketIDsSeenCount = len(diagnostics.MarketIDsSeen)
	diagnostics.StatusIDsSeen = make([]int, 0, len(c.statusIDs))
	for id := range c.statusIDs {
		diagnostics.StatusIDsSeen = append(diagnostics.StatusIDsSeen, id)
	}
	sort.Ints(diagnostics.StatusIDsSeen)
	diagnostics.StatusErrorCodes = make([]string, 0, len(c.errorCodes))
	for code := range c.errorCodes {
		diagnostics.StatusErrorCodes = append(diagnostics.StatusErrorCodes, code)
	}
	sort.Strings(diagnostics.StatusErrorCodes)
	if diagnostics.MCMWithoutMC > 0 && diagnostics.MCMWithMC == 0 && diagnostics.RunnerChangesSeen == 0 {
		diagnostics.Warnings = appendUniqueRuntimeString(diagnostics.Warnings, runtimeDiagnosticWarningNoMarketPayload)
	}
	if strings.EqualFold(diagnostics.SubscriptionStatusCode, "SUCCESS") &&
		diagnostics.RequestedMarketFilterMarketIDsCount > 0 &&
		diagnostics.MCMWithCTSubImage > 0 &&
		diagnostics.MCMWithMC == 0 &&
		diagnostics.RunnerChangesSeen == 0 {
		diagnostics.Warnings = appendUniqueRuntimeString(diagnostics.Warnings, runtimeDiagnosticWarningSubscriptionNoMarketPayload)
	}
	if len(diagnostics.QuoteCandidatesSkippedReasonCounts) == 0 {
		diagnostics.QuoteCandidatesSkippedReasonCounts = nil
	}
	return &diagnostics
}

func (c *runtimeStreamDiagnosticCollector) observeStatus(msg betfair.Message) {
	c.diagnostics.StatusMessages++
	c.diagnostics.StatusMessagesSeen++
	if msg.ID != nil {
		c.statusIDs[*msg.ID] = true
	}
	if strings.EqualFold(strings.TrimSpace(msg.StatusCode), "SUCCESS") {
		c.diagnostics.StatusSuccessCount++
	} else {
		c.diagnostics.StatusFailureCount++
	}
	if strings.TrimSpace(msg.ErrorCode) != "" {
		c.errorCodes[strings.TrimSpace(msg.ErrorCode)] = true
	}
	if msg.ConnectionClosed != nil && *msg.ConnectionClosed {
		c.diagnostics.ConnectionClosedCount++
	}
	if strings.TrimSpace(msg.ConnectionID) != "" {
		c.diagnostics.ConnectionIDPresent = true
	}
	c.appendStatusSample(runtimeDiagnosticSampleFromMessage(msg))
}

func (c *runtimeStreamDiagnosticCollector) observeMCMChangeType(msg betfair.Message) {
	switch strings.ToUpper(strings.TrimSpace(msg.ChangeType)) {
	case "HEARTBEAT":
		c.diagnostics.MCMWithCTHeartbeat++
	case "SUB_IMAGE":
		c.diagnostics.MCMWithCTSubImage++
	case "RESUB_IMAGE":
		c.diagnostics.MCMWithCTResubImage++
	case "UPDATE":
		c.diagnostics.MCMWithCTUpdate++
	default:
		c.diagnostics.MCMWithUnknownCT++
	}
}

func (c *runtimeStreamDiagnosticCollector) observeRunnerChange(change betfair.MarketChange, runner betfair.RunnerChange) {
	c.diagnostics.RunnerChangesSeen++
	if runner.SelectionID != 0 {
		c.diagnostics.RunnerChangesWithSelectionID++
	}
	if len(runner.BestAvailableToBack) > 0 {
		c.diagnostics.RunnerChangesWithBATB++
	}
	if len(runner.BestAvailableToLay) > 0 {
		c.diagnostics.RunnerChangesWithBATL++
	}
	if len(runner.AvailableToBack) > 0 {
		c.diagnostics.RunnerChangesWithATB++
	}
	if len(runner.AvailableToLay) > 0 {
		c.diagnostics.RunnerChangesWithATL++
	}
	if runner.LastTradedPrice != nil {
		c.diagnostics.RunnerChangesWithLTP++
	}
	if runner.TradedVolume != nil {
		c.diagnostics.RunnerChangesWithTradedVolume++
	}
	c.countRunnerSkipReasons(change, runner)
}

func (c *runtimeStreamDiagnosticCollector) countRunnerSkipReasons(change betfair.MarketChange, runner betfair.RunnerChange) {
	if strings.TrimSpace(change.ID) == "" {
		c.addSkipReason(runtimeSkipMissingMarketID)
	}
	if runner.SelectionID == 0 {
		c.addSkipReason(runtimeSkipMissingSelectionID)
	}
	if !runtimeRunnerHasBackPrice(runner) {
		c.addSkipReason(runtimeSkipMissingBackPrice)
	}
	if !runtimeRunnerHasLayPrice(runner) {
		c.addSkipReason(runtimeSkipMissingLayPrice)
	}
	if runtimeRunnerHasInvalidPrice(runner) {
		c.addSkipReason(runtimeSkipInvalidPrice)
	}
	if runtimeRunnerHasInvalidSize(runner) {
		c.addSkipReason(runtimeSkipInvalidSize)
	}
	if !runtimeMarketDefinitionHasRunner(change.MarketDefinition, runner.SelectionID) {
		c.addSkipReason(runtimeSkipMissingRunnerMetadata)
	}
	if runtimeMarketClosedOrSuspended(change.MarketDefinition) {
		c.addSkipReason(runtimeSkipMarketClosedSuspended)
	}
}

func (c *runtimeStreamDiagnosticCollector) addSkipReason(reason string) {
	if reason == "" {
		return
	}
	c.diagnostics.QuoteCandidatesSkipped++
	if c.diagnostics.QuoteCandidatesSkippedReasonCounts == nil {
		c.diagnostics.QuoteCandidatesSkippedReasonCounts = map[string]int{}
	}
	c.diagnostics.QuoteCandidatesSkippedReasonCounts[reason]++
}

func (c *runtimeStreamDiagnosticCollector) appendSample(sample RuntimeStreamDiagnosticSample) {
	if c.sampleLimit <= 0 || len(c.diagnostics.Samples) >= c.sampleLimit {
		return
	}
	c.diagnostics.Samples = append(c.diagnostics.Samples, sample)
}

func (c *runtimeStreamDiagnosticCollector) appendStatusSample(sample RuntimeStreamDiagnosticSample) {
	if c.sampleLimit <= 0 || len(c.diagnostics.StatusSamples) >= c.sampleLimit {
		return
	}
	c.diagnostics.StatusSamples = append(c.diagnostics.StatusSamples, sample)
}

func runtimeRunnerHasBackPrice(runner betfair.RunnerChange) bool {
	return runtimeLevelLadderHasQuote(runner.BestAvailableToBack) || runtimePriceLadderHasQuote(runner.AvailableToBack)
}

func runtimeRunnerHasLayPrice(runner betfair.RunnerChange) bool {
	return runtimeLevelLadderHasQuote(runner.BestAvailableToLay) || runtimePriceLadderHasQuote(runner.AvailableToLay)
}

func runtimeLevelLadderHasQuote(values []betfair.LevelLadderChange) bool {
	for _, value := range values {
		if value.Price > 1 && value.Size > 0 {
			return true
		}
	}
	return false
}

func runtimePriceLadderHasQuote(values []betfair.PriceLadderChange) bool {
	for _, value := range values {
		if value.Price > 1 && value.Size > 0 {
			return true
		}
	}
	return false
}

func runtimeRunnerHasInvalidPrice(runner betfair.RunnerChange) bool {
	for _, value := range runner.BestAvailableToBack {
		if value.Price <= 1 {
			return true
		}
	}
	for _, value := range runner.BestAvailableToLay {
		if value.Price <= 1 {
			return true
		}
	}
	for _, value := range runner.AvailableToBack {
		if value.Price <= 1 {
			return true
		}
	}
	for _, value := range runner.AvailableToLay {
		if value.Price <= 1 {
			return true
		}
	}
	return false
}

func runtimeRunnerHasInvalidSize(runner betfair.RunnerChange) bool {
	for _, value := range runner.BestAvailableToBack {
		if value.Size <= 0 {
			return true
		}
	}
	for _, value := range runner.BestAvailableToLay {
		if value.Size <= 0 {
			return true
		}
	}
	for _, value := range runner.AvailableToBack {
		if value.Size <= 0 {
			return true
		}
	}
	for _, value := range runner.AvailableToLay {
		if value.Size <= 0 {
			return true
		}
	}
	return false
}

func runtimeMarketDefinitionHasRunner(definition *betfair.MarketDefinition, selectionID int64) bool {
	if selectionID == 0 {
		return false
	}
	if definition == nil {
		return false
	}
	for _, runner := range definition.Runners {
		if runner.ID == selectionID {
			return true
		}
	}
	return false
}

func runtimeMarketClosedOrSuspended(definition *betfair.MarketDefinition) bool {
	if definition == nil {
		return false
	}
	switch strings.ToUpper(strings.TrimSpace(definition.Status)) {
	case "CLOSED", "SUSPENDED", "INACTIVE", "VOID", "SETTLED":
		return true
	default:
		return false
	}
}

func runtimeDiagnosticSampleFromMessage(msg betfair.Message) RuntimeStreamDiagnosticSample {
	return RuntimeStreamDiagnosticSample{
		Op:                  string(msg.Op),
		ID:                  cloneRuntimeIntPtr(msg.ID),
		StatusCode:          strings.TrimSpace(msg.StatusCode),
		ErrorCode:           strings.TrimSpace(msg.ErrorCode),
		ConnectionClosed:    cloneRuntimeBoolPtr(msg.ConnectionClosed),
		ConnectionIDPresent: strings.TrimSpace(msg.ConnectionID) != "",
		ChangeType:          strings.TrimSpace(msg.ChangeType),
	}
}

func runtimeDiagnosticSampleFromMarketChange(msg betfair.Message, change betfair.MarketChange) RuntimeStreamDiagnosticSample {
	fields := map[string]bool{}
	selectionIDs := make([]int64, 0, len(change.RunnerChanges))
	for _, runner := range change.RunnerChanges {
		if runner.SelectionID != 0 {
			selectionIDs = append(selectionIDs, runner.SelectionID)
		}
		if len(runner.BestAvailableToBack) > 0 {
			fields["batb"] = true
		}
		if len(runner.BestAvailableToLay) > 0 {
			fields["batl"] = true
		}
		if len(runner.AvailableToBack) > 0 {
			fields["atb"] = true
		}
		if len(runner.AvailableToLay) > 0 {
			fields["atl"] = true
		}
		if runner.LastTradedPrice != nil {
			fields["ltp"] = true
		}
		if runner.TradedVolume != nil {
			fields["tv"] = true
		}
	}
	sort.Slice(selectionIDs, func(i, j int) bool { return selectionIDs[i] < selectionIDs[j] })
	ladderFields := make([]string, 0, len(fields))
	for field := range fields {
		ladderFields = append(ladderFields, field)
	}
	sort.Strings(ladderFields)
	return RuntimeStreamDiagnosticSample{
		Op:                  string(msg.Op),
		ID:                  cloneRuntimeIntPtr(msg.ID),
		ChangeType:          strings.TrimSpace(msg.ChangeType),
		MarketID:            strings.TrimSpace(change.ID),
		HasMarketDefinition: change.MarketDefinition != nil,
		RunnerCount:         len(change.RunnerChanges),
		RunnerSelectionIDs:  selectionIDs,
		LadderFieldsPresent: ladderFields,
	}
}

func cloneRuntimeIntPtr(value *int) *int {
	if value == nil {
		return nil
	}
	out := *value
	return &out
}

func cloneRuntimeBoolPtr(value *bool) *bool {
	if value == nil {
		return nil
	}
	out := *value
	return &out
}

func runtimeDiagnosticHashMarketID(marketID string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(marketID)))
	return "sha256:" + hex.EncodeToString(sum[:])[:12]
}

func formatRuntimeDiagnosticReasonCounts(counts map[string]int) string {
	if len(counts) == 0 {
		return ""
	}
	keys := make([]string, 0, len(counts))
	for key := range counts {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, fmt.Sprintf("%s:%d", key, counts[key]))
	}
	return strings.Join(parts, ",")
}

func formatRuntimeDiagnosticSamples(samples []RuntimeStreamDiagnosticSample) string {
	if len(samples) == 0 {
		return ""
	}
	parts := make([]string, 0, len(samples))
	for _, sample := range samples {
		item := fmt.Sprintf("op=%s", sample.Op)
		if sample.ID != nil {
			item += fmt.Sprintf(",id=%d", *sample.ID)
		}
		if sample.StatusCode != "" {
			item += ",status_code=" + sample.StatusCode
		}
		if sample.ErrorCode != "" {
			item += ",error_code=" + sample.ErrorCode
		}
		if sample.ConnectionClosed != nil {
			item += fmt.Sprintf(",connection_closed=%t", *sample.ConnectionClosed)
		}
		if sample.ConnectionIDPresent {
			item += ",connection_id_present=true"
		}
		if sample.ChangeType != "" {
			item += ",ct=" + sample.ChangeType
		}
		if sample.MarketID != "" {
			item += fmt.Sprintf(",market_id=%s", sample.MarketID)
		}
		item += fmt.Sprintf(",has_market_definition=%t,runner_count=%d", sample.HasMarketDefinition, sample.RunnerCount)
		if len(sample.RunnerSelectionIDs) > 0 {
			ids := make([]string, 0, len(sample.RunnerSelectionIDs))
			for _, id := range sample.RunnerSelectionIDs {
				ids = append(ids, fmt.Sprintf("%d", id))
			}
			item += ",runner_selection_ids=" + strings.Join(ids, "/")
		}
		if len(sample.LadderFieldsPresent) > 0 {
			item += ",ladder_fields_present=" + strings.Join(sample.LadderFieldsPresent, "/")
		}
		parts = append(parts, item)
	}
	return strings.Join(parts, "|")
}

func formatRuntimeStringList(values []string) string {
	return strings.Join(values, ",")
}

func formatRuntimeIntList(values []int) string {
	if len(values) == 0 {
		return ""
	}
	parts := make([]string, 0, len(values))
	for _, value := range values {
		parts = append(parts, fmt.Sprintf("%d", value))
	}
	return strings.Join(parts, ",")
}

func appendUniqueRuntimeString(values []string, value string) []string {
	value = strings.TrimSpace(value)
	if value == "" {
		return values
	}
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}
