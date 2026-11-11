package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/your-org/pol-odds-collector/internal/model"
)

type fakeTokenAliasStore struct {
	approveDecision model.PolymarketTokenAliasDecision
	rejectDecision  model.PolymarketTokenAliasDecision
	listOpts        model.PolymarketTokenAliasListOptions
	listRows        []model.PolymarketTokenAlias
	approveErr      error
	rejectErr       error
	listErr         error
	approveCalls    int
	rejectCalls     int
	listCalls       int
}

func (s *fakeTokenAliasStore) ApprovePolymarketTokenAlias(_ context.Context, d model.PolymarketTokenAliasDecision) error {
	s.approveCalls++
	s.approveDecision = d
	return s.approveErr
}

func (s *fakeTokenAliasStore) RejectPolymarketTokenAlias(_ context.Context, d model.PolymarketTokenAliasDecision) error {
	s.rejectCalls++
	s.rejectDecision = d
	return s.rejectErr
}

func (s *fakeTokenAliasStore) ListPolymarketTokenAliases(_ context.Context, opts model.PolymarketTokenAliasListOptions) ([]model.PolymarketTokenAlias, error) {
	s.listCalls++
	s.listOpts = opts
	if s.listErr != nil {
		return nil, s.listErr
	}
	return s.listRows, nil
}

func TestApprovePolymarketTokenForwardsAllRequiredFields(t *testing.T) {
	st := &fakeTokenAliasStore{}
	var out bytes.Buffer
	err := approvePolymarketToken(context.Background(), st, &out, []string{
		"--token-id", "tok-cavs",
		"--canonical-event-id", "5aea2bfb-2b88-4eb4-9bcc-b1a3c4510c33",
		"--canonical-outcome-name", "Cleveland Cavaliers",
		"--canonical-outcome-side", "home",
		"--reviewer", "operator-1",
		"--reason", "Pistons-vs-Cavaliers Polymarket title; Cavaliers token maps to canonical home side",
		"--confidence", "95",
		"--condition-id", "0xcond",
		"--polymarket-market-id", "2152553",
	})
	if err != nil {
		t.Fatalf("approve: %v", err)
	}
	if st.approveCalls != 1 {
		t.Fatalf("ApprovePolymarketTokenAlias calls=%d want 1", st.approveCalls)
	}
	d := st.approveDecision
	if d.TokenID != "tok-cavs" || d.CanonicalEventID != "5aea2bfb-2b88-4eb4-9bcc-b1a3c4510c33" {
		t.Fatalf("decision IDs=%+v", d)
	}
	if d.CanonicalOutcomeName != "Cleveland Cavaliers" || d.CanonicalOutcomeSide != "home" {
		t.Fatalf("outcome=%+v", d)
	}
	if d.Reviewer != "operator-1" || d.Reason == "" {
		t.Fatalf("reviewer/reason=%+v", d)
	}
	if d.PolymarketMarketID != "2152553" || d.ConditionID != "0xcond" {
		t.Fatalf("optional ids=%+v", d)
	}
	if d.ConfidenceScore == nil || *d.ConfidenceScore != 95 {
		t.Fatalf("confidence=%+v", d.ConfidenceScore)
	}
	if !strings.Contains(out.String(), "approved polymarket token_id=tok-cavs") {
		t.Fatalf("stdout=%q", out.String())
	}
}

func TestApprovePolymarketTokenRejectsOverConfidence(t *testing.T) {
	st := &fakeTokenAliasStore{}
	err := approvePolymarketToken(context.Background(), st, &bytes.Buffer{}, []string{
		"--token-id", "tok-x", "--canonical-event-id", "00000000-0000-0000-0000-000000000000",
		"--canonical-outcome-name", "X", "--canonical-outcome-side", "home",
		"--reviewer", "r", "--reason", "r",
		"--confidence", "150",
	})
	if err == nil || !strings.Contains(err.Error(), "confidence must be between") {
		t.Fatalf("err=%v", err)
	}
	if st.approveCalls != 0 {
		t.Fatalf("approve must not call store on bad confidence; calls=%d", st.approveCalls)
	}
}

func TestApprovePolymarketTokenForwardsStoreError(t *testing.T) {
	st := &fakeTokenAliasStore{approveErr: errors.New("conflict: token already confirmed")}
	err := approvePolymarketToken(context.Background(), st, &bytes.Buffer{}, []string{
		"--token-id", "tok-x", "--canonical-event-id", "00000000-0000-0000-0000-000000000000",
		"--canonical-outcome-name", "X", "--canonical-outcome-side", "home",
		"--reviewer", "r", "--reason", "r",
	})
	if err == nil || !strings.Contains(err.Error(), "conflict") {
		t.Fatalf("err=%v want store error wrapped", err)
	}
}

func TestRejectPolymarketTokenForwardsToStore(t *testing.T) {
	st := &fakeTokenAliasStore{}
	err := rejectPolymarketToken(context.Background(), st, &bytes.Buffer{}, []string{
		"--token-id", "tok-bad",
		"--reviewer", "operator-1",
		"--reason", "title order does not match canonical home/away assignment",
	})
	if err != nil {
		t.Fatalf("reject: %v", err)
	}
	if st.rejectCalls != 1 || st.rejectDecision.TokenID != "tok-bad" {
		t.Fatalf("reject decision=%+v calls=%d", st.rejectDecision, st.rejectCalls)
	}
}

func TestListPolymarketTokenAliasesFiltersAndJSON(t *testing.T) {
	confidence := 95.0
	st := &fakeTokenAliasStore{
		listRows: []model.PolymarketTokenAlias{{
			AliasID:               17,
			TokenID:               "tok-cavs",
			CanonicalEventID:      "5aea2bfb-2b88-4eb4-9bcc-b1a3c4510c33",
			SportKey:              "basketball_nba",
			HomeTeam:              "Cleveland Cavaliers",
			AwayTeam:              "Detroit Pistons",
			CommenceTime:          time.Date(2026, 5, 9, 19, 10, 0, 0, time.UTC),
			CanonicalOutcomeName:  "Cleveland Cavaliers",
			CanonicalOutcomeSide:  "home",
			Status:                "confirmed",
			ConfidenceScore:       &confidence,
			Reviewer:              "operator-1",
			Reason:                "Pistons-vs-Cavaliers; Cavaliers token maps to home",
			DecisionSource:        "manual",
			PolymarketOutcomeName: "Cavaliers",
		}},
	}
	var out bytes.Buffer
	err := listPolymarketTokenAliases(context.Background(), st, &out, []string{
		"--canonical-event-id", "5aea2bfb-2b88-4eb4-9bcc-b1a3c4510c33",
		"--status", "confirmed",
		"--output", "json",
	})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if st.listOpts.CanonicalEventID != "5aea2bfb-2b88-4eb4-9bcc-b1a3c4510c33" || strings.ToLower(st.listOpts.Status) != "confirmed" {
		t.Fatalf("list opts=%+v", st.listOpts)
	}
	var rows []model.PolymarketTokenAlias
	if err := json.Unmarshal(out.Bytes(), &rows); err != nil {
		t.Fatalf("decode: %v\n%s", err, out.String())
	}
	if len(rows) != 1 || rows[0].TokenID != "tok-cavs" || rows[0].CanonicalOutcomeSide != "home" {
		t.Fatalf("rows=%+v", rows)
	}
}

func TestListPolymarketTokenAliasesTableContainsCriticalColumns(t *testing.T) {
	st := &fakeTokenAliasStore{
		listRows: []model.PolymarketTokenAlias{{
			AliasID: 1, TokenID: "tok-pist", CanonicalEventID: "evt-1",
			SportKey: "basketball_nba", HomeTeam: "Cleveland Cavaliers", AwayTeam: "Detroit Pistons",
			CanonicalOutcomeName: "Detroit Pistons", CanonicalOutcomeSide: "away",
			Status: "confirmed", Reviewer: "op", Reason: "explicit map",
			PolymarketOutcomeName: "Pistons",
		}},
	}
	var out bytes.Buffer
	if err := listPolymarketTokenAliases(context.Background(), st, &out, []string{"--output", "table"}); err != nil {
		t.Fatalf("list: %v", err)
	}
	output := out.String()
	for _, want := range []string{
		"alias_id", "token_id", "canonical_event_id",
		"outcome_side", "outcome_name", "pm_outcome_name",
		"tok-pist", "Detroit Pistons", "away", "Pistons",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("table output missing %q:\n%s", want, output)
		}
	}
}

func TestListPolymarketTokenAliasesRejectsInvalidOutput(t *testing.T) {
	err := listPolymarketTokenAliases(context.Background(), &fakeTokenAliasStore{}, &bytes.Buffer{}, []string{"--output", "xml"})
	if err == nil || !strings.Contains(err.Error(), "--output must be table or json") {
		t.Fatalf("err=%v", err)
	}
}

func TestTokenAliasUsageIsDocumentedInPrintUsage(t *testing.T) {
	var out bytes.Buffer
	printUsage(&out)
	usage := out.String()
	for _, want := range []string{
		"approve-polymarket-token",
		"reject-polymarket-token",
		"list-polymarket-token-aliases",
		"--canonical-outcome-side",
		"home|away|draw|over|under|other",
		"no auto-mapping",
		"order or",
	} {
		if !strings.Contains(usage, want) {
			t.Fatalf("usage missing %q:\n%s", want, usage)
		}
	}
	// No positive-claim trading terminology in the help text.
	lowered := strings.ToLower(usage)
	for _, forbidden := range []string{
		"emits a trading signal",
		"emits a buy signal",
		"emits a sell signal",
		"places an order",
		"submits an order",
		"automatic execution",
		"recommended trade",
	} {
		if strings.Contains(lowered, forbidden) {
			t.Fatalf("usage contains forbidden phrase %q:\n%s", forbidden, usage)
		}
	}
}

func TestTokenAliasOutputContainsNoTradingTerminology(t *testing.T) {
	st := &fakeTokenAliasStore{
		listRows: []model.PolymarketTokenAlias{{
			TokenID: "tok-x", CanonicalEventID: "evt-1",
			CanonicalOutcomeName: "X", CanonicalOutcomeSide: "home",
			Status: "confirmed", Reviewer: "op", Reason: "explicit map",
		}},
	}
	var out bytes.Buffer
	if err := listPolymarketTokenAliases(context.Background(), st, &out, []string{"--output", "json"}); err != nil {
		t.Fatalf("list: %v", err)
	}
	body := strings.ToLower(out.String())
	for _, forbidden := range []string{
		"emits a trading signal",
		"places an order",
		"submits an order",
		"automatic execution",
		"recommended trade",
		"auto-mapped",
	} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("output contains forbidden phrase %q:\n%s", forbidden, body)
		}
	}
}
