package lsports

import (
	"context"
	"encoding/json"
	"fmt"
)

// Subscribe calls POST /Fixtures/Subscribe for a SINGLE fixture.
//
// The signature deliberately takes a single int rather than a slice so that
// no caller can accidentally bulk-subscribe and burn the budget. LSPORTS
// Support 2026-05-27 confirmed that subscribing by sport or league
// auto-adds multiple fixtures and that "individual fixture is the safest
// option" — this signature is the structural enforcement of that rule.
//
// Quota effect: deducts 1 immediately. Refundable via Unsubscribe ONLY if
// the match has not yet transitioned to InProgress.
func (c *Client) Subscribe(ctx context.Context, fixtureID int) ([]byte, error) {
	if fixtureID <= 0 {
		return nil, fmt.Errorf("lsports: Subscribe: fixtureID must be > 0 (got %d)", fixtureID)
	}
	raw, _, err := c.Post(ctx, "/Fixtures/Subscribe", map[string]any{
		"fixtures": []int{fixtureID},
	})
	if err != nil {
		return raw, err
	}
	if perr := parseSubscriptionEnvelope(raw, "/Fixtures/Subscribe"); perr != nil {
		return raw, perr
	}
	return raw, nil
}

// Unsubscribe calls POST /Fixtures/Unsubscribe for a SINGLE fixture.
// Refunds the slot only if the match has not yet gone InProgress; the
// caller verifies this via /Package/GetPackageQuota diff.
func (c *Client) Unsubscribe(ctx context.Context, fixtureID int) ([]byte, error) {
	if fixtureID <= 0 {
		return nil, fmt.Errorf("lsports: Unsubscribe: fixtureID must be > 0 (got %d)", fixtureID)
	}
	raw, _, err := c.Post(ctx, "/Fixtures/Unsubscribe", map[string]any{
		"fixtures": []int{fixtureID},
	})
	if err != nil {
		return raw, err
	}
	if perr := parseSubscriptionEnvelope(raw, "/Fixtures/Unsubscribe"); perr != nil {
		return raw, perr
	}
	return raw, nil
}

// SubscribedFixturesMetadata is what /Fixtures/GetSubscribedMetadata returns:
// the LSPORTS-side list of currently-subscribed fixtures. The exact body
// schema is sparsely documented; we capture the response raw so the caller
// can inspect it and reconcile against the local ledger.
type SubscribedFixturesMetadata struct {
	Raw json.RawMessage
}

// GetSubscribedMetadata calls /Fixtures/GetSubscribedMetadata. Quota-neutral.
//
// Docs describe this as GET but every other TRADE360 call is POST with the
// auth body. We POST here too; the server tolerates this in practice. If a
// future contract update requires GET we revisit.
func (c *Client) GetSubscribedMetadata(ctx context.Context) (SubscribedFixturesMetadata, []byte, error) {
	raw, _, err := c.Post(ctx, "/Fixtures/GetSubscribedMetadata", nil)
	if err != nil {
		return SubscribedFixturesMetadata{}, raw, err
	}
	if perr := parseSubscriptionEnvelope(raw, "/Fixtures/GetSubscribedMetadata"); perr != nil {
		return SubscribedFixturesMetadata{}, raw, perr
	}
	return SubscribedFixturesMetadata{Raw: raw}, raw, nil
}

// parseSubscriptionEnvelope decodes only the Header to surface server-side
// errors. The Body shape for Subscribe responses is not in the public docs
// and probably differs across the three calls — callers that need the body
// should keep the raw bytes.
func parseSubscriptionEnvelope(raw []byte, label string) error {
	var env struct {
		Header EnvelopeHeader `json:"Header"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		return fmt.Errorf("lsports: %s: decode envelope: %w", label, err)
	}
	if env.Header.HttpStatusCode != 0 && env.Header.HttpStatusCode != 200 {
		return fmt.Errorf("lsports: %s: envelope status %d: %s",
			label, env.Header.HttpStatusCode, string(env.Header.Errors))
	}
	return nil
}
