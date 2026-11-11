package lsports

import (
	"context"
	"encoding/json"
	"fmt"
)

// Quota mirrors the LSPORTS Package/GetPackageQuota response body.
//
// Field semantics per LSPORTS docs:
//   - CreditLimit: hard cap on InProgress-committed fixtures during this
//     billing period (trial = 100 over 14 days).
//   - UsedCredit: fixtures already deducted (subscribed; not necessarily
//     yet InProgress).
//   - CreditRemaining: CreditLimit - UsedCredit. The number a caller must
//     check before any /Fixtures/Subscribe call.
//
// During the trial, the (C) refund semantic means a Subscribe deducts 1
// immediately and Unsubscribe refunds it only if the match has not yet
// transitioned to InProgress. UsedCredit therefore reflects the optimistic
// charge — the true permanent burn is "fixtures that transitioned to
// InProgress while subscribed", which is tracked in lsports_subscriptions
// (migration 047, separate from this REST response).
//
// JSON tags use PascalCase to match the live TRADE360 response wire format.
type Quota struct {
	CreditLimit     int `json:"CreditLimit"`
	UsedCredit      int `json:"UsedCredit"`
	CreditRemaining int `json:"CreditRemaining"`
}

type quotaEnvelope struct {
	Header EnvelopeHeader `json:"Header"`
	Body   Quota          `json:"Body"`
}

// GetPackageQuota calls POST /Package/GetPackageQuota. This call itself does
// NOT consume quota — it is safe to invoke before every Subscribe.
func (c *Client) GetPackageQuota(ctx context.Context) (Quota, []byte, error) {
	raw, _, err := c.Post(ctx, "/Package/GetPackageQuota", nil)
	if err != nil {
		return Quota{}, raw, err
	}
	var env quotaEnvelope
	if jerr := json.Unmarshal(raw, &env); jerr != nil {
		return Quota{}, raw, fmt.Errorf("lsports: /Package/GetPackageQuota: decode envelope: %w", jerr)
	}
	if env.Header.HttpStatusCode != 0 && env.Header.HttpStatusCode != 200 {
		return Quota{}, raw, fmt.Errorf("lsports: /Package/GetPackageQuota: envelope status %d: %s",
			env.Header.HttpStatusCode, string(env.Header.Errors))
	}
	return env.Body, raw, nil
}
