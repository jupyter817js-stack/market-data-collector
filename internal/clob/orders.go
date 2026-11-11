package clob

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// OrderStatus is Polymarket's CLOB order status string. Documented
// values seen in the SDK / API responses; lowercase canonical form.
type OrderStatus string

const (
	OrderStatusLive      OrderStatus = "live"      // resting on the book
	OrderStatusMatched   OrderStatus = "matched"   // matched against the book (likely filled)
	OrderStatusFilled    OrderStatus = "filled"    // fully filled
	OrderStatusUnmatched OrderStatus = "unmatched" // FAK that found no liquidity
	OrderStatusDelayed   OrderStatus = "delayed"   // server-side queued
	OrderStatusCanceled  OrderStatus = "canceled"  // canceled by user/system
	OrderStatusUnknown   OrderStatus = ""          // unparsed; treat as fresh
)

// IsTerminal reports whether the status will never advance further. The
// fill monitor uses this to stop polling and free the row.
func (s OrderStatus) IsTerminal() bool {
	switch s {
	case OrderStatusFilled, OrderStatusUnmatched, OrderStatusCanceled:
		return true
	default:
		return false
	}
}

// OrderInfo is what GET /data/order/{id} returns. Field set is the union
// of what the SDK documents and what the fill monitor actually consumes.
// Numeric fields stay strings because they are 256-bit-ish on the wire.
type OrderInfo struct {
	ID               string      `json:"id"`
	Status           OrderStatus `json:"status"`
	MakerAddress     string      `json:"maker_address"`
	Owner            string      `json:"owner"`
	TokenID          string      `json:"asset_id"`
	Side             string      `json:"side"`          // "BUY" / "SELL"
	OriginalSize     string      `json:"original_size"` // size_in_token at submit
	SizeMatched      string      `json:"size_matched"`  // filled so far
	Price            string      `json:"price"`         // requested price
	AveragePrice     string      `json:"average_price"` // avg fill price (when partial/full)
	Expiration       string      `json:"expiration"`
	CreatedAt        string      `json:"created_at"` // server-side timestamp
	AssociatedTrades []string    `json:"associated_trades,omitempty"`
}

// GetOrder calls GET /data/order/{orderID} with L2 auth headers and
// returns the parsed status. Treats 404 as a definite "not found" and
// returns a sentinel error so the caller can decide whether to keep
// polling (transient race after POST) or give up.
//
// SECURITY: same L2 HMAC scheme as POST /order — apiKey can only see
// orders it owns. The CLOB will refuse to disclose other operators'
// orders even if the orderID is guessed.
func (c *Client) GetOrder(ctx context.Context, orderID string, creds L2Credentials) (OrderInfo, error) {
	if strings.TrimSpace(orderID) == "" {
		return OrderInfo{}, fmt.Errorf("clob: GetOrder: empty order id")
	}
	path := "/data/order/" + orderID
	apiKey, sign, ts, passphrase, err := L2Headers(creds, time.Now().UTC(), http.MethodGet, path, "")
	if err != nil {
		return OrderInfo{}, fmt.Errorf("clob: GetOrder L2 headers: %w", err)
	}
	headers := map[string]string{
		"POLY_API_KEY":    apiKey,
		"POLY_SIGNATURE":  sign,
		"POLY_TIMESTAMP":  ts,
		"POLY_PASSPHRASE": passphrase,
	}
	body, err := c.do(ctx, http.MethodGet, path, nil, headers)
	if err != nil {
		if apiErr, ok := err.(*APIError); ok && apiErr.Status == http.StatusNotFound {
			return OrderInfo{}, ErrOrderNotFound
		}
		return OrderInfo{}, err
	}
	var info OrderInfo
	if err := json.Unmarshal(body, &info); err != nil {
		return OrderInfo{}, fmt.Errorf("clob: GetOrder decode: %w (body=%s)", err, truncate(string(body), 240))
	}
	if info.ID == "" {
		// Some CLOB deployments wrap the order in {"order":{...}}. Try that.
		var wrapped struct {
			Order OrderInfo `json:"order"`
		}
		if err := json.Unmarshal(body, &wrapped); err == nil && wrapped.Order.ID != "" {
			info = wrapped.Order
		}
	}
	return info, nil
}

// ErrOrderNotFound is returned by GetOrder when the server replies 404.
// Distinguished from transport errors so the fill monitor can decide
// whether to back off (likely a race right after POST) or mark the
// proposed_orders row as 'rejected' if the 404 persists.
var ErrOrderNotFound = fmt.Errorf("clob: order not found")
