// Command clob-test-order builds, signs, and (optionally) submits a
// single Polymarket CLOB v2 order to validate the full live-trading
// path end-to-end without going through the signal pipeline.
//
// Intended usage: place a deliberately un-matchable resting BUY at a
// very low price ($0.02 default) so PM accepts the order (returns
// orderID) but no fill happens — you can then cancel immediately.
//
// SAFETY:
//   - Default behaviour does NOT submit. Use -really-submit to POST.
//   - With -really-submit AND -auto-cancel, the cmd DELETEs the order
//     right after POST so you don't leave a resting bid on the book.
//   - Price defaults to 0.02 which on most live markets sits far below
//     best_bid, making accidental fill near-impossible. Override with
//     caution.
//
// USAGE:
//
//	go run ./cmd/clob-test-order \
//	    --token-id <decimal token id from PM Gamma> \
//	    --price 0.02 \
//	    --size-usdc 1 \
//	    --really-submit \
//	    --auto-cancel
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"math/big"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/your-org/pol-odds-collector/internal/clob"
	"github.com/your-org/pol-odds-collector/internal/config"
	"github.com/your-org/pol-odds-collector/internal/executor"
)

func main() {
	var (
		tokenID      = flag.String("token-id", "", "decimal token id of a live Polymarket CLOB market (required)")
		priceF       = flag.Float64("price", 0.02, "limit price (probability units, 0 < p < 1)")
		sizeUSDC     = flag.Float64("size-usdc", 1.0, "order size in USDC notional ($)")
		orderTypeS   = flag.String("order-type", "GTC", "GTC | FAK | FOK | GTD")
		side         = flag.String("side", "BUY", "BUY | SELL")
		negRisk      = flag.Bool("neg-risk", false, "use NegRiskCTFExchangeAddress as verifyingContract")
		credsFile    = flag.String("creds-file", "", "path to L2 creds JSON (default: user config dir)")
		baseURL      = flag.String("base-url", clob.DefaultBaseURL, "CLOB base URL")
		reallySubmit = flag.Bool("really-submit", false, "REQUIRED to POST /order")
		autoCancel   = flag.Bool("auto-cancel", false, "after successful POST, immediately DELETE the order")
		signerAsFunder = flag.Bool("signer-as-funder", false, "experimental: set order.signer = funder/proxy (some PM accounts require this)")
	)
	flag.Parse()

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))
	if _, err := config.Load(); err != nil {
		die("config.Load: " + err.Error())
	}

	if strings.TrimSpace(*tokenID) == "" {
		die("--token-id required")
	}
	if *priceF <= 0 || *priceF >= 1 {
		die(fmt.Sprintf("--price out of range: %v (must be 0 < p < 1)", *priceF))
	}
	if *sizeUSDC <= 0 {
		die("--size-usdc must be > 0")
	}
	pk := strings.TrimSpace(envOr("POLYMARKET_SIGNER_PK", os.Getenv("POLYMARKET_PRIVATE_KEY")))
	if pk == "" {
		die("POLYMARKET_SIGNER_PK env required")
	}
	proxy := strings.TrimSpace(envOr("POLYMARKET_PROXY_ADDRESS", os.Getenv("POLYMARKET_MAKER_ADDRESS")))
	builderCode := strings.TrimSpace(os.Getenv("POLYMARKET_BUILDER_CODE"))
	credsPath := strings.TrimSpace(*credsFile)
	if credsPath == "" {
		credsPath = clob.DefaultCredsPath()
	}

	credsF, err := clob.LoadCreds(credsPath)
	if err != nil {
		die("LoadCreds: " + err.Error())
	}
	creds := credsF.As()
	eoa := credsF.Address

	// Wallet mode resolution.
	sigType := executor.SignatureTypeEOA
	maker := eoa
	signer := eoa
	if proxy != "" {
		sigType = executor.SignatureTypePoly1271
		maker = proxy
		if *signerAsFunder {
			// Experimental: some PM accounts require order.signer = funder
			// (the API key is bound to the proxy, not the EOA). The 1271
			// inner wrapping then uses the proxy as verifyingContract; the
			// signature is still produced with the EOA private key.
			signer = proxy
		}
	}

	// Compute V2 amounts in 6-decimal fixed math.
	makerAmt, takerAmt := computeAmounts(*sizeUSDC, *priceF, strings.ToUpper(*side))
	if makerAmt.Sign() <= 0 || takerAmt.Sign() <= 0 {
		die(fmt.Sprintf("amount underflow: maker=%s taker=%s", makerAmt, takerAmt))
	}

	saltVal, err := jsNumberSafeSalt()
	if err != nil {
		die("salt: " + err.Error())
	}

	// Build OrderSpec V2.
	spec := executor.OrderSpec{
		Maker:         maker,
		Signer:        signer,
		TokenID:       strings.TrimSpace(*tokenID),
		Side:          executor.Side(strings.ToUpper(*side)),
		OrderType:     executor.OrderType(strings.ToUpper(*orderTypeS)),
		MakerAmount:   makerAmt,
		TakerAmount:   takerAmt,
		Price:         *priceF,
		SizeUSDC:      *sizeUSDC,
		Salt:          saltVal,
		SignatureType: sigType,
		Timestamp:     time.Now().UTC().UnixMilli(),
		Metadata:      "0x" + strings.Repeat("0", 64),
		Builder:       builderCode,
	}

	domain := clob.DefaultDomain()
	if *negRisk {
		domain.VerifyingContract = clob.NegRiskCTFExchangeAddress
	}

	slog.Info("test-order plan",
		"maker", maker, "signer", eoa,
		"sig_type", sigType,
		"side", spec.Side, "price", spec.Price, "size_usdc", spec.SizeUSDC,
		"maker_amt", makerAmt.String(), "taker_amt", takerAmt.String(),
		"salt", saltVal.String(),
		"timestamp_ms", spec.Timestamp,
		"builder_set", builderCode != "",
		"order_type", spec.OrderType,
		"verifying_contract", domain.VerifyingContract.Hex(),
	)

	// Sign.
	signed, err := clob.SignOrder(spec, domain, pk)
	if err != nil {
		die("SignOrder: " + err.Error())
	}
	slog.Info("signed",
		"order_hash", signed.OrderHash.Hex(),
		"sig_len_chars", len(signed.Signature),
		"signer_addr", signed.SignerAddr.Hex(),
	)

	if !*reallySubmit {
		// Dry-run: print the body bytes that WOULD be POSTed.
		body, err := clob.BodyJSON(signed, creds.APIKey, spec.OrderType)
		if err != nil {
			die("BodyJSON: " + err.Error())
		}
		fmt.Println()
		fmt.Println("=== Dry-run — POST body that would be sent ===")
		fmt.Println(string(body))
		fmt.Println()
		fmt.Println("Pass -really-submit to actually POST /order.")
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	c := clob.NewClient(*baseURL, 15*time.Second)
	resp, err := c.SubmitOrder(ctx, signed, creds, spec.OrderType)
	if err != nil {
		fmt.Println()
		fmt.Println("❌ POST /order FAILED")
		fmt.Println(err.Error())
		os.Exit(1)
	}
	respJSON, _ := json.MarshalIndent(resp, "", "  ")
	fmt.Println()
	fmt.Println("✅ POST /order OK")
	fmt.Println(string(respJSON))

	if *autoCancel && resp.OrderID != "" {
		slog.Info("auto-cancel requested — sending DELETE /order", "order_id", resp.OrderID)
		if err := cancelOrder(ctx, c, creds, eoa, resp.OrderID); err != nil {
			slog.Error("cancel failed", "err", err)
			return
		}
		slog.Info("cancel OK", "order_id", resp.OrderID)
	}
}

// computeAmounts returns (maker_amount, taker_amount) in 6-decimal
// micro-units for the given size+price. BUY: maker=USDC, taker=shares.
// SELL: maker=shares, taker=USDC.
func computeAmounts(sizeUSDC, price float64, side string) (maker, taker *big.Int) {
	const oneUnit = 1_000_000
	makerUSDC := big.NewInt(int64(sizeUSDC * oneUnit))
	// shares = size / price; integer in micro-units.
	priceMicro := big.NewInt(int64(price * oneUnit))
	if priceMicro.Sign() == 0 {
		return new(big.Int), new(big.Int)
	}
	sharesNum := new(big.Int).Mul(makerUSDC, big.NewInt(oneUnit))
	sharesMicro := new(big.Int).Quo(sharesNum, priceMicro)
	switch side {
	case "BUY":
		return makerUSDC, sharesMicro
	case "SELL":
		return sharesMicro, makerUSDC
	default:
		return new(big.Int), new(big.Int)
	}
}

// jsNumberSafeSalt produces a salt ≤ 2^53 — required because PM sends
// salt as a JSON NUMBER and parses with parseInt(salt, 10).
func jsNumberSafeSalt() (*big.Int, error) {
	nowMs := time.Now().UTC().UnixMilli()
	// 50-bit mask to stay well below 2^53.
	mask := (int64(1) << 50) - 1
	v := nowMs & mask
	if v == 0 {
		v = 1
	}
	return big.NewInt(v), nil
}

func cancelOrder(ctx context.Context, c *clob.Client, creds clob.L2Credentials, makerEOA, orderID string) error {
	body := []byte(fmt.Sprintf(`{"orderID":"%s"}`, orderID))
	apiKey, sign, ts, passphrase, err := clob.L2Headers(creds, time.Now().UTC(), http.MethodDelete, "/order", string(body))
	if err != nil {
		return fmt.Errorf("L2Headers: %w", err)
	}
	headers := map[string]string{
		"POLY_ADDRESS":    strings.ToLower(makerEOA),
		"POLY_API_KEY":    apiKey,
		"POLY_SIGNATURE":  sign,
		"POLY_TIMESTAMP":  ts,
		"POLY_PASSPHRASE": passphrase,
		"Content-Type":    "application/json",
	}
	_, err = c.RawRequest(ctx, http.MethodDelete, "/order", body, headers)
	return err
}

func envOr(primary, fallback string) string {
	v := strings.TrimSpace(os.Getenv(primary))
	if v != "" {
		return v
	}
	return strings.TrimSpace(fallback)
}

func die(msg string) {
	fmt.Fprintln(os.Stderr, msg)
	os.Exit(1)
}
