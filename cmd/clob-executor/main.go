// Command clob-executor previews what Polymarket CLOB orders WOULD be
// submitted for the freshest BUY_CANDIDATE signals. Phase 1: builds the
// OrderSpec via internal/executor and signs it via internal/clob, then
// prints the JSON-ready order body. No HTTP submission yet — that lands
// in a later commit once the L2 credential bootstrap is wired.
//
//	# preview without signing (no private key required)
//	go run ./cmd/clob-executor -limit 5
//
//	# sign the previews so the operator can eyeball the EIP-712 output
//	POLYMARKET_PRIVATE_KEY=<hex-no-0x> go run ./cmd/clob-executor -limit 5 -sign
//
// The maker address (the EOA that will sign + fund) must be set via
// POLYMARKET_MAKER_ADDRESS (or -maker). With -sign, we additionally
// require the private key to derive the same address — otherwise we
// refuse to sign (defensive against mis-pairing keys and addresses).
//
// SAFETY: the binary cannot post orders. It does not import an http
// client targeting clob.polymarket.com. Real submission is a separate
// later command + ramp.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/your-org/pol-odds-collector/internal/clob"
	"github.com/your-org/pol-odds-collector/internal/config"
	"github.com/your-org/pol-odds-collector/internal/executor"
	"github.com/your-org/pol-odds-collector/internal/normalize"
	sigpkg "github.com/your-org/pol-odds-collector/internal/signal"
	"github.com/your-org/pol-odds-collector/internal/store"
)

func main() {
	mode := flag.String("mode", "preview", "preview | bootstrap | show-creds | submit")
	limit := flag.Int("limit", 5, "preview: max BUY_CANDIDATE signals to preview")
	signFlag := flag.Bool("sign", false, "preview: sign each (requires POLYMARKET_PRIVATE_KEY)")
	makerFlag := flag.String("maker", "", "override POLYMARKET_MAKER_ADDRESS")
	negRisk := flag.Bool("neg-risk", false, "use NegRiskCTFExchangeAddress as verifyingContract")
	sizeOverride := flag.Float64("size-usdc", 0, "preview/submit: override SignalProposedSizeUSDC (0=use signal)")
	demo := flag.Bool("demo", false, "preview: skip DB; use a canonical synthesized BUY_CANDIDATE")
	credsFile := flag.String("creds-file", "", "bootstrap/show-creds/submit: path to L2 creds JSON (default: user config dir)")
	baseURL := flag.String("base-url", clob.DefaultBaseURL, "bootstrap/submit: CLOB API origin")
	reallyBootstrap := flag.Bool("really-bootstrap", false, "bootstrap: required to actually POST /auth/api-key")
	signalID := flag.Int64("signal-id", 0, "submit: signals.id to send (0 = pick freshest BUY_CANDIDATE)")
	reallySubmit := flag.Bool("really-submit", false, "submit: required to actually POST /order (no-flag = build + sign + persist row, never POST)")
	verifyAsEOA := flag.Bool("verify-as-eoa", false, "verify: send POLY_ADDRESS = EOA (not proxy). Try this if balance-allowance returns 401.")
	verifyVerbose := flag.Bool("verify-verbose", false, "verify: print HMAC prehash and base64 secret prefix for debugging 401")
	verifyToken := flag.String("verify-token", "", "verify: override the test token_id for /book (default uses a stale fixture)")
	flag.Parse()

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	credsPath := strings.TrimSpace(*credsFile)
	if credsPath == "" {
		credsPath = clob.DefaultCredsPath()
	}

	// Load .env FIRST so every mode (including bootstrap) sees the
	// V2 env vars. Previously this lived after the switch and bootstrap
	// silently ran with an empty env.
	cfg, cerr := config.Load()
	if cerr != nil {
		die("config: " + cerr.Error())
	}
	_ = cfg

	switch *mode {
	case "show-creds":
		runShowCreds(credsPath)
		return
	case "bootstrap":
		runBootstrap(credsPath, *baseURL, *reallyBootstrap)
		return
	case "submit":
		maker := strings.TrimSpace(*makerFlag)
		if maker == "" {
			maker = pmMakerOrProxyAddress()
		}
		if maker == "" {
			die("submit: POLYMARKET_PROXY_ADDRESS (or -maker) required")
		}
		runSubmit(credsPath, *baseURL, maker, *signalID, *sizeOverride, *negRisk, *reallySubmit)
		return
	case "verify":
		runVerify(credsPath, *baseURL, *verifyAsEOA, *verifyVerbose, *verifyToken)
		return
	case "preview":
		// fall through
	default:
		die("unknown -mode: " + *mode + " (want preview | bootstrap | show-creds | verify | submit)")
	}

	maker := strings.TrimSpace(*makerFlag)
	if maker == "" {
		maker = pmMakerOrProxyAddress()
	}
	if maker == "" {
		die("POLYMARKET_PROXY_ADDRESS (or -maker) is required; this is the EOA/proxy that would sign + pay")
	}

	privKey := pmSignerPrivateKey()
	if *signFlag && privKey == "" {
		die("-sign was set but POLYMARKET_SIGNER_PK env is empty")
	}

	domain := clob.DefaultDomain()
	if *negRisk {
		domain.VerifyingContract = clob.NegRiskCTFExchangeAddress
	}

	var signals []sigpkg.Signal
	if *demo {
		signals = []sigpkg.Signal{demoSignal(maker)}
	} else {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		db, err := sql.Open("pgx", cfg.DatabaseURL)
		if err != nil {
			die("open db: " + err.Error())
		}
		defer func() { _ = db.Close() }()
		if err := db.PingContext(ctx); err != nil {
			die("ping db: " + err.Error())
		}

		signals, err = loadFreshBuyCandidates(ctx, db, *limit)
		if err != nil {
			die("load signals: " + err.Error())
		}
		if len(signals) == 0 {
			slog.Info("no fresh BUY_CANDIDATE signals in window")
			return
		}
	}

	buildCfg := executor.BuildConfig{
		MakerAddress:     maker,
		OrderType:        executor.OrderTypeFAK,
		SizeOverrideUSDC: *sizeOverride,
	}

	for i, sig := range signals {
		spec, err := executor.Build(sig, buildCfg)
		if err != nil {
			slog.Warn("skip: build", "signal_id", sig.ID, "err", err)
			continue
		}
		out := previewLine{Index: i, Spec: orderSpecJSON(spec, domain)}

		if *signFlag {
			signed, err := clob.Sign(spec, domain, privKey)
			if err != nil {
				slog.Warn("skip: sign", "signal_id", sig.ID, "err", err)
				continue
			}
			out.Signed = &signedJSON{
				OrderHash:  signed.OrderHash.Hex(),
				Signature:  signed.Signature,
				SignerAddr: signed.SignerAddr.Hex(),
			}
		}
		b, _ := json.MarshalIndent(out, "", "  ")
		fmt.Println(string(b))
	}
}

type previewLine struct {
	Index  int           `json:"i"`
	Spec   orderSpecBody `json:"spec"`
	Signed *signedJSON   `json:"signed,omitempty"`
}

type orderSpecBody struct {
	Maker             string `json:"maker"`
	Signer            string `json:"signer"`
	TokenID           string `json:"tokenId"`
	Side              string `json:"side"`
	OrderType         string `json:"orderType"`
	MakerAmount       string `json:"makerAmount"`
	TakerAmount       string `json:"takerAmount"`
	Salt              string `json:"salt"`
	Expiration        int64  `json:"expiration"`
	SignatureType     int    `json:"signatureType"`
	VerifyingContract string `json:"verifyingContract"`
	ChainID           int64  `json:"chainId"`

	// Audit fields (will not be POSTed; useful for visual inspection)
	Price          float64 `json:"price"`
	SizeUSDC       float64 `json:"sizeUSDC"`
	HomeName       string  `json:"home"`
	AwayName       string  `json:"away"`
	OutcomeSide    string  `json:"outcomeSide"`
	OutcomeName    string  `json:"outcomeName"`
	LeagueName     string  `json:"league"`
	SourceSignalID int64   `json:"signalID"`
	ClaimedNetEdge float64 `json:"netEdge"`
}

type signedJSON struct {
	OrderHash  string `json:"orderHash"`
	Signature  string `json:"signature"`
	SignerAddr string `json:"signerAddr"`
}

func orderSpecJSON(s executor.OrderSpec, domain clob.DomainConfig) orderSpecBody {
	return orderSpecBody{
		Maker:             s.Maker,
		Signer:            s.Signer,
		TokenID:           s.TokenID,
		Side:              string(s.Side),
		OrderType:         string(s.OrderType),
		MakerAmount:       s.MakerAmount.String(),
		TakerAmount:       s.TakerAmount.String(),
		Salt:              s.Salt.String(),
		Expiration:        s.Expiration,
		SignatureType:     int(s.SignatureType),
		VerifyingContract: domain.VerifyingContract.Hex(),
		ChainID:           domain.ChainID,
		Price:             s.Price,
		SizeUSDC:          s.SizeUSDC,
		HomeName:          s.HomeName,
		AwayName:          s.AwayName,
		OutcomeSide:       s.OutcomeSide,
		OutcomeName:       s.OutcomeName,
		LeagueName:        s.LeagueName,
		SourceSignalID:    s.SourceSignalID,
		ClaimedNetEdge:    s.ClaimedNetEdge,
	}
}

// loadFreshBuyCandidates pulls the most recent BUY_CANDIDATE rows from
// signals_buy_candidates_fresh view. The view already enforces freshness
// gates per signal-engine config.
func loadFreshBuyCandidates(ctx context.Context, db *sql.DB, limit int) ([]sigpkg.Signal, error) {
	rows, err := db.QueryContext(ctx, `
        SELECT id, matchup_id, polymarket_condition_id, polymarket_token_id,
               outcome_side, outcome_name, home_name, away_name, league_name,
               COALESCE(pinnacle_fair_prob, 0)::float8,
               COALESCE(pm_ask, 0)::float8,
               COALESCE(gross_edge_to_buy, 0)::float8,
               COALESCE(net_edge_buy, 0)::float8,
               COALESCE(proposed_size_usdc, 0)::float8,
               state,
               COALESCE(pinnacle_age_sec, 0)::float8,
               COALESCE(pm_age_sec, 0)::float8
          FROM signals_buy_candidates_fresh
         ORDER BY created_at DESC
         LIMIT $1
    `, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []sigpkg.Signal
	for rows.Next() {
		var s sigpkg.Signal
		var ask, fair, gross, net, size, pinAge, pmAge float64
		if err := rows.Scan(&s.ID, &s.MatchupID, &s.PolymarketConditionID, &s.PolymarketTokenID,
			&s.OutcomeSide, &s.OutcomeName, &s.HomeName, &s.AwayName, &s.LeagueName,
			&fair, &ask, &gross, &net, &size, &s.State, &pinAge, &pmAge); err != nil {
			return nil, err
		}
		s.PinnacleFairProb = fair
		s.PMAsk = sql.NullFloat64{Float64: ask, Valid: ask > 0}
		s.GrossEdgeToBuy = gross
		s.NetEdgeBuy = net
		s.ProposedSizeUSDC = size
		s.PinnacleAgeSec = pinAge
		s.PMAgeSec = pmAge
		out = append(out, s)
	}
	return out, rows.Err()
}

// runShowCreds prints the metadata of the stored L2 creds file. NEVER
// prints the api secret. Useful for auditing which address / api key
// is currently armed for trading.
func runShowCreds(path string) {
	f, err := clob.LoadCreds(path)
	if err != nil {
		die("show-creds: " + err.Error())
	}
	out := map[string]any{
		"path":           path,
		"address":        f.Address,
		"base_url":       f.BaseURL,
		"api_key":        f.APIKey,
		"api_passphrase": f.Passphrase,
		"issued_at_utc":  f.IssuedAt.Format(time.RFC3339),
		"api_secret":     "<redacted; never printed>",
	}
	b, _ := json.MarshalIndent(out, "", "  ")
	fmt.Println(string(b))
}

// runBootstrap exchanges the L1 EOA signature for an L2 trading credential
// and saves it to disk. Default behaviour is DRY-RUN — it only prints the
// signed request headers and exits without contacting the network. The
// operator must pass -really-bootstrap to actually POST to clob.polymarket.
// com (this is the first network egress this binary can do; the gate is
// deliberate).
func runBootstrap(credsPath, baseURL string, reallyBootstrap bool) {
	privKey := pmSignerPrivateKey()
	if privKey == "" {
		die("bootstrap: POLYMARKET_SIGNER_PK env is required (legacy POLYMARKET_PRIVATE_KEY also accepted)")
	}
	// Mode B (POLY_1271): if POLYMARKET_PROXY_ADDRESS is set we ask PM
	// to issue api-keys bound to the proxy, not to the bare EOA. The
	// signature itself is still produced with the EOA private key.
	funder := pmMakerOrProxyAddress()
	authDomain := clob.DefaultAuthDomain()
	req, err := clob.SignAuthWithFunder(privKey, funder, authDomain)
	if err != nil {
		die("bootstrap: SignAuth: " + err.Error())
	}
	if funder != "" {
		slog.Info("bootstrap: mode B — POLY_ADDRESS will carry funder",
			"funder", funder, "signer_eoa", req.Address.Hex())
	} else {
		slog.Info("bootstrap: mode A — POLY_ADDRESS = EOA", "eoa", req.Address.Hex())
	}
	if err := clob.VerifyAuthRoundTrip(req, authDomain); err != nil {
		die("bootstrap: self-verify: " + err.Error())
	}

	if !reallyBootstrap {
		out := map[string]any{
			"mode":      "DRY_RUN",
			"address":   req.Address.Hex(),
			"base_url":  baseURL,
			"will_post": baseURL + "/auth/api-key",
			"headers":   req.Headers(),
			"hint":      "pass -really-bootstrap to actually POST and persist L2 creds",
		}
		b, _ := json.MarshalIndent(out, "", "  ")
		fmt.Println(string(b))
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c := clob.NewClient(baseURL, 30*time.Second)
	slog.Info("bootstrap: POSTing /auth/api-key", "base_url", baseURL, "address", req.Address.Hex())
	creds, err := c.BootstrapL2(ctx, req)
	if err != nil {
		die("bootstrap: BootstrapL2: " + err.Error())
	}

	file := clob.L2CredsFile{
		Address:    req.Address.Hex(),
		IssuedAt:   time.Now().UTC(),
		BaseURL:    baseURL,
		APIKey:     creds.APIKey,
		APISecret:  creds.APISecret,
		Passphrase: creds.APIPassphrase,
	}
	if err := clob.SaveCreds(credsPath, file); err != nil {
		die("bootstrap: SaveCreds: " + err.Error())
	}
	slog.Info("bootstrap: L2 creds saved", "path", credsPath, "address", req.Address.Hex(), "api_key", creds.APIKey)
}

// runSubmit drives the full live-trading flow for one signal:
//  1. Load L2 creds (the API key + HMAC secret bootstrapped earlier)
//  2. Fetch the signal (by id, or the freshest BUY_CANDIDATE)
//  3. Build OrderSpec via executor.Build, sign L1 via clob.Sign
//  4. Insert a proposed_orders row with status='planned' (atomic
//     audit before any network call)
//  5. Without -really-submit: print the body that WOULD be POSTed
//     and exit (the row remains 'planned'; useful for last-mile
//     validation before going live)
//  6. With -really-submit: POST /order, update the row to
//     'sent' (success) or 'rejected' (server error / network)
//
// SAFETY:
//   - The default behaviour does NOT send to the network.
//   - This binary processes exactly one signal per invocation; no
//     loop/daemon mode. Repeated runs are intentional friction; an
//     always-on submitter is Phase 5 once the kill-switch + position
//     gates land.
func runSubmit(credsPath, baseURL, maker string, signalID int64, sizeOverride float64, negRisk bool, reallySubmit bool) {
	privKey := pmSignerPrivateKey()
	if privKey == "" {
		die("submit: POLYMARKET_SIGNER_PK env required (legacy POLYMARKET_PRIVATE_KEY also accepted)")
	}

	credsFile, err := clob.LoadCreds(credsPath)
	if err != nil {
		die("submit: load creds (" + credsPath + "): " + err.Error())
	}
	creds := credsFile.As()

	cfg, err := config.Load()
	if err != nil {
		die("submit: config: " + err.Error())
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db, err := sql.Open("pgx", cfg.DatabaseURL)
	if err != nil {
		die("submit: open db: " + err.Error())
	}
	defer func() { _ = db.Close() }()
	if err := db.PingContext(ctx); err != nil {
		die("submit: ping db: " + err.Error())
	}

	sig, err := loadOneBuyCandidate(ctx, db, signalID)
	if err != nil {
		die("submit: load signal: " + err.Error())
	}

	domain := clob.DefaultDomain()
	if negRisk {
		domain.VerifyingContract = clob.NegRiskCTFExchangeAddress
	}

	spec, err := executor.Build(sig, executor.BuildConfig{
		MakerAddress:     maker,
		OrderType:        executor.OrderTypeFAK,
		SizeOverrideUSDC: sizeOverride,
	})
	if err != nil {
		die("submit: build: " + err.Error())
	}
	// SourceSignalID is set by executor.Build only if the signal had one;
	// double-check (FK requires non-zero).
	if spec.SourceSignalID == 0 {
		spec.SourceSignalID = sig.ID
	}

	signed, err := clob.Sign(spec, domain, privKey)
	if err != nil {
		die("submit: sign: " + err.Error())
	}

	pg, err := store.Open(ctx, cfg.DatabaseURL, false, normalize.FreshnessPolicy{
		FreshAfter: cfg.FreshAfter,
		AgingAfter: cfg.AgingAfter,
	})
	if err != nil {
		die("submit: open postgres store: " + err.Error())
	}
	defer func() { _ = pg.Close() }()
	ps := store.NewProposedOrderStore(pg)

	notes := fmt.Sprintf("clob-executor submit signal=%d order_hash=%s signer=%s domain=%s chainid=%d",
		sig.ID, signed.OrderHash.Hex(), signed.SignerAddr.Hex(),
		domain.VerifyingContract.Hex(), domain.ChainID)
	rowID, err := ps.InsertPlanned(ctx, spec, notes)
	if err != nil {
		die("submit: persist planned: " + err.Error())
	}
	slog.Info("submit: inserted proposed_orders row", "id", rowID, "status", "planned", "signal_id", sig.ID)

	bodyJSON, err := clob.BodyJSON(signed, creds.APIKey, executor.OrderTypeFAK)
	if err != nil {
		die("submit: BodyJSON: " + err.Error())
	}

	if !reallySubmit {
		out := map[string]any{
			"mode":              "DRY_RUN",
			"proposed_order_id": rowID,
			"signal_id":         sig.ID,
			"will_post":         baseURL + "/order",
			"body":              json.RawMessage(bodyJSON),
			"hint":              "pass -really-submit to actually POST",
		}
		b, _ := json.MarshalIndent(out, "", "  ")
		fmt.Println(string(b))
		return
	}

	slog.Warn("submit: POSTing live order", "base_url", baseURL, "signal_id", sig.ID, "row_id", rowID,
		"signer", signed.SignerAddr.Hex(), "size_usdc", spec.SizeUSDC, "price", spec.Price)
	c := clob.NewClient(baseURL, 15*time.Second)
	resp, err := c.SubmitOrder(ctx, signed, creds, executor.OrderTypeFAK)
	if err != nil {
		_ = ps.UpdateStatus(ctx, rowID, "rejected", "submit error: "+err.Error())
		die("submit: SubmitOrder: " + err.Error())
	}
	respJSON, _ := json.Marshal(resp)
	status := "sent"
	if resp.Status == "matched" || resp.Status == "filled" {
		status = "filled"
	}
	if err := ps.UpdateStatus(ctx, rowID, status, "response: "+string(respJSON)); err != nil {
		slog.Error("submit: failed to update row status after success", "row_id", rowID, "err", err)
	}
	out := map[string]any{
		"mode":              "LIVE",
		"proposed_order_id": rowID,
		"signal_id":         sig.ID,
		"row_status":        status,
		"response":          resp,
	}
	b, _ := json.MarshalIndent(out, "", "  ")
	fmt.Println(string(b))
}

// loadOneBuyCandidate fetches one fresh BUY_CANDIDATE. If signalID > 0 it
// reads that specific signal (and refuses if its state isn't BUY_CANDIDATE).
func loadOneBuyCandidate(ctx context.Context, db *sql.DB, signalID int64) (sigpkg.Signal, error) {
	var (
		query string
		args  []any
	)
	if signalID > 0 {
		query = `
            SELECT id, matchup_id, polymarket_condition_id, polymarket_token_id,
                   outcome_side, outcome_name, home_name, away_name, league_name,
                   COALESCE(pinnacle_fair_prob, 0)::float8,
                   COALESCE(pm_ask, 0)::float8,
                   COALESCE(gross_edge_to_buy, 0)::float8,
                   COALESCE(net_edge_buy, 0)::float8,
                   COALESCE(proposed_size_usdc, 0)::float8,
                   state,
                   COALESCE(pinnacle_age_sec, 0)::float8,
                   COALESCE(pm_age_sec, 0)::float8
              FROM signals_buy_candidates_fresh
             WHERE id = $1
             LIMIT 1
        `
		args = []any{signalID}
	} else {
		query = `
            SELECT id, matchup_id, polymarket_condition_id, polymarket_token_id,
                   outcome_side, outcome_name, home_name, away_name, league_name,
                   COALESCE(pinnacle_fair_prob, 0)::float8,
                   COALESCE(pm_ask, 0)::float8,
                   COALESCE(gross_edge_to_buy, 0)::float8,
                   COALESCE(net_edge_buy, 0)::float8,
                   COALESCE(proposed_size_usdc, 0)::float8,
                   state,
                   COALESCE(pinnacle_age_sec, 0)::float8,
                   COALESCE(pm_age_sec, 0)::float8
              FROM signals_buy_candidates_fresh
             ORDER BY created_at DESC
             LIMIT 1
        `
	}
	row := db.QueryRowContext(ctx, query, args...)
	var s sigpkg.Signal
	var ask, fair, gross, net, size, pinAge, pmAge float64
	err := row.Scan(&s.ID, &s.MatchupID, &s.PolymarketConditionID, &s.PolymarketTokenID,
		&s.OutcomeSide, &s.OutcomeName, &s.HomeName, &s.AwayName, &s.LeagueName,
		&fair, &ask, &gross, &net, &size, &s.State, &pinAge, &pmAge)
	if err == sql.ErrNoRows {
		if signalID > 0 {
			return sigpkg.Signal{}, fmt.Errorf("no fresh BUY_CANDIDATE row with id=%d", signalID)
		}
		return sigpkg.Signal{}, fmt.Errorf("no fresh BUY_CANDIDATE in window")
	}
	if err != nil {
		return sigpkg.Signal{}, err
	}
	s.PinnacleFairProb = fair
	s.PMAsk = sql.NullFloat64{Float64: ask, Valid: ask > 0}
	s.GrossEdgeToBuy = gross
	s.NetEdgeBuy = net
	s.ProposedSizeUSDC = size
	s.PinnacleAgeSec = pinAge
	s.PMAgeSec = pmAge
	return s, nil
}

// demoSignal returns a canonical BUY_CANDIDATE used by -demo. Fixed
// values so the operator can sanity-check the signed-order JSON
// independently (e.g. against the TS SDK) before live data flows.
func demoSignal(maker string) sigpkg.Signal {
	_ = maker // maker comes from BuildConfig; the Signal itself doesn't carry it
	return sigpkg.Signal{
		EdgeRow: sigpkg.EdgeRow{
			MatchupID:             1629916014,
			PolymarketConditionID: "0x_demo_condition_id",
			PolymarketTokenID:     "12345678901234567890",
			OutcomeSide:           "home",
			OutcomeName:           "Manchester United FC",
			HomeName:              "Manchester United",
			AwayName:              "Nottingham Forest",
			LeagueName:            "England - Premier League",
			PinnacleFairProb:      0.55,
			PMAsk:                 sql.NullFloat64{Float64: 0.50, Valid: true},
			GrossEdgeToBuy:        0.05,
			PinnacleAgeSec:        2.0,
			PMAgeSec:              3.0,
		},
		ID:               -1,
		NetEdgeBuy:       0.035,
		ProposedSizeUSDC: 20.0,
		State:            sigpkg.StateBuyCandidate,
	}
}

func die(msg string) {
	fmt.Fprintln(os.Stderr, msg)
	os.Exit(1)
}

// runVerify exercises every authenticated and unauthenticated path the
// trading daemon will use, so the operator confirms PM accepts our L1
// signing (already proven by bootstrap), L2 HMAC (URL-safe base64 +
// bare-path), and that CTF Exchange has an allowance/deposit. Read-only:
// no orders placed, no money moved.
//
// Checks in order:
//
//	1. show creds from local file
//	2. GET /book?token_id=...   (unauthenticated; sanity)
//	3. GET /balance-allowance?asset_type=COLLATERAL&signature_type=3 (L2)
//	4. print available USDC for trading
func runVerify(credsPath, baseURL string, asEOA, verbose bool, tokenOverride string) {
	credsFile, err := clob.LoadCreds(credsPath)
	if err != nil {
		die("verify: load creds (" + credsPath + "): " + err.Error())
	}
	creds := credsFile.As()
	fmt.Println("[1] L2 creds loaded OK")
	fmt.Printf("    address (EOA): %s\n", credsFile.Address)
	fmt.Printf("    api_key:       %s\n", credsFile.APIKey)
	if verbose {
		// Print first 8 chars of secret to confirm .env vs file consistency
		// without leaking the full key. Also normalize URL-safe vs std.
		sec := creds.APISecret
		if len(sec) > 8 {
			fmt.Printf("    secret prefix: %s... (len=%d)\n", sec[:8], len(sec))
		} else {
			fmt.Printf("    secret prefix: %s (len=%d)\n", sec, len(sec))
		}
		fmt.Printf("    passphrase prefix: %s...\n", credsFile.Passphrase[:min(8, len(credsFile.Passphrase))])
	}

	// POLY_ADDRESS for L2 trading is ALWAYS the EOA (py-clob-client
	// convention). The proxy/funder shows up only in the order body
	// `maker` field and in the `signature_type=3` query parameter.
	proxyEnv := pmMakerOrProxyAddress()
	eoa := credsFile.Address
	sigType := 0
	switch {
	case asEOA:
		sigType = 0
		fmt.Println("    --verify-as-eoa: signature_type=0 (EOA-only path)")
	case proxyEnv != "":
		sigType = 3
		fmt.Printf("    POLYMARKET_PROXY_ADDRESS set -> signature_type=3 (POLY_1271)\n")
		fmt.Printf("    funder/proxy (for body.maker, query): %s\n", strings.ToLower(proxyEnv))
	default:
		sigType = 0
		fmt.Println("    no POLYMARKET_PROXY_ADDRESS set; signature_type=0")
	}
	fmt.Printf("    POLY_ADDRESS will be sent as: %s (EOA always)\n", strings.ToLower(eoa))
	maker := eoa // POLY_ADDRESS header always EOA

	c := clob.NewClient(baseURL, 15*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// [2] Public /book — verifies network + cert + parsing.
	testToken := strings.TrimSpace(tokenOverride)
	if testToken == "" {
		testToken = "42264481236134722863278073323768265860677832848598472536689542143648934697887"
	}
	fmt.Println()
	fmt.Println("[2] GET /book?token_id=... (public, no auth)")
	if err := c.GetBookSanity(ctx, testToken); err != nil {
		fmt.Printf("    FAIL: %v\n", err)
		fmt.Println("    (404 on a stale token is normal — code/network are still fine.)")
	} else {
		fmt.Println("    OK: 200 + parsed bids/asks")
	}

	// [3] L2 /balance-allowance — the gate that proves HMAC works AND
	// that CTF Exchange has been approved+deposited.
	fmt.Println()
	fmt.Println("[3] GET /balance-allowance?asset_type=COLLATERAL (L2 HMAC, bare-path)")
	fmt.Printf("    signature_type query param: %d\n", sigType)
	if verbose {
		// Render the exact prehash and HMAC bytes the request will use.
		// Lets the operator compare against the Polymarket reference flow
		// (curl + py-clob-client) when 401 persists.
		ts := time.Now().UTC()
		_, sign, tsStr, _, herr := clob.L2Headers(creds, ts, "GET", "/balance-allowance", "")
		if herr != nil {
			fmt.Printf("    verbose: L2Headers err: %v\n", herr)
		} else {
			fmt.Printf("    verbose prehash: %sGET/balance-allowance\n", tsStr)
			fmt.Printf("    verbose POLY_SIGNATURE: %s\n", sign)
		}
	}
	bal, err := c.GetBalanceAllowance(ctx, creds, maker, sigType)
	if err != nil {
		fmt.Printf("    FAIL: %v\n", err)
		fmt.Println("    hints:")
		fmt.Println("      401  -> try --verify-as-eoa (some accounts bind creds to EOA)")
		fmt.Println("      401  -> try --verify-verbose to dump prehash + HMAC")
		fmt.Println("      non-401, allowance=0 -> approve+deposit needed on PM UI")
		return
	}
	fmt.Printf("    OK: balance=%s allowance=%s\n", bal.Balance, bal.Allowance)
	if bal.Balance == "0" || bal.Allowance == "0" {
		fmt.Println()
		fmt.Println("⚠️  CLOB-side balance/allowance is 0. PM UI must show USDC")
		fmt.Println("    actually deposited into the proxy AND approved to the")
		fmt.Println("    CTF Exchange contract. The PM UI does this automatically")
		fmt.Println("    when you complete a Deposit flow from a connected wallet.")
	} else {
		fmt.Println("    ready for trading.")
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// pmSignerPrivateKey returns the EOA private key from V2 env name
// (POLYMARKET_SIGNER_PK), falling back to legacy V1 (POLYMARKET_PRIVATE_KEY).
// Returns empty string if neither is set.
func pmSignerPrivateKey() string {
	if v := strings.TrimSpace(os.Getenv("POLYMARKET_SIGNER_PK")); v != "" {
		return v
	}
	return strings.TrimSpace(os.Getenv("POLYMARKET_PRIVATE_KEY"))
}

// pmMakerOrProxyAddress returns the maker/funder/proxy address from V2
// env name (POLYMARKET_PROXY_ADDRESS), falling back to legacy V1
// (POLYMARKET_MAKER_ADDRESS). Returns empty string if neither is set.
func pmMakerOrProxyAddress() string {
	if v := strings.TrimSpace(os.Getenv("POLYMARKET_PROXY_ADDRESS")); v != "" {
		return v
	}
	return strings.TrimSpace(os.Getenv("POLYMARKET_MAKER_ADDRESS"))
}
