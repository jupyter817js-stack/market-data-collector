// Command clob-auto-submit is the always-on Polymarket CLOB v2 submitter.
// It polls signals_buy_candidates_fresh, runs every candidate through
// internal/clob.RiskConfig, and (when -really-submit is set) signs +
// POSTs the survivors as live orders.
//
// SAFETY POSTURE
//
//   - Default behaviour does NOT call the network: the daemon evaluates
//     risk, signs the order, inserts the proposed_orders row with status
//     'planned', and stops. Useful to dry-run the gate against live signals.
//   - -really-submit gates POST /order. Even with it set, every order
//     still has to pass the RiskConfig gates (per-token cap, total
//     exposure cap, rate cap, cooldown, kill-switch file, size bounds).
//   - The kill switch is a filesystem path. `touch <path>` halts trading
//     within the next tick; `rm <path>` resumes it.
//   - One BUY_CANDIDATE → at most one order per tick per matchup+token+
//     side. The CooldownPerMatchup gate prevents the same signal from
//     firing repeatedly across ticks while it remains BUY_CANDIDATE.
//
// CONFIG (env vars) — V2 layout:
//
//	POLYMARKET_SIGNER_PK                  EOA private key (hex; 0x optional)
//	POLYMARKET_PROXY_ADDRESS              funder/proxy address (POLY_1271, mode B).
//	                                      EMPTY -> mode A (EOA-only, signatureType=0).
//	POLYMARKET_SIGNATURE_TYPE             -1 (auto: 3 if PROXY set, else 0) | 0 | 1 | 2 | 3
//	POLYMARKET_BUILDER_CODE               bytes32 hex of PM Builder ID (optional)
//
//	# Legacy V1 names (still read as fallback):
//	POLYMARKET_PRIVATE_KEY                -> use POLYMARKET_SIGNER_PK
//	POLYMARKET_MAKER_ADDRESS              -> use POLYMARKET_PROXY_ADDRESS
//
//	CLOB_BASE_URL                         default https://clob.polymarket.com
//	CLOB_CREDS_FILE                       default <user config dir>/pol-odds-collector/clob-creds.json
//	RISK_MAX_POSITION_USDC                per-(token,side) cap (default 50)
//	RISK_MAX_TOTAL_EXPOSURE_USDC          across all live positions (default 200)
//	RISK_MAX_ORDERS_PER_HOUR              rate cap (default 10)
//	RISK_COOLDOWN_MATCHUP_SEC             dedupe window (default 300)
//	RISK_EXPOSURE_WINDOW_HOURS            exposure look-back (default 24)
//	RISK_KILL_SWITCH_PATH                 file-based kill switch (default ./var/clob-kill-switch)
//	RISK_MIN_SIZE_USDC                    min order size (default 5)
//	RISK_MAX_SIZE_USDC_PER_ORDER          per-order size cap (default 50)
//
// Usage:
//
//	clob-auto-submit                              # one tick, no network (audit only)
//	clob-auto-submit -loop 5s                     # daemon, no network
//	clob-auto-submit -loop 5s -really-submit      # ACTUALLY TRADE — be sure
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
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
	loop := flag.Duration("loop", 0, "tick interval (e.g. 5s); zero = one tick then exit")
	limit := flag.Int("limit", 10, "max signals to evaluate per tick")
	reallySubmit := flag.Bool("really-submit", false, "REQUIRED to POST /order. Without it, the gate runs and rows are inserted but no network call happens.")
	negRisk := flag.Bool("neg-risk", false, "use NegRiskCTFExchangeAddress as verifyingContract")
	logLevel := flag.String("log-level", "info", "debug | info | warn | error")
	flag.Parse()

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: parseLevel(*logLevel)})))

	cfg, err := config.Load()
	if err != nil {
		die("config: " + err.Error())
	}
	// V2 env vars (with V1 fallback for backward compat).
	privKey := strings.TrimSpace(os.Getenv("POLYMARKET_SIGNER_PK"))
	if privKey == "" {
		privKey = strings.TrimSpace(os.Getenv("POLYMARKET_PRIVATE_KEY"))
	}
	if privKey == "" {
		die("POLYMARKET_SIGNER_PK env required (EOA private key, hex)")
	}
	proxy := strings.TrimSpace(os.Getenv("POLYMARKET_PROXY_ADDRESS"))
	if proxy == "" {
		proxy = strings.TrimSpace(os.Getenv("POLYMARKET_MAKER_ADDRESS"))
	}
	// signerAddr is derived from privKey; do that lazily via clob.Sign.
	// For wallet mode B (POLY_1271), maker=proxy and signer=EOA.
	// For mode A (EOA), maker=signer=EOA — the EOA is filled in via
	// SignerAddress only, and Build() copies it to Maker when both are unset.
	sigTypeStr := strings.TrimSpace(os.Getenv("POLYMARKET_SIGNATURE_TYPE"))
	if sigTypeStr == "" {
		sigTypeStr = "-1" // auto
	}
	sigTypeInt, perr := strconv.Atoi(sigTypeStr)
	if perr != nil {
		die("POLYMARKET_SIGNATURE_TYPE must be -1/0/1/2/3: " + perr.Error())
	}
	if sigTypeInt < -1 || sigTypeInt > 3 {
		die(fmt.Sprintf("POLYMARKET_SIGNATURE_TYPE out of range: %d", sigTypeInt))
	}
	// Auto-resolve sigType: proxy present => 3 (POLY_1271), else 0 (EOA).
	var sigType executor.SignatureType
	if sigTypeInt == -1 {
		if proxy != "" {
			sigType = executor.SignatureTypePoly1271
		} else {
			sigType = executor.SignatureTypeEOA
		}
	} else {
		sigType = executor.SignatureType(sigTypeInt)
	}
	// In mode B (POLY_1271), proxy is required.
	if sigType == executor.SignatureTypePoly1271 && proxy == "" {
		die("POLYMARKET_PROXY_ADDRESS env required when SIGNATURE_TYPE=3")
	}
	// Resolve maker:
	//   mode A (EOA): maker = signer = EOA (we don't know EOA yet — derive
	//                 from privKey via clob.Sign first; here we'll let
	//                 Build use cfg.SignerAddress="").
	//   mode B (1271): maker = proxy.
	maker := proxy
	if maker == "" {
		// Mode A. We must derive the EOA address from privKey to fill maker
		// because PM's POLY_ADDRESS header needs it lowercase. We derive it
		// here once.
		eoaAddr, derr := clob.DeriveEOAAddress(privKey)
		if derr != nil {
			die("derive EOA from POLYMARKET_SIGNER_PK: " + derr.Error())
		}
		maker = eoaAddr
	}
	builderCode := strings.TrimSpace(os.Getenv("POLYMARKET_BUILDER_CODE"))
	slog.Info("wallet mode resolved",
		"sig_type", sigType,
		"maker", maker,
		"proxy_set", proxy != "",
		"builder_code_set", builderCode != "",
	)

	risk := loadRiskConfig()
	for _, w := range risk.Validate() {
		slog.Warn("risk config", "warning", w)
	}
	slog.Info("risk config loaded",
		"max_position_usdc", risk.MaxPositionUSDC,
		"max_total_usdc", risk.MaxTotalExposureUSDC,
		"max_orders_per_hour", risk.MaxOrdersPerHour,
		"cooldown_per_matchup", risk.CooldownPerMatchup,
		"exposure_window", risk.ExposureWindow,
		"kill_switch_path", risk.KillSwitchPath,
		"min_size_usdc", risk.MinSizeUSDC,
		"max_size_usdc_per_order", risk.MaxSizeUSDCPerOrder,
	)

	baseURL := envDefault("CLOB_BASE_URL", clob.DefaultBaseURL)
	credsPath := envDefault("CLOB_CREDS_FILE", clob.DefaultCredsPath())

	var creds clob.L2Credentials
	if *reallySubmit {
		f, err := clob.LoadCreds(credsPath)
		if err != nil {
			die("creds: " + err.Error())
		}
		if !strings.EqualFold(f.Address, maker) {
			die(fmt.Sprintf("creds address %s != POLYMARKET_MAKER_ADDRESS %s — refusing to trade", f.Address, maker))
		}
		creds = f.As()
		slog.Warn("auto-submit ARMED FOR LIVE TRADING",
			"base_url", baseURL, "maker", maker, "api_key", f.APIKey)
	} else {
		slog.Info("auto-submit dry-run (no -really-submit; rows go in as 'planned', no POST)")
	}

	var (
		ctx    context.Context
		cancel context.CancelFunc
	)
	if *loop > 0 {
		ctx, cancel = signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	} else {
		ctx, cancel = context.WithTimeout(context.Background(), 60*time.Second)
	}
	defer cancel()

	db, err := sql.Open("pgx", cfg.DatabaseURL)
	if err != nil {
		die("open db: " + err.Error())
	}
	defer func() { _ = db.Close() }()
	if err := db.PingContext(ctx); err != nil {
		die("ping db: " + err.Error())
	}
	pg, err := store.Open(ctx, cfg.DatabaseURL, false, normalize.FreshnessPolicy{
		FreshAfter: cfg.FreshAfter, AgingAfter: cfg.AgingAfter,
	})
	if err != nil {
		die("open postgres store: " + err.Error())
	}
	defer func() { _ = pg.Close() }()
	ps := store.NewProposedOrderStore(pg)

	domain := clob.DefaultDomain()
	if *negRisk {
		domain.VerifyingContract = clob.NegRiskCTFExchangeAddress
	}
	client := clob.NewClient(baseURL, 15*time.Second)

	tick := func() {
		signals, err := loadFreshBuyCandidates(ctx, db, *limit)
		if err != nil {
			slog.Error("load signals", "err", err)
			return
		}
		if len(signals) == 0 {
			slog.Info("auto-submit tick: no fresh BUY_CANDIDATE signals")
			return
		}
		allowed := 0
		refused := 0
		errors := 0
		for _, sig := range signals {
			cand := clob.OrderCandidate{
				MatchupID: sig.MatchupID,
				TokenID:   sig.PolymarketTokenID,
				Side:      "BUY",
				SizeUSDC:  sig.ProposedSizeUSDC,
			}
			decision, err := risk.Evaluate(ctx, ps, cand)
			if err != nil {
				slog.Error("risk evaluate", "signal_id", sig.ID, "err", err)
				errors++
				continue
			}
			if !decision.Allowed {
				slog.Info("refuse",
					"signal_id", sig.ID, "matchup", sig.MatchupID,
					"home", sig.HomeName, "away", sig.AwayName,
					"reason", decision.Reason)
				refused++
				continue
			}
			if err := submitOne(ctx, ps, client, domain, sig, walletCfg{
				Maker: maker, Proxy: proxy, SigType: sigType, BuilderCode: builderCode,
			}, privKey, creds, *reallySubmit); err != nil {
				slog.Error("submit one", "signal_id", sig.ID, "err", err)
				errors++
				continue
			}
			allowed++
		}
		slog.Info("auto-submit tick done",
			"considered", len(signals), "allowed", allowed, "refused", refused, "errors", errors)
	}

	if *loop <= 0 {
		tick()
		return
	}
	t := time.NewTicker(*loop)
	defer t.Stop()
	tick()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			tick()
		}
	}
}

// walletCfg captures the resolved wallet mode for one submitOne call.
type walletCfg struct {
	Maker       string                 // funder/proxy (mode B) or EOA (mode A)
	Proxy       string                 // empty in mode A
	SigType     executor.SignatureType // resolved at startup
	BuilderCode string                 // bytes32 hex; empty -> no builder
}

// submitOne handles one allowed signal end-to-end: build → sign → insert
// planned row → (with reallySubmit) POST → RecordSent or MarkTerminal.
func submitOne(ctx context.Context, ps *store.ProposedOrderStore, client *clob.Client, domain clob.DomainConfig, sig sigpkg.Signal, wallet walletCfg, privKey string, creds clob.L2Credentials, reallySubmit bool) error {
	// In mode A SignerAddress is derived from privKey by clob.Sign; we
	// still need to set both Maker and Signer here. For mode B SignerAddress
	// is the EOA (derived from privKey at runtime), Maker is the proxy.
	signerEOA, err := clob.DeriveEOAAddress(privKey)
	if err != nil {
		return fmt.Errorf("derive signer EOA: %w", err)
	}
	spec, err := executor.Build(sig, executor.BuildConfig{
		MakerAddress:  wallet.Maker,
		SignerAddress: signerEOA,
		SignatureType: wallet.SigType,
		BuilderCode:   wallet.BuilderCode,
		OrderType:     executor.OrderTypeFAK,
	})
	if err != nil {
		return fmt.Errorf("build: %w", err)
	}
	if spec.SourceSignalID == 0 {
		spec.SourceSignalID = sig.ID
	}
	signed, err := clob.SignOrder(spec, domain, privKey)
	if err != nil {
		return fmt.Errorf("sign: %w", err)
	}
	notes := fmt.Sprintf("auto-submit signal=%d order_hash=%s signer=%s",
		sig.ID, signed.OrderHash.Hex(), signed.SignerAddr.Hex())
	rowID, err := ps.InsertPlanned(ctx, spec, notes)
	if err != nil {
		return fmt.Errorf("insert planned: %w", err)
	}
	slog.Info("planned",
		"row_id", rowID, "signal_id", sig.ID, "matchup", sig.MatchupID,
		"home", sig.HomeName, "away", sig.AwayName,
		"side", spec.OutcomeSide, "size_usdc", spec.SizeUSDC, "price", spec.Price)

	if !reallySubmit {
		return nil
	}

	resp, err := client.SubmitOrder(ctx, signed, creds, executor.OrderTypeFAK)
	if err != nil {
		_ = ps.UpdateStatus(ctx, rowID, "rejected", "submit error: "+err.Error())
		return fmt.Errorf("submit: %w", err)
	}
	respJSON, _ := json.Marshal(resp)
	if resp.OrderID == "" {
		// Server accepted but returned no order id — treat as rejected so the
		// fill monitor doesn't try to poll an empty string.
		_ = ps.UpdateStatus(ctx, rowID, "rejected", "no orderID in response: "+string(respJSON))
		return fmt.Errorf("submit: no orderID returned (body=%s)", string(respJSON))
	}
	if err := ps.RecordSent(ctx, rowID, resp.OrderID, "submitted: "+string(respJSON)); err != nil {
		return fmt.Errorf("record sent: %w", err)
	}
	slog.Warn("LIVE ORDER SUBMITTED",
		"row_id", rowID, "clob_order_id", resp.OrderID,
		"status", resp.Status, "size_usdc", spec.SizeUSDC, "price", spec.Price,
		"matchup", sig.MatchupID, "home", sig.HomeName, "away", sig.AwayName)
	return nil
}

// loadFreshBuyCandidates pulls the freshest BUY_CANDIDATE rows from the
// signals view. signal-engine is the producer; this command is the consumer.
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

func loadRiskConfig() clob.RiskConfig {
	return clob.RiskConfig{
		MaxPositionUSDC:      envFloat("RISK_MAX_POSITION_USDC", 50),
		MaxTotalExposureUSDC: envFloat("RISK_MAX_TOTAL_EXPOSURE_USDC", 200),
		MaxOrdersPerHour:     envInt("RISK_MAX_ORDERS_PER_HOUR", 10),
		CooldownPerMatchup:   time.Duration(envInt("RISK_COOLDOWN_MATCHUP_SEC", 300)) * time.Second,
		ExposureWindow:       time.Duration(envInt("RISK_EXPOSURE_WINDOW_HOURS", 24)) * time.Hour,
		KillSwitchPath:       envDefault("RISK_KILL_SWITCH_PATH", "./var/clob-kill-switch"),
		MinSizeUSDC:          envFloat("RISK_MIN_SIZE_USDC", 5),
		MaxSizeUSDCPerOrder:  envFloat("RISK_MAX_SIZE_USDC_PER_ORDER", 50),
	}
}

func envDefault(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}
func envFloat(key string, def float64) float64 {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}
func envInt(key string, def int) int {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
func parseLevel(s string) slog.Level {
	switch s {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
func die(msg string) {
	fmt.Fprintln(os.Stderr, msg)
	os.Exit(1)
}
