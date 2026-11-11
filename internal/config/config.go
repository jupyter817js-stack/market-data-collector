package config

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	OddsAPIKey             string
	OddsAPIBaseURL         string
	OddsAPIMaxRetries      int
	OddsAPIRetryBase       time.Duration
	OddsAPIRetryMax        time.Duration
	PolymarketGammaBaseURL string
	PolymarketCLOBBaseURL  string
	DatabaseURL            string
	SportKeys              []string
	Regions                string
	Markets                string
	Bookmakers             string
	PollInterval           time.Duration
	MaxConcurrentSports    int
	HTTPTimeout            time.Duration
	HealthAddr             string
	PrematchOnly           bool
	FreshAfter             time.Duration
	AgingAfter             time.Duration

	PS3838Username     string
	PS3838Password     string
	PS3838BaseURL      string
	PS3838SportIDs     []int
	PS3838LeagueIDs    []int
	PS3838PollInterval time.Duration
	PS3838OddsFormat   string

	PinnacleBaseURL             string
	PinnacleAPIKey              string
	PinnacleSportIDs            []int
	PinnacleLeagueNameIncludes  []string
	PinnacleLeagueNameExcludes  []string
	PinnacleLeaguePollInterval  time.Duration
	PinnacleMatchupPollInterval time.Duration
	PinnacleMarketsPollInterval time.Duration
	PinnacleMarketsConcurrency  int

	// Signal engine (Step 4). All thresholds are fractions of probability,
	// not percentages — e.g. 0.03 means 3 percentage points of edge.
	SignalPollInterval         time.Duration
	SignalProposedSizeUSDC     float64
	SignalMinNetEdgeBuy        float64
	SignalMinGrossEdgeSell     float64
	SignalMaxPinnacleAgeSec    float64
	SignalMaxPMAgeSec          float64
	SignalMinPMLiquidityUSDC   float64
	SignalSpreadCostMultiplier float64
	SignalSlippageBaseBps      float64
	SignalSlippageVsLiqBps     float64
	SignalLatencyBufferPerSec  float64
	SignalOverroundShare       float64
}

func Load() (Config, error) {
	loadDotEnvIfPresent()
	// ODDS_API_KEY is optional as of 2026-05-17: the live-only pipeline uses
	// Pinnacle + Polymarket only. The Odds API is retained for possible
	// future pre-match analytics; the legacy `cmd/collector` requires it via
	// MustOddsAPIKey() and will fail loudly there if missing.
	oddsAPIKey := strings.TrimSpace(os.Getenv("ODDS_API_KEY"))

	oddsAPIMaxRetries, err := intEnv("ODDS_API_MAX_RETRIES", 2)
	if err != nil {
		return Config{}, err
	}
	oddsAPIRetryBase, err := millisecondsEnv("ODDS_API_RETRY_BASE_MS", 250)
	if err != nil {
		return Config{}, err
	}
	oddsAPIRetryMax, err := millisecondsEnv("ODDS_API_RETRY_MAX_MS", 2000)
	if err != nil {
		return Config{}, err
	}
	pollInterval, err := secondsEnv("POLL_INTERVAL_SECONDS", 60)
	if err != nil {
		return Config{}, err
	}
	maxConcurrentSports, err := positiveIntEnv("MAX_CONCURRENT_SPORTS", 2)
	if err != nil {
		return Config{}, err
	}
	httpTimeout, err := secondsEnv("HTTP_TIMEOUT_SECONDS", 15)
	if err != nil {
		return Config{}, err
	}
	prematchOnly, err := boolEnv("PREMATCH_ONLY", true)
	if err != nil {
		return Config{}, err
	}
	freshAfter, err := secondsEnv("FRESH_AFTER_SECONDS", 120)
	if err != nil {
		return Config{}, err
	}
	agingAfter, err := secondsEnv("AGING_AFTER_SECONDS", 300)
	if err != nil {
		return Config{}, err
	}
	ps3838PollInterval, err := millisecondsEnv("PS3838_POLL_INTERVAL_MS", 1500)
	if err != nil {
		return Config{}, err
	}
	ps3838SportIDs, err := intCSVEnv("PS3838_SPORT_IDS")
	if err != nil {
		return Config{}, err
	}
	ps3838LeagueIDs, err := intCSVEnv("PS3838_LEAGUE_IDS")
	if err != nil {
		return Config{}, err
	}
	ps3838OddsFormat := envDefault("PS3838_ODDS_FORMAT", "Decimal")
	if !validPS3838OddsFormat(ps3838OddsFormat) {
		return Config{}, fmt.Errorf("PS3838_ODDS_FORMAT must be one of American, Decimal, HongKong, Indonesian, Malay")
	}

	pinnSportIDs, err := intCSVEnv("PINNACLE_SPORT_IDS")
	if err != nil {
		return Config{}, err
	}
	pinnLeaguePoll, err := millisecondsEnv("PINNACLE_LEAGUE_POLL_INTERVAL_MS", 60000)
	if err != nil {
		return Config{}, err
	}
	pinnMatchupPoll, err := millisecondsEnv("PINNACLE_MATCHUP_POLL_INTERVAL_MS", 30000)
	if err != nil {
		return Config{}, err
	}
	pinnMarketsPoll, err := millisecondsEnv("PINNACLE_MARKETS_POLL_INTERVAL_MS", 2500)
	if err != nil {
		return Config{}, err
	}
	pinnConcurrency, err := positiveIntEnv("PINNACLE_MARKETS_CONCURRENCY", 3)
	if err != nil {
		return Config{}, err
	}
	pinnIncludes := lowerCSV(os.Getenv("PINNACLE_LEAGUE_NAME_INCLUDES"))
	pinnExcludes := lowerCSV(os.Getenv("PINNACLE_LEAGUE_NAME_EXCLUDES"))

	signalPoll, err := millisecondsEnv("SIGNAL_POLL_INTERVAL_MS", 2000)
	if err != nil {
		return Config{}, err
	}
	signalSize, err := floatEnv("SIGNAL_PROPOSED_SIZE_USDC", 20.0)
	if err != nil {
		return Config{}, err
	}
	signalMinNet, err := floatEnv("SIGNAL_MIN_NET_EDGE_BUY", 0.03)
	if err != nil {
		return Config{}, err
	}
	signalMinGrossSell, err := floatEnv("SIGNAL_MIN_GROSS_EDGE_SELL", 0.03)
	if err != nil {
		return Config{}, err
	}
	signalMaxPinAge, err := floatEnv("SIGNAL_MAX_PINNACLE_AGE_SEC", 10.0)
	if err != nil {
		return Config{}, err
	}
	signalMaxPMAge, err := floatEnv("SIGNAL_MAX_PM_AGE_SEC", 10.0)
	if err != nil {
		return Config{}, err
	}
	signalMinLiq, err := floatEnv("SIGNAL_MIN_PM_LIQUIDITY_USDC", 100.0)
	if err != nil {
		return Config{}, err
	}
	signalSpreadMult, err := floatEnv("SIGNAL_SPREAD_COST_MULTIPLIER", 0.5)
	if err != nil {
		return Config{}, err
	}
	signalSlipBase, err := floatEnv("SIGNAL_SLIPPAGE_BASE_BPS", 50.0)
	if err != nil {
		return Config{}, err
	}
	signalSlipLiq, err := floatEnv("SIGNAL_SLIPPAGE_VS_LIQ_BPS", 200.0)
	if err != nil {
		return Config{}, err
	}
	signalLatBuf, err := floatEnv("SIGNAL_LATENCY_BUFFER_PER_SEC", 0.001)
	if err != nil {
		return Config{}, err
	}
	signalOverShare, err := floatEnv("SIGNAL_OVERROUND_SHARE", 0.5)
	if err != nil {
		return Config{}, err
	}

	cfg := Config{
		OddsAPIKey:             oddsAPIKey,
		OddsAPIBaseURL:         envDefault("ODDS_API_BASE_URL", "https://api.the-odds-api.com"),
		OddsAPIMaxRetries:      oddsAPIMaxRetries,
		OddsAPIRetryBase:       oddsAPIRetryBase,
		OddsAPIRetryMax:        oddsAPIRetryMax,
		PolymarketGammaBaseURL: envDefault("POLYMARKET_GAMMA_BASE_URL", "https://gamma-api.polymarket.com"),
		PolymarketCLOBBaseURL:  envDefault("POLYMARKET_CLOB_BASE_URL", "https://clob.polymarket.com"),
		DatabaseURL:            envDefault("DATABASE_URL", "postgres://pol:pol@localhost:5432/pol_odds?sslmode=disable"),
		SportKeys:              splitCSV(envDefault("SPORT_KEYS", "basketball_nba")),
		Regions:                envDefault("REGIONS", "us"),
		Markets:                envDefault("MARKETS", "h2h,spreads,totals"),
		Bookmakers:             strings.TrimSpace(os.Getenv("BOOKMAKERS")),
		PollInterval:           pollInterval,
		MaxConcurrentSports:    maxConcurrentSports,
		HTTPTimeout:            httpTimeout,
		HealthAddr:             envDefault("HEALTH_ADDR", ":8080"),
		PrematchOnly:           prematchOnly,
		FreshAfter:             freshAfter,
		AgingAfter:             agingAfter,

		PS3838Username:     strings.TrimSpace(os.Getenv("PS3838_USERNAME")),
		PS3838Password:     os.Getenv("PS3838_PASSWORD"),
		PS3838BaseURL:      envDefault("PS3838_BASE_URL", "https://api.ps3838.com"),
		PS3838SportIDs:     ps3838SportIDs,
		PS3838LeagueIDs:    ps3838LeagueIDs,
		PS3838PollInterval: ps3838PollInterval,
		PS3838OddsFormat:   ps3838OddsFormat,

		PinnacleBaseURL:             envDefault("PINNACLE_BASE_URL", "https://guest.api.arcadia.pinnacle.com"),
		PinnacleAPIKey:              envDefault("PINNACLE_API_KEY", ""),
		PinnacleSportIDs:            pinnSportIDs,
		PinnacleLeagueNameIncludes:  pinnIncludes,
		PinnacleLeagueNameExcludes:  pinnExcludes,
		PinnacleLeaguePollInterval:  pinnLeaguePoll,
		PinnacleMatchupPollInterval: pinnMatchupPoll,
		PinnacleMarketsPollInterval: pinnMarketsPoll,
		PinnacleMarketsConcurrency:  pinnConcurrency,

		SignalPollInterval:         signalPoll,
		SignalProposedSizeUSDC:     signalSize,
		SignalMinNetEdgeBuy:        signalMinNet,
		SignalMinGrossEdgeSell:     signalMinGrossSell,
		SignalMaxPinnacleAgeSec:    signalMaxPinAge,
		SignalMaxPMAgeSec:          signalMaxPMAge,
		SignalMinPMLiquidityUSDC:   signalMinLiq,
		SignalSpreadCostMultiplier: signalSpreadMult,
		SignalSlippageBaseBps:      signalSlipBase,
		SignalSlippageVsLiqBps:     signalSlipLiq,
		SignalLatencyBufferPerSec:  signalLatBuf,
		SignalOverroundShare:       signalOverShare,
	}

	if cfg.AgingAfter < cfg.FreshAfter {
		return cfg, fmt.Errorf("AGING_AFTER_SECONDS must be >= FRESH_AFTER_SECONDS")
	}
	if cfg.OddsAPIRetryMax < cfg.OddsAPIRetryBase {
		return cfg, fmt.Errorf("ODDS_API_RETRY_MAX_MS must be >= ODDS_API_RETRY_BASE_MS")
	}
	return cfg, nil
}

// MustOddsAPIConfig validates that legacy Odds-API-specific config is
// present. Call this from `cmd/collector` only — pinnacle-collector and
// polymarket-sync do not need it.
func MustOddsAPIConfig(cfg Config) error {
	if strings.TrimSpace(cfg.OddsAPIKey) == "" || cfg.OddsAPIKey == "replace_me" {
		return errors.New("ODDS_API_KEY is required for cmd/collector (Odds API path)")
	}
	if len(cfg.SportKeys) == 0 {
		return errors.New("SPORT_KEYS must contain at least one sport key")
	}
	if cfg.Regions == "" && cfg.Bookmakers == "" {
		return errors.New("set REGIONS or BOOKMAKERS")
	}
	if cfg.Markets == "" {
		return errors.New("MARKETS is required")
	}
	return nil
}

func envDefault(key, fallback string) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	return v
}

func secondsEnv(key string, fallback int) (time.Duration, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return time.Duration(fallback) * time.Second, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer number of seconds", key)
	}
	return time.Duration(n) * time.Second, nil
}

func millisecondsEnv(key string, fallback int) (time.Duration, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return time.Duration(fallback) * time.Millisecond, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer number of milliseconds", key)
	}
	return time.Duration(n) * time.Millisecond, nil
}

func intEnv(key string, fallback int) (int, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("%s must be a non-negative integer", key)
	}
	return n, nil
}

func positiveIntEnv(key string, fallback int) (int, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", key)
	}
	return n, nil
}

func boolEnv(key string, fallback bool) (bool, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback, nil
	}
	v, err := strconv.ParseBool(raw)
	if err != nil {
		return false, fmt.Errorf("%s must be a boolean", key)
	}
	return v, nil
}

// loadDotEnvIfPresent reads .env from the current working directory or any
// ancestor directory up to filesystem root and sets variables that are not
// already present in the process environment. It is intentionally minimal:
// no quote stripping, no variable interpolation. Lines starting with # are
// comments; blank lines are skipped. Errors are silent — production runs are
// expected to inject env via docker-compose/systemd, not from a .env file.
func loadDotEnvIfPresent() {
	dir, err := os.Getwd()
	if err != nil {
		return
	}
	for {
		path := filepath.Join(dir, ".env")
		if f, err := os.Open(path); err == nil {
			applyDotEnv(f)
			f.Close()
			return
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return
		}
		dir = parent
	}
}

func applyDotEnv(r *os.File) {
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		eq := strings.IndexByte(line, '=')
		if eq <= 0 {
			continue
		}
		key := strings.TrimSpace(line[:eq])
		val := line[eq+1:]
		if _, exists := os.LookupEnv(key); exists {
			continue
		}
		os.Setenv(key, val)
	}
}

func intCSVEnv(key string) ([]int, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return nil, nil
	}
	parts := strings.Split(raw, ",")
	out := make([]int, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		n, err := strconv.Atoi(p)
		if err != nil {
			return nil, fmt.Errorf("%s must be a comma-separated list of integers (bad value %q)", key, p)
		}
		out = append(out, n)
	}
	return out, nil
}

func floatEnv(key string, fallback float64) (float64, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback, nil
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0, fmt.Errorf("%s must be a number (got %q)", key, raw)
	}
	return v, nil
}

func lowerCSV(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.ToLower(strings.TrimSpace(p))
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func validPS3838OddsFormat(s string) bool {
	switch s {
	case "American", "Decimal", "HongKong", "Indonesian", "Malay":
		return true
	}
	return false
}

func splitCSV(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}
