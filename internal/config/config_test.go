package config

import (
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/your-org/pol-odds-collector/internal/source/oddsapi"
)

// As of 2026-05-17 ODDS_API_KEY is optional at Load() time so that
// pinnacle-collector and polymarket-sync can run without it. The legacy
// Odds-API path validates via MustOddsAPIConfig.
func TestLoadWithoutOddsAPIKeySucceeds(t *testing.T) {
	clearConfigEnv(t)
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() unexpected error: %v", err)
	}
	if cfg.OddsAPIKey != "" {
		t.Fatalf("OddsAPIKey=%q want empty", cfg.OddsAPIKey)
	}
}

func TestMustOddsAPIConfigRejectsMissingKey(t *testing.T) {
	clearConfigEnv(t)
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() unexpected error: %v", err)
	}
	err = MustOddsAPIConfig(cfg)
	assertErrorContains(t, err, "ODDS_API_KEY")
}

func TestMustOddsAPIConfigRejectsReplaceMe(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("ODDS_API_KEY", "replace_me")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() unexpected error: %v", err)
	}
	err = MustOddsAPIConfig(cfg)
	assertErrorContains(t, err, "ODDS_API_KEY")
	assertErrorDoesNotContain(t, err, "replace_me")
}

func TestLoadValidMinimalConfigSucceeds(t *testing.T) {
	setMinimalEnv(t)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if cfg.OddsAPIKey != "test-key" {
		t.Fatalf("OddsAPIKey=%q want test-key", cfg.OddsAPIKey)
	}
	if cfg.OddsAPIBaseURL != "https://api.the-odds-api.com" {
		t.Fatalf("OddsAPIBaseURL=%q", cfg.OddsAPIBaseURL)
	}
	if cfg.PolymarketGammaBaseURL != "https://gamma-api.polymarket.com" {
		t.Fatalf("PolymarketGammaBaseURL=%q", cfg.PolymarketGammaBaseURL)
	}
	if cfg.PolymarketCLOBBaseURL != "https://clob.polymarket.com" {
		t.Fatalf("PolymarketCLOBBaseURL=%q", cfg.PolymarketCLOBBaseURL)
	}
	if got, want := cfg.SportKeys, []string{"basketball_nba"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("SportKeys=%v want %v", got, want)
	}
	if cfg.Regions != "us" {
		t.Fatalf("Regions=%q want us", cfg.Regions)
	}
	if cfg.Markets != "h2h,spreads,totals" {
		t.Fatalf("Markets=%q want h2h,spreads,totals", cfg.Markets)
	}
	if cfg.PollInterval != time.Minute {
		t.Fatalf("PollInterval=%s want 1m", cfg.PollInterval)
	}
	if cfg.MaxConcurrentSports != 2 {
		t.Fatalf("MaxConcurrentSports=%d want 2", cfg.MaxConcurrentSports)
	}
	if cfg.HTTPTimeout != 15*time.Second {
		t.Fatalf("HTTPTimeout=%s want 15s", cfg.HTTPTimeout)
	}
	if cfg.HealthAddr != ":8080" {
		t.Fatalf("HealthAddr=%q want :8080", cfg.HealthAddr)
	}
	if !cfg.PrematchOnly {
		t.Fatal("PrematchOnly=false want true")
	}
}

func TestLoadHealthAddrEnv(t *testing.T) {
	setMinimalEnv(t)
	t.Setenv("HEALTH_ADDR", "127.0.0.1:9090")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if cfg.HealthAddr != "127.0.0.1:9090" {
		t.Fatalf("HealthAddr=%q want 127.0.0.1:9090", cfg.HealthAddr)
	}
}

func TestLoadPolymarketGammaBaseURLEnv(t *testing.T) {
	setMinimalEnv(t)
	t.Setenv("POLYMARKET_GAMMA_BASE_URL", "http://127.0.0.1:9999")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if cfg.PolymarketGammaBaseURL != "http://127.0.0.1:9999" {
		t.Fatalf("PolymarketGammaBaseURL=%q want http://127.0.0.1:9999", cfg.PolymarketGammaBaseURL)
	}
}

func TestLoadPolymarketCLOBBaseURLEnv(t *testing.T) {
	setMinimalEnv(t)
	t.Setenv("POLYMARKET_CLOB_BASE_URL", "http://127.0.0.1:9998")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if cfg.PolymarketCLOBBaseURL != "http://127.0.0.1:9998" {
		t.Fatalf("PolymarketCLOBBaseURL=%q want http://127.0.0.1:9998", cfg.PolymarketCLOBBaseURL)
	}
}

func TestLoadMaxConcurrentSportsEnv(t *testing.T) {
	setMinimalEnv(t)
	t.Setenv("MAX_CONCURRENT_SPORTS", "4")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if cfg.MaxConcurrentSports != 4 {
		t.Fatalf("MaxConcurrentSports=%d want 4", cfg.MaxConcurrentSports)
	}
}

func TestLoadSportKeysCSVTrimming(t *testing.T) {
	setMinimalEnv(t)
	t.Setenv("SPORT_KEYS", " basketball_nba, soccer_epl , ,tennis_atp ")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	want := []string{"basketball_nba", "soccer_epl", "tennis_atp"}
	if !reflect.DeepEqual(cfg.SportKeys, want) {
		t.Fatalf("SportKeys=%v want %v", cfg.SportKeys, want)
	}
}

func TestLoadBookmakersCanTakePriorityAtSourceConstruction(t *testing.T) {
	setMinimalEnv(t)
	t.Setenv("REGIONS", "us")
	t.Setenv("BOOKMAKERS", "draftkings,fanduel")
	t.Setenv("ODDS_API_RETRY_BASE_MS", "1")
	t.Setenv("ODDS_API_RETRY_MAX_MS", "1")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}

	var gotQuery string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`[]`))
	}))
	defer server.Close()

	client := oddsapi.NewClientWithRetry(
		cfg.OddsAPIKey,
		server.URL,
		cfg.Regions,
		cfg.Markets,
		cfg.Bookmakers,
		cfg.HTTPTimeout,
		oddsapi.RetryConfig{
			MaxRetries: cfg.OddsAPIMaxRetries,
			BaseDelay:  cfg.OddsAPIRetryBase,
			MaxDelay:   cfg.OddsAPIRetryMax,
		},
	)

	if _, err := client.FetchOdds(context.Background(), "basketball_nba"); err != nil {
		t.Fatalf("FetchOdds returned error: %v", err)
	}
	if !strings.Contains(gotQuery, "bookmakers=draftkings%2Cfanduel") {
		t.Fatalf("query=%q missing bookmakers", gotQuery)
	}
	if strings.Contains(gotQuery, "regions=") {
		t.Fatalf("query=%q should not include regions when bookmakers is set", gotQuery)
	}
}

func TestLoadAgingBeforeFreshFails(t *testing.T) {
	setMinimalEnv(t)
	t.Setenv("FRESH_AFTER_SECONDS", "300")
	t.Setenv("AGING_AFTER_SECONDS", "120")

	_, err := Load()
	assertErrorContains(t, err, "AGING_AFTER_SECONDS")
}

func TestLoadRetryDefaults(t *testing.T) {
	setMinimalEnv(t)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if cfg.OddsAPIMaxRetries != 2 {
		t.Fatalf("OddsAPIMaxRetries=%d want 2", cfg.OddsAPIMaxRetries)
	}
	if cfg.OddsAPIRetryBase != 250*time.Millisecond {
		t.Fatalf("OddsAPIRetryBase=%s want 250ms", cfg.OddsAPIRetryBase)
	}
	if cfg.OddsAPIRetryMax != 2*time.Second {
		t.Fatalf("OddsAPIRetryMax=%s want 2s", cfg.OddsAPIRetryMax)
	}
}

func TestLoadRetryEnv(t *testing.T) {
	setMinimalEnv(t)
	t.Setenv("ODDS_API_MAX_RETRIES", "4")
	t.Setenv("ODDS_API_RETRY_BASE_MS", "100")
	t.Setenv("ODDS_API_RETRY_MAX_MS", "750")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if cfg.OddsAPIMaxRetries != 4 {
		t.Fatalf("OddsAPIMaxRetries=%d want 4", cfg.OddsAPIMaxRetries)
	}
	if cfg.OddsAPIRetryBase != 100*time.Millisecond {
		t.Fatalf("OddsAPIRetryBase=%s want 100ms", cfg.OddsAPIRetryBase)
	}
	if cfg.OddsAPIRetryMax != 750*time.Millisecond {
		t.Fatalf("OddsAPIRetryMax=%s want 750ms", cfg.OddsAPIRetryMax)
	}
}

func TestLoadRetryValidation(t *testing.T) {
	tests := []struct {
		name    string
		key     string
		value   string
		wantErr string
	}{
		{"invalid max retries", "ODDS_API_MAX_RETRIES", "-1", "ODDS_API_MAX_RETRIES"},
		{"invalid retry base", "ODDS_API_RETRY_BASE_MS", "bad", "ODDS_API_RETRY_BASE_MS"},
		{"invalid retry max", "ODDS_API_RETRY_MAX_MS", "0", "ODDS_API_RETRY_MAX_MS"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			setMinimalEnv(t)
			t.Setenv(tc.key, tc.value)

			_, err := Load()
			assertErrorContains(t, err, tc.wantErr)
		})
	}
}

func TestLoadRejectsRetryMaxBelowBase(t *testing.T) {
	setMinimalEnv(t)
	t.Setenv("ODDS_API_RETRY_BASE_MS", "1000")
	t.Setenv("ODDS_API_RETRY_MAX_MS", "100")

	_, err := Load()
	assertErrorContains(t, err, "ODDS_API_RETRY_MAX_MS")
}

func TestLoadNumericDurationValidation(t *testing.T) {
	tests := []struct {
		name    string
		key     string
		value   string
		wantErr string
	}{
		{"poll interval text", "POLL_INTERVAL_SECONDS", "soon", "POLL_INTERVAL_SECONDS"},
		{"max concurrent sports zero", "MAX_CONCURRENT_SPORTS", "0", "MAX_CONCURRENT_SPORTS"},
		{"http timeout zero", "HTTP_TIMEOUT_SECONDS", "0", "HTTP_TIMEOUT_SECONDS"},
		{"fresh after negative", "FRESH_AFTER_SECONDS", "-5", "FRESH_AFTER_SECONDS"},
		{"aging after text", "AGING_AFTER_SECONDS", "later", "AGING_AFTER_SECONDS"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			setMinimalEnv(t)
			t.Setenv(tc.key, tc.value)

			_, err := Load()
			assertErrorContains(t, err, tc.wantErr)
		})
	}
}

func TestLoadBoolValidation(t *testing.T) {
	setMinimalEnv(t)
	t.Setenv("PREMATCH_ONLY", "sometimes")

	_, err := Load()
	assertErrorContains(t, err, "PREMATCH_ONLY")
}

func TestLoadErrorsDoNotExposeOddsAPIKey(t *testing.T) {
	setMinimalEnv(t)
	t.Setenv("ODDS_API_KEY", "super-secret-key")
	t.Setenv("POLL_INTERVAL_SECONDS", "bad")

	_, err := Load()
	assertErrorContains(t, err, "POLL_INTERVAL_SECONDS")
	assertErrorDoesNotContain(t, err, "super-secret-key")
}

func setMinimalEnv(t *testing.T) {
	t.Helper()
	clearConfigEnv(t)
	t.Setenv("ODDS_API_KEY", "test-key")
}

func clearConfigEnv(t *testing.T) {
	t.Helper()
	keys := []string{
		"ODDS_API_KEY",
		"ODDS_API_BASE_URL",
		"ODDS_API_MAX_RETRIES",
		"ODDS_API_RETRY_BASE_MS",
		"ODDS_API_RETRY_MAX_MS",
		"POLYMARKET_GAMMA_BASE_URL",
		"POLYMARKET_CLOB_BASE_URL",
		"DATABASE_URL",
		"SPORT_KEYS",
		"REGIONS",
		"MARKETS",
		"BOOKMAKERS",
		"POLL_INTERVAL_SECONDS",
		"MAX_CONCURRENT_SPORTS",
		"HTTP_TIMEOUT_SECONDS",
		"HEALTH_ADDR",
		"PREMATCH_ONLY",
		"FRESH_AFTER_SECONDS",
		"AGING_AFTER_SECONDS",
	}
	for _, key := range keys {
		t.Setenv(key, "")
	}
}

func assertErrorContains(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil {
		t.Fatalf("Load returned nil error, want %q", want)
	}
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("error=%q want substring %q", err.Error(), want)
	}
}

func assertErrorDoesNotContain(t *testing.T, err error, forbidden string) {
	t.Helper()
	if err == nil {
		return
	}
	if strings.Contains(err.Error(), forbidden) {
		t.Fatalf("error=%q should not contain %q", err.Error(), forbidden)
	}
}
