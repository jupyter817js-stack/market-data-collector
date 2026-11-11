// cmd/lsports-distribution toggles the LSPORTS distributor for our
// package. The distributor must be activated BEFORE either AMQP consumers
// or Snapshot polling can receive real fixture data — without it, AMQP
// returns 403 and Snapshot returns empty Body. This is the critical
// missing step that silently broke the first trial match.
//
// Usage:
//
//	# Activate (run once at start of each trial day):
//	go run ./cmd/lsports-distribution --start
//
//	# Deactivate (operator-only; usually not needed during the trial):
//	go run ./cmd/lsports-distribution --stop
//
// Quota cost: 0. Calling Start when already-on returns
// "Value was already set" rather than an error.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/your-org/pol-odds-collector/internal/config"
	"github.com/your-org/pol-odds-collector/internal/source/lsports"
)

func main() {
	var (
		start = flag.Bool("start", false, "call POST /Distribution/Start (activate the distributor)")
		stop  = flag.Bool("stop", false, "call POST /Distribution/Stop (deactivate the distributor)")
	)
	flag.Parse()
	_, _ = config.Load()

	if *start == *stop {
		fail("exactly one of --start or --stop is required")
	}

	base := envOr("LSPORTS_BASE_URL", lsports.DefaultBaseURL)
	user := strings.TrimSpace(os.Getenv("LSPORTS_USERNAME"))
	pw := os.Getenv("LSPORTS_PASSWORD")
	pkgStr := envOr("LSPORTS_INPLAY_PACKAGE_ID", "4044")

	pkg, err := strconv.Atoi(pkgStr)
	if err != nil {
		fail("LSPORTS_INPLAY_PACKAGE_ID must be integer: %v", err)
	}
	if user == "" || pw == "" {
		fail("LSPORTS_USERNAME and LSPORTS_PASSWORD required")
	}

	client := lsports.NewClient(base, lsports.Credentials{
		PackageID: pkg,
		Username:  user,
		Password:  pw,
	}, 45*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var (
		action string
		resp   lsports.DistributionResponse
	)
	if *start {
		action = "/Distribution/Start"
		resp, err = client.StartDistribution(ctx)
	} else {
		action = "/Distribution/Stop"
		resp, err = client.StopDistribution(ctx)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s FAILED: %v\n", action, err)
		fmt.Fprintf(os.Stderr, "raw: %s\n", string(resp.Raw))
		os.Exit(2)
	}
	fmt.Printf("%s OK\n", action)
	fmt.Printf("  response body: %s\n", resp.Body)
	fmt.Printf("  raw bytes    : %s\n", string(resp.Raw))
}

func envOr(key, fallback string) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	return v
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
