package main

import (
	"bytes"
	"context"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunDryRunDoesNotOpenDatabase(t *testing.T) {
	input := filepath.Join("..", "..", "testdata", "betfair", "market_cache_messages.jsonl")
	var out bytes.Buffer
	err := run(context.Background(), []string{
		"--input", input,
		"--format", "jsonl",
		"--database-url", "postgres://invalid.invalid:5432/nope?sslmode=disable",
	}, &out)
	if err != nil {
		t.Fatalf("dry-run replay returned error: %v", err)
	}
	got := out.String()
	for _, want := range []string{
		"dry_run=true",
		"write_mode=false",
		"messages_parsed=6",
		"candidates_emitted=8",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("dry-run output missing %q:\n%s", want, got)
		}
	}
}

func TestRunRequiresInputPath(t *testing.T) {
	var out bytes.Buffer
	err := run(context.Background(), nil, &out)
	if err == nil {
		t.Fatal("missing input returned nil error")
	}
	if !strings.Contains(err.Error(), "input path is required") {
		t.Fatalf("missing input error=%v", err)
	}
}

func TestRunRejectsUnsupportedFormat(t *testing.T) {
	input := filepath.Join("..", "..", "testdata", "betfair", "market_cache_messages.jsonl")
	var out bytes.Buffer
	err := run(context.Background(), []string{"--input", input, "--format", "xml"}, &out)
	if err == nil {
		t.Fatal("unsupported format returned nil error")
	}
	if !strings.Contains(err.Error(), "unsupported format") {
		t.Fatalf("unsupported format error=%v", err)
	}
}
