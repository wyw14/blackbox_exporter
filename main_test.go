// Copyright 2016 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/prometheus/blackbox_exporter/config"
	"github.com/prometheus/blackbox_exporter/tracing"
)

const goodConfigContents = `modules:
  http_2xx:
    prober: http
    timeout: 5s
`

func writeConfigFile(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "blackbox.yml")
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func newTestSafeConfig() *config.SafeConfig {
	return config.NewSafeConfig(prometheus.NewRegistry())
}

func bufferLogger() (*slog.Logger, *bytes.Buffer) {
	var buf bytes.Buffer
	return slog.New(slog.NewTextHandler(&buf, nil)), &buf
}

func TestComputeExternalURL(t *testing.T) {
	tests := []struct {
		input string
		valid bool
	}{
		{
			input: "",
			valid: true,
		},
		{
			input: "http://proxy.com/prometheus",
			valid: true,
		},
		{
			input: "'https://url/prometheus'",
			valid: false,
		},
		{
			input: "'relative/path/with/quotes'",
			valid: false,
		},
		{
			input: "http://alertmanager.company.com",
			valid: true,
		},
		{
			input: "https://double--dash.de",
			valid: true,
		},
		{
			input: "'http://starts/with/quote",
			valid: false,
		},
		{
			input: "ends/with/quote\"",
			valid: false,
		},
	}

	for _, test := range tests {
		_, err := computeExternalURL(test.input, "0.0.0.0:9090")
		if test.valid {
			if err != nil {
				t.Errorf("unexpected error %v", err)
			}
		} else {
			if err == nil {
				t.Errorf("expected error computing %s got none", test.input)
			}
		}
	}
}

func TestReloadConfigCorrelatesOperationOnSuccess(t *testing.T) {
	tracing.SetDefaultSampler(tracing.AlwaysSample{})
	t.Cleanup(func() { tracing.SetDefaultSampler(tracing.AlwaysSample{}) })

	ctx, op := tracing.Start(context.Background(), "reload-success-op")
	logger, buf := bufferLogger()

	if err := reloadConfig(ctx, newTestSafeConfig(), writeConfigFile(t, goodConfigContents), logger); err != nil {
		t.Fatalf("reloadConfig returned unexpected error: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "operation_id="+op.ID) {
		t.Errorf("successful reload not correlated with operation id: %s", out)
	}
	if !strings.Contains(out, "attempt=1") {
		t.Errorf("successful reload should be logged as attempt 1: %s", out)
	}
	if !strings.Contains(out, "Reloaded config file") {
		t.Errorf("missing success log: %s", out)
	}
}

func TestReloadConfigRetriesShareOperationID(t *testing.T) {
	tracing.SetDefaultSampler(tracing.AlwaysSample{})
	t.Cleanup(func() { tracing.SetDefaultSampler(tracing.AlwaysSample{}) })

	ctx, op := tracing.Start(context.Background(), "reload-retry-op")
	logger, buf := bufferLogger()
	missingFile := filepath.Join(t.TempDir(), "does-not-exist.yml")

	if err := reloadConfig(ctx, newTestSafeConfig(), missingFile, logger); err == nil {
		t.Fatal("reloadConfig with missing file must return an error")
	}
	out := buf.String()
	if count := strings.Count(out, "Error reloading config"); count != configReloadAttempts {
		t.Errorf("reload attempted %d times, want %d. Logs: %s", count, configReloadAttempts, out)
	}
	for attempt := 1; attempt <= configReloadAttempts; attempt++ {
		if !strings.Contains(out, "attempt="+strconv.Itoa(attempt)) {
			t.Errorf("retry logs missing attempt %d: %s", attempt, out)
		}
	}
	if count := strings.Count(out, "operation_id="+op.ID); count != configReloadAttempts {
		t.Errorf("operation id appeared %d times, want %d (once per attempt). Logs: %s", count, configReloadAttempts, out)
	}
}

func TestReloadConfigSamplingOffDoesNotChangeRetryBehavior(t *testing.T) {
	tracing.SetDefaultSampler(tracing.NeverSample{})
	t.Cleanup(func() { tracing.SetDefaultSampler(tracing.AlwaysSample{}) })

	ctx, _ := tracing.Start(context.Background(), "reload-unsampled-op")
	logger, buf := bufferLogger()
	missingFile := filepath.Join(t.TempDir(), "does-not-exist.yml")

	if err := reloadConfig(ctx, newTestSafeConfig(), missingFile, logger); err == nil {
		t.Fatal("reloadConfig with missing file must return an error")
	}
	out := buf.String()
	// All retries still run when the operation is not sampled.
	if count := strings.Count(out, "Error reloading config"); count != configReloadAttempts {
		t.Errorf("reload attempted %d times, want %d. Logs: %s", count, configReloadAttempts, out)
	}
	// The operation ID is simply absent from the logs.
	if strings.Contains(out, "operation_id") {
		t.Errorf("unsampled reload must not log the operation id: %s", out)
	}
}

func TestHandleReloadRequestDetachesCancelledRequestContext(t *testing.T) {
	tracing.SetDefaultSampler(tracing.AlwaysSample{})
	t.Cleanup(func() { tracing.SetDefaultSampler(tracing.AlwaysSample{}) })

	ctx, op := tracing.Start(context.Background(), "http-reload-op")
	wrapped, cancel := context.WithCancel(ctx)
	// Simulate the HTTP client going away before the background worker runs.
	cancel()

	req := reloadRequest{ctx: wrapped, rc: make(chan error, 1)}
	logger, buf := bufferLogger()
	// The detached reload must succeed despite the cancelled request context.
	if err := handleReloadRequest(req, newTestSafeConfig(), writeConfigFile(t, goodConfigContents), logger); err != nil {
		t.Fatalf("detached reload failed after request cancellation: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "operation_id="+op.ID) {
		t.Errorf("asynchronous reload lost the inbound operation id: %s", out)
	}
	// The result must be deliverable to the (buffered) response channel even
	// though nobody is reading it yet.
	req.rc <- nil
}

func TestHandleReloadRequestPropagatesFailure(t *testing.T) {
	ctx, _ := tracing.Start(context.Background(), "http-reload-fail-op")
	req := reloadRequest{ctx: ctx, rc: make(chan error, 1)}
	logger, _ := bufferLogger()
	missingFile := filepath.Join(t.TempDir(), "does-not-exist.yml")
	if err := handleReloadRequest(req, newTestSafeConfig(), missingFile, logger); err == nil {
		t.Fatal("expected reload error for a missing file")
	}
}

func TestHandleReloadRequestConcurrentOperationsStayCorrelated(t *testing.T) {
	tracing.SetDefaultSampler(tracing.AlwaysSample{})
	t.Cleanup(func() { tracing.SetDefaultSampler(tracing.AlwaysSample{}) })

	configFile := writeConfigFile(t, goodConfigContents)

	const n = 30
	var wg sync.WaitGroup
	buffers := make([]*bytes.Buffer, n)
	for i := range n {
		wg.Go(func() {
			operationID := "concurrent-reload-op-" + strconv.Itoa(i)
			ctx, _ := tracing.Start(context.Background(), operationID)
			req := reloadRequest{ctx: ctx, rc: make(chan error, 1)}
			logger, buf := bufferLogger()
			buffers[i] = buf
			// Every reload uses its own SafeConfig, mirroring independent
			// operations; only the config file is shared.
			if err := handleReloadRequest(req, newTestSafeConfig(), configFile, logger); err != nil {
				t.Errorf("reload %d failed: %v", i, err)
			}
		})
	}
	wg.Wait()

	for i := range n {
		ownID := "concurrent-reload-op-" + strconv.Itoa(i)
		out := buffers[i].String()
		// A word boundary keeps op-1 from matching op-10.
		if !regexp.MustCompile(`operation_id=` + regexp.QuoteMeta(ownID) + `\b`).MatchString(out) {
			t.Errorf("reload %d missing its own operation id %q: %s", i, ownID, out)
		}
		for j := range n {
			if j == i {
				continue
			}
			otherID := "concurrent-reload-op-" + strconv.Itoa(j)
			if regexp.MustCompile(`operation_id=` + regexp.QuoteMeta(otherID) + `\b`).MatchString(out) {
				t.Errorf("reload %d leaked correlation from operation %q: %s", i, otherID, out)
			}
		}
	}
}
