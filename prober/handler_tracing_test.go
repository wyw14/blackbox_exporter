// Copyright 2026 The Prometheus Authors
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

package prober

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/promslog"

	"github.com/prometheus/blackbox_exporter/config"
	"github.com/prometheus/blackbox_exporter/tracing"
)

// debugProbeLogConfig returns a promslog.Config that makes the scrape logger
// buffer DEBUG records so debug output contains the operation correlation.
func debugProbeLogConfig(t *testing.T) *promslog.Config {
	t.Helper()
	lvl := promslog.NewLevel()
	if err := lvl.Set("debug"); err != nil {
		t.Fatal(err)
	}
	return &promslog.Config{Level: lvl, Writer: io.Discard}
}

// httpFallbackConfig is an HTTP module that falls back between IP protocols,
// so probes work regardless of the test host's resolved address families.
var httpFallbackConfig = &config.Config{Modules: map[string]config.Module{
	"http_2xx": {
		Prober:  "http",
		Timeout: 5 * time.Second,
		HTTP:    config.HTTPProbe{IPProtocolFallback: true},
	},
}}

// withSampler temporarily installs sampler as the default tracing sampler.
func withSampler(t *testing.T, sampler tracing.Sampler) {
	t.Helper()
	previous := tracing.DefaultSampler()
	tracing.SetDefaultSampler(sampler)
	t.Cleanup(func() { tracing.SetDefaultSampler(previous) })
}

func serveProbe(t *testing.T, conf *config.Config, module, target, operationID string, rh *ResultHistory) *httptest.ResponseRecorder {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, "/probe", nil)
	if err != nil {
		t.Fatal(err)
	}
	q := req.URL.Query()
	q.Set("debug", "true")
	if module != "" {
		q.Set("module", module)
	}
	if target != "" {
		q.Set("target", target)
	}
	req.URL.RawQuery = q.Encode()
	if operationID != "" {
		req.Header.Set(tracing.HeaderOperationID, operationID)
	}
	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		Handler(w, r, conf, promslog.NewNopLogger(), rh, 0.5, nil, nil, debugProbeLogConfig(t))
	})
	handler.ServeHTTP(rr, req)
	return rr
}

func TestProbeCorrelatesInboundOperationIDInLogsAndMetrics(t *testing.T) {
	withSampler(t, tracing.AlwaysSample{})

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	rh := &ResultHistory{MaxResults: 10}
	const operationID = "inbound-op-001"
	rr := serveProbe(t, httpFallbackConfig, "", ts.URL, operationID, rh)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusOK)
	}
	body := rr.Body.String()
	// Logs emitted during the synchronous probe carry the operation ID.
	if !strings.Contains(body, "operation_id="+operationID) {
		t.Errorf("probe logs not correlated with operation id, body: %s", body)
	}
	// Metrics expose the same ID so logs and metrics aggregate per operation.
	if !strings.Contains(body, fmt.Sprintf("probe_operation_info{operation_id=%q} 1", operationID)) {
		t.Errorf("probe metrics not correlated with operation id, body: %s", body)
	}
	// The recorded result is correlated too.
	result := rh.GetByID(0)
	if result == nil {
		t.Fatal("probe result not recorded in history")
	}
	if result.OperationID != operationID {
		t.Errorf("history operation id = %q, want %q", result.OperationID, operationID)
	}
}

func TestProbeGeneratesOperationIDWithoutHeader(t *testing.T) {
	withSampler(t, tracing.AlwaysSample{})

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	rr := serveProbe(t, httpFallbackConfig, "", ts.URL, "", &ResultHistory{MaxResults: 10})
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusOK)
	}
	body := rr.Body.String()
	// A generated 16-byte hex ID must show up identically in logs and the
	// metric.
	logMarker := "operation_id="
	idx := strings.Index(body, logMarker)
	if idx < 0 {
		t.Fatalf("no generated operation id in probe logs, body: %s", body)
	}
	idEnd := idx + len(logMarker) + 32
	if idEnd > len(body) {
		t.Fatalf("generated operation id too short, body: %s", body)
	}
	generatedID := body[idx+len(logMarker) : idEnd]
	if !strings.Contains(body, fmt.Sprintf("probe_operation_info{operation_id=%q} 1", generatedID)) {
		t.Errorf("generated id %q missing from metrics, body: %s", generatedID, body)
	}
}

func TestProbeSamplingSwitchDoesNotChangeResult(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	const operationID = "sampling-neutral-op"

	withSampler(t, tracing.AlwaysSample{})
	sampled := serveProbe(t, httpFallbackConfig, "", ts.URL, operationID, &ResultHistory{MaxResults: 10}).Body.String()

	withSampler(t, tracing.NeverSample{})
	rh := &ResultHistory{MaxResults: 10}
	unsampled := serveProbe(t, httpFallbackConfig, "", ts.URL, operationID, rh).Body.String()

	// Business outcome is identical.
	for _, body := range []string{sampled, unsampled} {
		if !strings.Contains(body, "probe_success 1") {
			t.Errorf("expected successful probe, body: %s", body)
		}
	}
	// Sampled operation exposes correlation data.
	if !strings.Contains(sampled, "operation_id="+operationID) ||
		!strings.Contains(sampled, "probe_operation_info") {
		t.Errorf("sampled probe missing correlation data: %s", sampled)
	}
	// Unsampled operation exposes none of it.
	if strings.Contains(unsampled, "operation_id") ||
		strings.Contains(unsampled, "probe_operation_info") {
		t.Errorf("unsampled probe must not expose correlation data: %s", unsampled)
	}
	if result := rh.GetByID(0); result == nil || result.OperationID != "" {
		t.Errorf("unsampled result must record an empty operation id, got: %+v", result)
	}
}

func TestProbeFailureStillCorrelatedWithOperation(t *testing.T) {
	withSampler(t, tracing.AlwaysSample{})

	// A server that is closed forces the probe down the failure path.
	ts := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {}))
	ts.Close()

	rr := serveProbe(t, httpFallbackConfig, "", ts.URL, "failed-op", &ResultHistory{MaxResults: 10})
	body := rr.Body.String()
	if !strings.Contains(body, "probe_success 0") {
		t.Errorf("expected failed probe, body: %s", body)
	}
	if !strings.Contains(body, "operation_id=failed-op") {
		t.Errorf("failed probe logs must stay correlated with operation id, body: %s", body)
	}
	if !strings.Contains(body, "probe_operation_info{operation_id=\"failed-op\"} 1") {
		t.Errorf("failed probe metrics must stay correlated with operation id, body: %s", body)
	}
}

func TestProbeOperationPropagatesIntoTCPModule(t *testing.T) {
	withSampler(t, tracing.AlwaysSample{})

	conf := &config.Config{Modules: map[string]config.Module{
		"tcp_connect": {
			Prober:  "tcp",
			Timeout: 2 * time.Second,
			TCP:     config.TCPProbe{IPProtocol: "ip4"},
		},
	}}

	// Port 1 is virtually always closed/refused, so the probe fails fast but
	// still traverses chooseProtocol and dialTCP with the operation context.
	target := net.JoinHostPort("127.0.0.1", "1")
	rr := serveProbe(t, conf, "tcp_connect", target, "tcp-op", &ResultHistory{MaxResults: 10})
	body := rr.Body.String()
	if !strings.Contains(body, "probe_success 0") {
		t.Errorf("expected failed tcp probe, body: %s", body)
	}
	if !strings.Contains(body, "operation_id=tcp-op") {
		t.Errorf("tcp probe logs not correlated with operation id, body: %s", body)
	}
}

func TestChooseProtocolFallbackKeepsOperationAndAdvancesAttempt(t *testing.T) {
	withSampler(t, tracing.AlwaysSample{})

	ctx, op := tracing.Start(context.Background(), "fallback-op")
	var buf bytes.Buffer
	logger := tracing.LoggerWithOperation(ctx, slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))

	// "127.0.0.1" is a literal address: preferring ip6 finds no usable address
	// and the resolver falls back to ip4, exercising the retry path without
	// any network dependency.
	registry := prometheus.NewRegistry()
	ip, _, err := chooseProtocol(ctx, "ip6", true, "127.0.0.1", registry, logger)
	if err != nil {
		t.Fatalf("chooseProtocol returned unexpected error: %v", err)
	}
	if ip == nil || ip.IP.To4() == nil {
		t.Fatalf("expected fallback ipv4 address, got %v", ip)
	}

	out := buf.String()
	if !strings.Contains(out, "operation_id="+op.ID) {
		t.Errorf("fallback attempt lost operation id: %s", out)
	}
	if !strings.Contains(out, "attempt=2") {
		t.Errorf("fallback attempt should be logged as attempt 2: %s", out)
	}
	if strings.Count(out, "operation_id="+op.ID) < 2 {
		t.Errorf("expected both fallback log lines under one operation id: %s", out)
	}

	// The derived context used by the retry branch reports attempt 2 while
	// the operation stays the same.
	if got := tracing.OperationID(tracing.WithAttempt(ctx, 2)); got != op.ID {
		t.Fatalf("retry branch changed operation id: %q", got)
	}
}

func TestProbeHandlerConcurrentOperations(t *testing.T) {
	withSampler(t, tracing.AlwaysSample{})

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	const n = 50
	rh := &ResultHistory{MaxResults: n}
	var wg sync.WaitGroup
	statuses := make([]int, n)
	for i := range n {
		wg.Go(func() {
			operationID := fmt.Sprintf("concurrent-op-%03d", i)
			rr := serveProbe(t, httpFallbackConfig, "", ts.URL, operationID, rh)
			statuses[i] = rr.Code
		})
	}
	wg.Wait()

	for i, status := range statuses {
		if status != http.StatusOK {
			t.Errorf("probe %d status = %d, want %d", i, status, http.StatusOK)
		}
	}

	results := rh.List()
	if len(results) != n {
		t.Fatalf("history contains %d results, want %d", len(results), n)
	}
	seen := make(map[string]int, n)
	for _, result := range results {
		if result.OperationID == "" {
			t.Errorf("result %d missing operation id", result.ID)
			continue
		}
		seen[result.OperationID]++
	}
	if len(seen) != n {
		t.Errorf("observed %d distinct operation ids, want %d", len(seen), n)
	}
	for id, count := range seen {
		if count != 1 {
			t.Errorf("operation id %q recorded %d times, want exactly once", id, count)
		}
	}
}

func TestProbeHandlerExceptionPathsAreSamplingNeutral(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	cases := []struct {
		name       string
		target     string
		module     string
		header     string
		headerVal  string
		wantStatus int
	}{
		{"unknown module", ts.URL, "does_not_exist", "", "", http.StatusBadRequest},
		{"missing target", "", "http_2xx", "", "", http.StatusBadRequest},
		{"bad timeout header", ts.URL, "http_2xx", "X-Prometheus-Scrape-Timeout-Seconds", "not-a-float", http.StatusInternalServerError},
	}

	for _, sampler := range []struct {
		name    string
		sampler tracing.Sampler
	}{
		{"sampled", tracing.AlwaysSample{}},
		{"unsampled", tracing.NeverSample{}},
	} {
		t.Run(sampler.name, func(t *testing.T) {
			withSampler(t, sampler.sampler)
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					req, err := http.NewRequest(http.MethodGet, "/probe", nil)
					if err != nil {
						t.Fatal(err)
					}
					q := req.URL.Query()
					if tc.target != "" {
						q.Set("target", tc.target)
					}
					q.Set("module", tc.module)
					req.URL.RawQuery = q.Encode()
					if tc.header != "" {
						req.Header.Set(tc.header, tc.headerVal)
					}
					rr := httptest.NewRecorder()
					http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						Handler(w, r, c, promslog.NewNopLogger(), &ResultHistory{MaxResults: 10}, 0.5, nil, nil, debugProbeLogConfig(t))
					}).ServeHTTP(rr, req)
					if rr.Code != tc.wantStatus {
						t.Errorf("status = %d, want %d", rr.Code, tc.wantStatus)
					}
				})
			}
		})
	}
}
