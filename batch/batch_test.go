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

package batch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.yaml.in/yaml/v3"

	"github.com/prometheus/blackbox_exporter/config"
)

// modulesPayload builds a legacy YAML payload where every module uses the
// given prober.
func modulesPayload(modules map[string]string) []byte {
	body := map[string]any{"modules": map[string]any{}}
	mods := body["modules"].(map[string]any)
	for name, prober := range modules {
		mods[name] = map[string]any{"prober": prober}
	}
	data, err := yaml.Marshal(body)
	if err != nil {
		panic(err)
	}
	return data
}

func waitTerminal(t *testing.T, m *Manager, id string) *JobStatus {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	status, err := m.Wait(ctx, id)
	if err != nil {
		t.Fatalf("waiting for job %s: %v", id, err)
	}
	return status
}

func waitFor(t *testing.T, condition func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", msg)
}

// countingStore records how many times Put is invoked.
type countingStore struct {
	inner ModuleStore
	puts  atomic.Int64
}

func (s *countingStore) Has(name string) bool { return s.inner.Has(name) }
func (s *countingStore) Get(name string) (config.Module, bool) {
	return s.inner.Get(name)
}

func (s *countingStore) Put(name string, module config.Module) {
	s.puts.Add(1)
	s.inner.Put(name, module)
}
func (s *countingStore) List() []string { return s.inner.List() }

func TestParseLegacyAndVersionedEnvelope(t *testing.T) {
	legacy := []byte(`
modules:
  http_2xx:
    prober: http
  tcp_connect:
    prober: tcp
`)
	parsed, err := ParseEnvelope(legacy, FormatYAML)
	if err != nil {
		t.Fatalf("legacy payload: %v", err)
	}
	if parsed.Version != 0 {
		t.Errorf("legacy version = %d, want 0", parsed.Version)
	}
	if len(parsed.Modules) != 2 {
		t.Fatalf("parsed %d modules, want 2", len(parsed.Modules))
	}

	versioned := []byte(`
version: 1
kind: modules
modules:
  http_2xx:
    prober: http
`)
	parsed, err = ParseEnvelope(versioned, FormatYAML)
	if err != nil {
		t.Fatalf("versioned payload: %v", err)
	}
	if parsed.Version != 1 {
		t.Errorf("version = %d, want 1", parsed.Version)
	}

	for name, payload := range map[string]string{
		"unknown field":   "version: 1\nkind: modules\nmodules: {}\nfrobnicate: true\n",
		"unknown version": "version: 2\nkind: modules\nmodules: {}\n",
		"unknown kind":    "version: 1\nkind: targets\nmodules: {}\n",
		"missing modules": "version: 1\nkind: modules\n",
		"broken yaml":     "modules: [\n",
	} {
		if _, err := ParseEnvelope([]byte(payload), FormatYAML); err == nil {
			t.Errorf("expected error for %s", name)
		}
	}

	// Duplicate module names inside one payload are surfaced, not dropped.
	dup := []byte(`
modules:
  same:
    prober: tcp
  same:
    prober: http
`)
	parsed, err = ParseEnvelope(dup, FormatYAML)
	if err != nil {
		t.Fatalf("duplicate payload: %v", err)
	}
	if len(parsed.Modules) != 1 || len(parsed.DuplicateNames) != 1 || parsed.DuplicateNames[0] != "same" {
		t.Fatalf("unexpected duplicate handling: %+v", parsed)
	}
}

func TestParseJSONEnvelope(t *testing.T) {
	payload := []byte(`{"version":1,"kind":"modules","modules":{"j1":{"prober":"tcp"}}}`)
	parsed, err := ParseEnvelope(payload, FormatJSON)
	if err != nil {
		t.Fatalf("json payload: %v", err)
	}
	if len(parsed.Modules) != 1 || parsed.Modules[0].Name != "j1" {
		t.Fatalf("unexpected parse result: %+v", parsed)
	}
	if _, err := ValidateModule(parsed.Modules[0].Raw); err != nil {
		t.Fatalf("json module validation: %v", err)
	}
}

func TestLegacyExportStaysReadable(t *testing.T) {
	store := NewMemoryStore()
	m, err := NewManager(store)
	if err != nil {
		t.Fatal(err)
	}

	status, _, err := m.SubmitImport(ImportRequest{
		Payload: modulesPayload(map[string]string{"http_2xx": "http", "tcp_connect": "tcp"}),
	})
	if err != nil {
		t.Fatal(err)
	}
	waitTerminal(t, m, status.ID)

	data, err := m.Export(nil, false)
	if err != nil {
		t.Fatal(err)
	}

	// The export must be loadable through the strict historical loader.
	var legacy config.Config
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(&legacy); err != nil {
		t.Fatalf("exported legacy format not readable by the old loader: %v\n%s", err, data)
	}
	if _, ok := legacy.Modules["http_2xx"]; !ok {
		t.Errorf("exported data missing http_2xx:\n%s", data)
	}

	// And it must round-trip through the batch importer.
	if _, err := ParseEnvelope(data, FormatYAML); err != nil {
		t.Fatalf("legacy export not re-importable: %v", err)
	}

	versioned, err := m.Export(nil, true)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := ParseEnvelope(versioned, FormatYAML)
	if err != nil {
		t.Fatalf("versioned export not importable: %v", err)
	}
	if parsed.Version != FormatVersion {
		t.Errorf("versioned export version = %d, want %d", parsed.Version, FormatVersion)
	}
}

func TestImportValidationAndPartialSuccess(t *testing.T) {
	store := NewMemoryStore()
	m, err := NewManager(store)
	if err != nil {
		t.Fatal(err)
	}

	payload := []byte(`
modules:
  good_tcp:
    prober: tcp
  bad_prober:
    prober: ping
  bad_dns:
    prober: dns
    dns: {}
  good_http:
    prober: http
`)
	status, _, err := m.SubmitImport(ImportRequest{Payload: payload})
	if err != nil {
		t.Fatal(err)
	}
	status = waitTerminal(t, m, status.ID)

	if status.State != StateCompletedWithErrors {
		t.Fatalf("state = %s, want %s", status.State, StateCompletedWithErrors)
	}
	if status.Total != 4 || status.Succeeded != 2 || status.Failed != 2 || status.Processed != 4 {
		t.Fatalf("unexpected counters: %+v", status)
	}

	byName := map[string]ItemStatus{}
	for _, result := range status.Results {
		byName[result.Name] = result
	}
	for _, good := range []string{"good_tcp", "good_http"} {
		if byName[good].Status != ItemApplied {
			t.Errorf("module %s: %+v", good, byName[good])
		}
		if !store.Has(good) {
			t.Errorf("valid module %s was not stored", good)
		}
	}
	for _, bad := range []string{"bad_prober", "bad_dns"} {
		if byName[bad].Status != ItemFailed || byName[bad].Error == "" {
			t.Errorf("module %s should be reported failed with an error, got %+v", bad, byName[bad])
		}
		if store.Has(bad) {
			t.Errorf("invalid module %s was stored", bad)
		}
	}
}

func TestImportDuplicateNameInBatchIsPartialFailure(t *testing.T) {
	store := NewMemoryStore()
	m, err := NewManager(store)
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte(`
modules:
  dup:
    prober: tcp
  other:
    prober: tcp
  dup:
    prober: http
`)
	status, _, err := m.SubmitImport(ImportRequest{Payload: payload})
	if err != nil {
		t.Fatal(err)
	}
	status = waitTerminal(t, m, status.ID)
	if status.State != StateCompletedWithErrors || status.Succeeded != 2 || status.Failed != 1 {
		t.Fatalf("unexpected status: %+v", status)
	}
}

func TestImportModes(t *testing.T) {
	store := NewMemoryStore()
	m, err := NewManager(store)
	if err != nil {
		t.Fatal(err)
	}

	first := modulesPayload(map[string]string{"m": "tcp"})

	create, _, err := m.SubmitImport(ImportRequest{Payload: first, Mode: ModeCreate})
	if err != nil {
		t.Fatal(err)
	}
	if s := waitTerminal(t, m, create.ID); s.State != StateCompleted {
		t.Fatalf("create new: state = %s", s.State)
	}

	createAgain, _, err := m.SubmitImport(ImportRequest{Payload: first, Mode: ModeCreate})
	if err != nil {
		t.Fatal(err)
	}
	if s := waitTerminal(t, m, createAgain.ID); s.State != StateCompletedWithErrors {
		t.Fatalf("create existing: state = %s, want partial failure", s.State)
	}

	updateMissing, _, err := m.SubmitImport(ImportRequest{
		Payload: modulesPayload(map[string]string{"missing": "tcp"}),
		Mode:    ModeUpdate,
	})
	if err != nil {
		t.Fatal(err)
	}
	if s := waitTerminal(t, m, updateMissing.ID); s.State != StateCompletedWithErrors {
		t.Fatalf("update missing: state = %s, want partial failure", s.State)
	}

	updateExisting, _, err := m.SubmitImport(ImportRequest{Payload: first, Mode: ModeUpdate})
	if err != nil {
		t.Fatal(err)
	}
	if s := waitTerminal(t, m, updateExisting.ID); s.State != StateCompleted {
		t.Fatalf("update existing: state = %s", s.State)
	}

	// Invalid mode is rejected before creating a job.
	if _, _, err := m.SubmitImport(ImportRequest{Payload: first, Mode: "nope"}); err == nil {
		t.Fatal("expected error for invalid mode")
	}
}

func TestEmptyAndInvalidRequestsRejected(t *testing.T) {
	m, err := NewManager(NewMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	for _, payload := range [][]byte{nil, []byte(""), []byte("modules: {}\n")} {
		if _, _, err := m.SubmitImport(ImportRequest{Payload: payload}); err == nil {
			t.Errorf("expected rejection for payload %q", payload)
		}
	}
}

func TestIdempotentRetryAppliesModulesOnce(t *testing.T) {
	store := &countingStore{inner: NewMemoryStore()}
	m, err := NewManager(store)
	if err != nil {
		t.Fatal(err)
	}

	payload := modulesPayload(map[string]string{"a": "tcp", "b": "tcp", "c": "tcp"})
	const key = "request-1"

	first, duplicated, err := m.SubmitImport(ImportRequest{Payload: payload, IdempotencyKey: key})
	if err != nil || duplicated {
		t.Fatalf("first submit: duplicated=%v err=%v", duplicated, err)
	}
	waitTerminal(t, m, first.ID)

	for range 2 {
		retry, duplicated, err := m.SubmitImport(ImportRequest{Payload: payload, IdempotencyKey: key})
		if err != nil || !duplicated {
			t.Fatalf("retry: duplicated=%v err=%v", duplicated, err)
		}
		if retry.ID != first.ID {
			t.Fatalf("retry created job %s, want original %s", retry.ID, first.ID)
		}
		if retry.Succeeded != 3 {
			t.Fatalf("retry counters: %+v", retry)
		}
	}

	if got := store.puts.Load(); got != 3 {
		t.Fatalf("store Put called %d times across 3 identical requests, want 3", got)
	}

	// Same key with a different payload is a conflict.
	if _, _, err := m.SubmitImport(ImportRequest{
		Payload:        modulesPayload(map[string]string{"different": "tcp"}),
		IdempotencyKey: key,
	}); err != ErrIdempotencyConflict {
		t.Fatalf("conflict error = %v, want ErrIdempotencyConflict", err)
	}
}

func TestConcurrentDuplicateRequestsCreateSingleJob(t *testing.T) {
	store := &countingStore{inner: NewMemoryStore()}
	m, err := NewManager(store)
	if err != nil {
		t.Fatal(err)
	}

	payload := modulesPayload(map[string]string{"a": "tcp", "b": "tcp"})
	const key = "concurrent-key"

	const submitters = 50
	var wg sync.WaitGroup
	ids := make(chan string, submitters)
	errs := make(chan error, submitters)
	for range submitters {
		wg.Go(func() {
			status, _, err := m.SubmitImport(ImportRequest{Payload: payload, IdempotencyKey: key})
			if err != nil {
				errs <- err
				return
			}
			ids <- status.ID
		})
	}
	wg.Wait()
	close(ids)
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent submit: %v", err)
	}

	first := ""
	count := 0
	for id := range ids {
		if first == "" {
			first = id
		}
		if id != first {
			t.Fatalf("concurrent duplicate produced job %s, want %s", id, first)
		}
		count++
	}
	if count != submitters {
		t.Fatalf("got %d submissions, want %d", count, submitters)
	}
	waitTerminal(t, m, first)
	if got := store.puts.Load(); got != 2 {
		t.Fatalf("concurrent Put calls = %d, want 2", got)
	}
}

func TestLargeBatch(t *testing.T) {
	const n = 2000
	store := NewMemoryStore()
	m, err := NewManager(store)
	if err != nil {
		t.Fatal(err)
	}

	modules := make(map[string]string, n)
	for i := range n {
		modules[fmt.Sprintf("module_%04d", i)] = "tcp"
	}

	status, _, err := m.SubmitImport(ImportRequest{Payload: modulesPayload(modules)})
	if err != nil {
		t.Fatal(err)
	}
	status = waitTerminal(t, m, status.ID)

	if status.State != StateCompleted {
		t.Fatalf("state = %s, want completed", status.State)
	}
	if status.Total != n || status.Succeeded != n || status.Failed != 0 || status.Processed != n {
		t.Fatalf("unexpected counters: total=%d processed=%d succeeded=%d failed=%d",
			status.Total, status.Processed, status.Succeeded, status.Failed)
	}
	if len(status.Results) != n {
		t.Fatalf("got %d item results, want %d", len(status.Results), n)
	}
	if len(store.List()) != n {
		t.Fatalf("stored %d modules, want %d", len(store.List()), n)
	}
}

func TestCancelBatchHasExplicitStates(t *testing.T) {
	store := NewMemoryStore()
	release := make(chan struct{})
	reached := make(chan struct{}, 1)

	m, err := NewManager(store, WithItemHook(func(_ string, index, _ int, _ string) ItemHookAction {
		if index == 1 {
			select {
			case reached <- struct{}{}:
			default:
			}
			<-release
		}
		return HookContinue
	}))
	if err != nil {
		t.Fatal(err)
	}

	payload := modulesPayload(map[string]string{"m0": "tcp", "m1": "tcp", "m2": "tcp", "m3": "tcp"})
	status, _, err := m.SubmitImport(ImportRequest{Payload: payload})
	if err != nil {
		t.Fatal(err)
	}

	// Wait until the first item is applied and the worker is parked before
	// the second one.
	waitFor(t, func() bool {
		return m.Get(status.ID).Processed == 1
	}, "first item processed")

	cancelled, err := m.Cancel(status.ID)
	if err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if cancelled.State != StateCancelling {
		t.Fatalf("state after cancel = %s, want cancelling", cancelled.State)
	}

	close(release)
	final := waitTerminal(t, m, status.ID)
	if final.State != StateCancelled {
		t.Fatalf("final state = %s, want cancelled", final.State)
	}
	if final.Succeeded != 1 || final.Processed != 1 {
		t.Fatalf("cancelled job counters: %+v", final)
	}
	if store.Has("m1") || store.Has("m2") || store.Has("m3") {
		t.Fatal("cancellation stored modules beyond the processed item")
	}
	if !store.Has("m0") {
		t.Fatal("processed module m0 was not stored")
	}

	// Cancelling an already finished job is an error.
	if _, err := m.Cancel(status.ID); err != ErrJobFinished {
		t.Fatalf("second cancel error = %v, want ErrJobFinished", err)
	}
}

func TestInterruptRecoveryResumesJob(t *testing.T) {
	stateFile := filepath.Join(t.TempDir(), "batch-state.jsonl")
	store := NewMemoryStore()

	const total = 10
	modules := make(map[string]string, total)
	for i := range total {
		modules[fmt.Sprintf("mod_%d", i)] = "tcp"
	}
	payload := modulesPayload(modules)

	// First manager: stop abruptly (simulating a killed process) right
	// before processing the 5th item (index 4).
	m1, err := NewManager(store, WithStateFile(stateFile), WithItemHook(
		func(_ string, index, _ int, _ string) ItemHookAction {
			if index == 4 {
				return HookAbruptStop
			}
			return HookContinue
		}))
	if err != nil {
		t.Fatal(err)
	}
	status, _, err := m1.SubmitImport(ImportRequest{Payload: payload, IdempotencyKey: "resumable"})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		for i := range 4 {
			if !store.Has(fmt.Sprintf("mod_%d", i)) {
				return false
			}
		}
		return true
	}, "first four modules applied before crash")
	// Worker has exited; safe to release the state file handle.
	if err := m1.Close(); err != nil {
		t.Fatal(err)
	}

	// Second manager: replays the journal and resumes the job.
	m2, err := NewManager(store, WithStateFile(stateFile))
	if err != nil {
		t.Fatal(err)
	}
	defer m2.Close()

	recovered := m2.Get(status.ID)
	if recovered == nil {
		t.Fatal("recovered job missing after restart")
	}

	final := waitTerminal(t, m2, status.ID)
	if final.State != StateCompleted {
		t.Fatalf("recovered job state = %s, want completed", final.State)
	}
	if final.Total != total || final.Succeeded != total || final.Processed != total {
		t.Fatalf("recovered counters: %+v", final)
	}
	for i := range total {
		if !store.Has(fmt.Sprintf("mod_%d", i)) {
			t.Fatalf("module mod_%d missing after recovery", i)
		}
	}

	// The idempotency table survived the restart.
	retry, duplicated, err := m2.SubmitImport(ImportRequest{Payload: payload, IdempotencyKey: "resumable"})
	if err != nil {
		t.Fatal(err)
	}
	if !duplicated || retry.ID != status.ID {
		t.Fatalf("idempotency key not recovered: duplicated=%v id=%s want %s", duplicated, retry.ID, status.ID)
	}
}

func TestRecoveredCancelledJobStaysCancelled(t *testing.T) {
	stateFile := filepath.Join(t.TempDir(), "batch-state.jsonl")
	store := NewMemoryStore()

	// The worker parks before the second item, then crashes (abrupt stop)
	// once the test has requested cancellation.
	instruction := make(chan ItemHookAction, 1)
	m1, err := NewManager(store, WithStateFile(stateFile), WithItemHook(
		func(_ string, index, _ int, _ string) ItemHookAction {
			if index == 1 {
				return <-instruction
			}
			return HookContinue
		}))
	if err != nil {
		t.Fatal(err)
	}
	payload := modulesPayload(map[string]string{"a": "tcp", "b": "tcp", "c": "tcp"})
	status, _, err := m1.SubmitImport(ImportRequest{Payload: payload})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return m1.Get(status.ID).Processed == 1 }, "one item processed")
	if _, err := m1.Cancel(status.ID); err != nil {
		t.Fatal(err)
	}
	// Kill the worker while the job is in "cancelling".
	instruction <- HookAbruptStop
	if err := m1.Close(); err != nil {
		t.Fatal(err)
	}

	m2, err := NewManager(store, WithStateFile(stateFile))
	if err != nil {
		t.Fatal(err)
	}
	defer m2.Close()

	final := waitTerminal(t, m2, status.ID)
	if final.State != StateCancelled {
		t.Fatalf("recovered state = %s, want cancelled", final.State)
	}
	if final.Processed != 1 {
		t.Fatalf("recovered cancelled job processed %d items, want 1", final.Processed)
	}
}

func TestExportSubsetAndMissing(t *testing.T) {
	store := NewMemoryStore()
	m, err := NewManager(store)
	if err != nil {
		t.Fatal(err)
	}
	status, _, err := m.SubmitImport(ImportRequest{
		Payload: modulesPayload(map[string]string{"a": "tcp", "b": "http", "c": "tcp"}),
	})
	if err != nil {
		t.Fatal(err)
	}
	waitTerminal(t, m, status.ID)

	data, err := m.Export([]string{"a", "b"}, false)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := ParseEnvelope(data, FormatYAML)
	if err != nil {
		t.Fatal(err)
	}
	if len(parsed.Modules) != 2 {
		t.Fatalf("exported %d modules, want 2", len(parsed.Modules))
	}

	if _, err := m.Export([]string{"a", "nope"}, false); err == nil {
		t.Fatal("expected error when exporting a missing module")
	}
}

func TestExportThenImportRoundTrip(t *testing.T) {
	source := NewMemoryStore()
	exporter, err := NewManager(source)
	if err != nil {
		t.Fatal(err)
	}
	status, _, err := exporter.SubmitImport(ImportRequest{
		Payload: modulesPayload(map[string]string{"http_2xx": "http", "tcp_connect": "tcp"}),
	})
	if err != nil {
		t.Fatal(err)
	}
	waitTerminal(t, exporter, status.ID)

	data, err := exporter.Export(nil, false)
	if err != nil {
		t.Fatal(err)
	}

	target := NewMemoryStore()
	importer, err := NewManager(target)
	if err != nil {
		t.Fatal(err)
	}
	importStatus, _, err := importer.SubmitImport(ImportRequest{Payload: data})
	if err != nil {
		t.Fatal(err)
	}
	final := waitTerminal(t, importer, importStatus.ID)
	if final.State != StateCompleted || final.Succeeded != 2 {
		t.Fatalf("round trip import: %+v", final)
	}
	for _, name := range []string{"http_2xx", "tcp_connect"} {
		if !target.Has(name) {
			t.Errorf("module %s missing after round trip", name)
		}
	}
}

func TestHTTPAPI(t *testing.T) {
	m, err := NewManager(NewMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(m.Handler())
	defer server.Close()

	// Import a module with an idempotency key.
	payload := modulesPayload(map[string]string{"http_2xx": "http"})
	req, _ := http.NewRequest(http.MethodPost, server.URL+"/import", bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/yaml")
	req.Header.Set(HeaderIdempotencyKey, "http-key")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("import status = %d, want 202", resp.StatusCode)
	}
	var created JobStatus
	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	// Poll the job until completion.
	waitFor(t, func() bool {
		r, err := http.Get(server.URL + "/jobs/" + created.ID)
		if err != nil {
			return false
		}
		defer r.Body.Close()
		var status JobStatus
		if json.NewDecoder(r.Body).Decode(&status) != nil {
			return false
		}
		return status.State.IsTerminal()
	}, "import job to complete")

	// Retrying the same key returns 200 and the same job.
	req, _ = http.NewRequest(http.MethodPost, server.URL+"/import", bytes.NewReader(payload))
	req.Header.Set(HeaderIdempotencyKey, "http-key")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("idempotent retry status = %d, want 200", resp.StatusCode)
	}
	var retried JobStatus
	json.NewDecoder(resp.Body).Decode(&retried)
	resp.Body.Close()
	if retried.ID != created.ID {
		t.Fatalf("retry job id = %s, want %s", retried.ID, created.ID)
	}

	// Cancelling a finished job conflicts.
	resp, err = http.Post(server.URL+"/jobs/"+created.ID+"/cancel", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("cancel finished status = %d, want 409", resp.StatusCode)
	}
	resp.Body.Close()

	// Unknown job is 404.
	resp, err = http.Get(server.URL + "/jobs/does-not-exist")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown job status = %d, want 404", resp.StatusCode)
	}
	resp.Body.Close()

	// Invalid payload is 400.
	resp, err = http.Post(server.URL+"/import", "application/yaml", strings.NewReader("version: 2\nmodules: {}\n"))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("invalid payload status = %d, want 400", resp.StatusCode)
	}
	resp.Body.Close()

	// Export returns YAML containing the imported module.
	resp, err = http.Get(server.URL + "/export")
	if err != nil {
		t.Fatal(err)
	}
	body := readAll(t, resp)
	if resp.StatusCode != http.StatusOK || !strings.Contains(body, "http_2xx:") {
		t.Fatalf("export status = %d body = %q", resp.StatusCode, body)
	}
	resp.Body.Close()

	// Versioned export carries the envelope version.
	resp, err = http.Get(server.URL + "/export?versioned=true")
	if err != nil {
		t.Fatal(err)
	}
	body = readAll(t, resp)
	if resp.StatusCode != http.StatusOK || !strings.Contains(body, "version: 1") {
		t.Fatalf("versioned export status = %d body = %q", resp.StatusCode, body)
	}
	resp.Body.Close()

	// Jobs list includes the import.
	resp, err = http.Get(server.URL + "/jobs")
	if err != nil {
		t.Fatal(err)
	}
	var jobs []JobStatus
	if err := json.NewDecoder(resp.Body).Decode(&jobs); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if len(jobs) != 1 {
		t.Fatalf("jobs list length = %d, want 1", len(jobs))
	}
}

func readAll(t *testing.T, r *http.Response) string {
	t.Helper()
	defer r.Body.Close()
	buf := new(bytes.Buffer)
	if _, err := buf.ReadFrom(r.Body); err != nil {
		t.Fatal(err)
	}
	return buf.String()
}
