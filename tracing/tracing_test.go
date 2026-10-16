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

package tracing

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func newTextLogger(w io.Writer) *slog.Logger {
	return slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

func TestNewIDUniqueAndWellFormed(t *testing.T) {
	const n = 1000
	seen := make(map[string]struct{}, n)
	for range n {
		id := NewID()
		if len(id) != 32 {
			t.Fatalf("operation id %q has unexpected length %d", id, len(id))
		}
		if _, dup := seen[id]; dup {
			t.Fatalf("duplicate operation id generated: %q", id)
		}
		seen[id] = struct{}{}
	}
}

func TestNewIDUniqueUnderConcurrency(t *testing.T) {
	const goroutines = 64
	const perGoroutine = 100

	var mu sync.Mutex
	seen := make(map[string]struct{}, goroutines*perGoroutine)
	var wg sync.WaitGroup
	for range goroutines {
		wg.Go(func() {
			ids := make([]string, perGoroutine)
			for i := range perGoroutine {
				ids[i] = NewID()
			}
			mu.Lock()
			defer mu.Unlock()
			for _, id := range ids {
				if _, dup := seen[id]; dup {
					t.Errorf("duplicate operation id generated under concurrency: %q", id)
					return
				}
				seen[id] = struct{}{}
			}
		})
	}
	wg.Wait()
}

func TestStartGeneratesOrReusesID(t *testing.T) {
	SetDefaultSampler(AlwaysSample{})
	t.Cleanup(func() { SetDefaultSampler(AlwaysSample{}) })

	// An empty inbound ID is replaced by a generated one.
	ctx, op := Start(context.Background(), "")
	if op.ID == "" {
		t.Fatal("expected generated operation id")
	}
	if got := OperationID(ctx); got != op.ID {
		t.Fatalf("context operation id %q, want %q", got, op.ID)
	}
	if !op.Sampled {
		t.Fatal("new operation should be sampled with AlwaysSample")
	}
	if op.Attempt != 1 {
		t.Fatalf("initial attempt = %d, want 1", op.Attempt)
	}

	// An inbound ID is propagated unchanged (cross-module continuation).
	const inbound = "upstream-op-42"
	ctx2, op2 := Start(context.Background(), inbound)
	if op2.ID != inbound {
		t.Fatalf("operation id %q, want inbound %q", op2.ID, inbound)
	}
	if got := OperationID(ctx2); got != inbound {
		t.Fatalf("context operation id %q, want %q", got, inbound)
	}
}

func TestContextAccessorsWithoutOperation(t *testing.T) {
	ctx := context.Background()
	if op, ok := FromContext(ctx); ok {
		t.Fatalf("unexpected operation on bare context: %+v", op)
	}
	if got := OperationID(ctx); got != "" {
		t.Fatalf("OperationID() = %q, want empty", got)
	}
	if !Sampled(ctx) {
		t.Fatal("bare context should be considered sampled")
	}
	if got := Attempt(ctx); got != 0 {
		t.Fatalf("Attempt() = %d, want 0", got)
	}
	// WithAttempt on a context without operation is a no-op.
	if WithAttempt(ctx, 5) != ctx {
		t.Fatal("WithAttempt should return the context unchanged when there is no operation")
	}
}

func TestWithAttemptKeepsOperationCorrelation(t *testing.T) {
	ctx, op := Start(context.Background(), "op-retry")

	attempt2 := WithAttempt(ctx, 2)
	if got := OperationID(attempt2); got != op.ID {
		t.Fatalf("attempt 2 operation id %q, want %q", got, op.ID)
	}
	if got := Attempt(attempt2); got != 2 {
		t.Fatalf("attempt number %d, want 2", got)
	}

	// The sampling decision is stable across retries, for both decisions.
	for _, sampler := range []Sampler{AlwaysSample{}, NeverSample{}} {
		SetDefaultSampler(sampler)
		base, started := Start(context.Background(), "op-retry-stable")
		wantSampled := started.Sampled
		for attempt := 1; attempt <= 5; attempt++ {
			retryCtx := WithAttempt(base, attempt)
			if OperationID(retryCtx) != started.ID {
				t.Fatalf("operation id changed on attempt %d", attempt)
			}
			if Sampled(retryCtx) != wantSampled {
				t.Fatalf("sampling decision changed on attempt %d", attempt)
			}
		}
	}
	SetDefaultSampler(AlwaysSample{})

	// Deriving an attempt must not mutate the parent context.
	if got := Attempt(ctx); got != 1 {
		t.Fatalf("parent context attempt mutated: %d, want 1", got)
	}
}

func TestRetrySucceedsAfterTransientFailures(t *testing.T) {
	ctx, op := Start(context.Background(), "op-flaky")

	var attempts []int
	var ids []string
	var sampled []bool
	wantErr := errors.New("boom")

	err := Retry(ctx, 4, func(ctx context.Context, attempt int) error {
		attempts = append(attempts, attempt)
		ids = append(ids, OperationID(ctx))
		sampled = append(sampled, Sampled(ctx))
		if attempt < 3 {
			return wantErr
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Retry returned unexpected error: %v", err)
	}
	if got := len(attempts); got != 3 {
		t.Fatalf("fn ran %d times, want 3", got)
	}
	for i, attempt := range attempts {
		if attempt != i+1 {
			t.Fatalf("attempt sequence %v, want [1 2 3]", attempts)
		}
	}
	for _, id := range ids {
		if id != op.ID {
			t.Fatalf("retry attempt lost operation correlation: got %q, want %q", id, op.ID)
		}
	}
	for _, s := range sampled {
		if !s {
			t.Fatal("sampling decision must stay constant across retries")
		}
	}
}

func TestRetryExhaustedReturnsLastError(t *testing.T) {
	ctx, _ := Start(context.Background(), "op-permanent-failure")

	var calls int
	first := errors.New("first")
	last := errors.New("last")
	err := Retry(ctx, 3, func(context.Context, int) error {
		calls++
		if calls == 3 {
			return last
		}
		return first
	})
	if !errors.Is(err, last) {
		t.Fatalf("Retry error = %v, want last error %v", err, last)
	}
	if calls != 3 {
		t.Fatalf("fn ran %d times, want 3", calls)
	}
}

func TestRetryNonPositiveAttemptsRunsOnce(t *testing.T) {
	for _, attempts := range []int{0, -1} {
		var calls int
		if err := Retry(context.Background(), attempts, func(context.Context, int) error {
			calls++
			return nil
		}); err != nil {
			t.Fatalf("attempts=%d: unexpected error: %v", attempts, err)
		}
		if calls != 1 {
			t.Fatalf("attempts=%d: fn ran %d times, want 1", attempts, calls)
		}
	}
}

func TestRetryStopsOnCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var calls int
	err := Retry(ctx, 5, func(_ context.Context, _ int) error {
		calls++
		return errors.New("never reached")
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Retry error = %v, want context.Canceled", err)
	}
	if calls != 0 {
		t.Fatalf("fn ran %d times on already-cancelled context", calls)
	}
}

func TestRetryStopsWhenContextExpiresMidway(t *testing.T) {
	ctx, op := Start(context.Background(), "op-timeout")
	wrapped, cancel := context.WithCancel(ctx)

	var mu sync.Mutex
	var ids []string
	err := Retry(wrapped, 10, func(ctx context.Context, attempt int) error {
		mu.Lock()
		ids = append(ids, OperationID(ctx))
		mu.Unlock()
		if attempt == 2 {
			cancel()
		}
		return errors.New("still failing")
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Retry error = %v, want context.Canceled", err)
	}
	for _, id := range ids {
		if id != op.ID {
			t.Fatalf("retry after cancellation lost operation id: %q, want %q", id, op.ID)
		}
	}
}

func TestDetachKeepsOperationButDropsCancellation(t *testing.T) {
	parent, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	ctx, op := Start(parent, "op-async")
	ctx, cancelInner := context.WithCancel(ctx)

	detached := Detach(ctx)
	if got := OperationID(detached); got != op.ID {
		t.Fatalf("detached operation id %q, want %q", got, op.ID)
	}
	if Sampled(detached) != op.Sampled {
		t.Fatal("detached context changed sampling decision")
	}
	if got := Attempt(detached); got != 1 {
		t.Fatalf("detached attempt = %d, want 1", got)
	}
	if _, ok := detached.Deadline(); ok {
		t.Fatal("detached context must not carry a deadline")
	}
	if err := detached.Err(); err != nil {
		t.Fatalf("detached context already done: %v", err)
	}

	// Cancelling the parent (which also carries a deadline) must not cancel
	// the detached work.
	cancelInner()
	cancel()
	if err := ctx.Err(); !errors.Is(err, context.Canceled) {
		t.Fatalf("parent context err = %v, want Canceled", err)
	}
	if err := detached.Err(); err != nil {
		t.Fatalf("detached context canceled with parent: %v", err)
	}

	// Detach on a context without an operation returns it unchanged.
	plain := context.Background()
	if Detach(plain) != plain {
		t.Fatal("Detach should return a bare context unchanged")
	}
}

func TestGoPropagatesOperationAsynchronously(t *testing.T) {
	ctx, op := Start(context.Background(), "op-goroutine")
	ctx, cancel := context.WithCancel(ctx)

	result := make(chan Operation, 1)
	var wg sync.WaitGroup
	wg.Add(1)
	Go(ctx, func(ctx context.Context) {
		defer wg.Done()
		got, ok := FromContext(ctx)
		if !ok {
			t.Error("goroutine context lost the operation")
			return
		}
		result <- got
	})
	cancel() // Parent cancellation must not affect the detached goroutine.
	wg.Wait()

	got := <-result
	if got.ID != op.ID {
		t.Fatalf("async operation id %q, want %q", got.ID, op.ID)
	}
	if got.Sampled != op.Sampled {
		t.Fatal("async continuation changed sampling decision")
	}
}

func TestGoManyConcurrentOperationsDoNotCorrelateAcrossOps(t *testing.T) {
	const n = 200
	var wg sync.WaitGroup
	results := make([]string, n)
	for i := range n {
		ctx, _ := Start(context.Background(), "op-concurrent-"+itoa(i))
		i := i
		wg.Go(func() {
			// The worker goroutine observes a detached (but correlated) ctx.
			results[i] = OperationID(Detach(ctx))
		})
	}
	wg.Wait()
	for i := range n {
		if want := "op-concurrent-" + itoa(i); results[i] != want {
			t.Fatalf("goroutine %d observed operation id %q, want %q", i, results[i], want)
		}
	}
}

func TestLoggerWithOperationSampling(t *testing.T) {
	SetDefaultSampler(AlwaysSample{})
	t.Cleanup(func() { SetDefaultSampler(AlwaysSample{}) })

	var buf bytes.Buffer
	ctx, op := Start(context.Background(), "op-logs")
	logger := LoggerWithOperation(ctx, newTextLogger(&buf))
	logger.Info("sync step")
	if !strings.Contains(buf.String(), "operation_id="+op.ID) {
		t.Fatalf("sampled log missing operation id: %q", buf.String())
	}

	// Retry logs keep the same operation id and gain the attempt attribute.
	buf.Reset()
	retryLogger := LoggerWithOperation(WithAttempt(ctx, 2), newTextLogger(&buf))
	retryLogger.Info("retry step")
	out := buf.String()
	if !strings.Contains(out, "operation_id="+op.ID) || !strings.Contains(out, "attempt=2") {
		t.Fatalf("retry log missing correlation attributes: %q", out)
	}

	// Unsampled operations must not leak the operation id into logs.
	SetDefaultSampler(NeverSample{})
	buf.Reset()
	ctx2, _ := Start(context.Background(), "op-unsampled-logs")
	logger2 := LoggerWithOperation(ctx2, newTextLogger(&buf))
	logger2.Info("still runs")
	if strings.Contains(buf.String(), "operation_id") {
		t.Fatalf("unsampled log must not carry the operation id: %q", buf.String())
	}

	// A context without an operation leaves the logger untouched.
	buf.Reset()
	logger3 := LoggerWithOperation(context.Background(), newTextLogger(&buf))
	logger3.Info("plain")
	if strings.Contains(buf.String(), "operation_id") {
		t.Fatalf("plain log must not carry the operation id: %q", buf.String())
	}

	// Nil logger stays nil.
	if got := LoggerWithOperation(ctx, nil); got != nil {
		t.Fatalf("LoggerWithOperation(nil) = %v, want nil", got)
	}
}

func TestRegisterOperationMetric(t *testing.T) {
	SetDefaultSampler(AlwaysSample{})
	t.Cleanup(func() { SetDefaultSampler(AlwaysSample{}) })

	// Sampled operation: info metric exposes the operation id.
	ctx, op := Start(context.Background(), "op-metrics")
	registry := prometheus.NewRegistry()
	RegisterOperationMetric(ctx, registry)
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	gauge := findGauge(families, MetricName)
	if gauge == nil {
		t.Fatalf("%s metric not registered", MetricName)
	}
	if got := gaugeLabel(gauge, MetricLabelOperationID); got != op.ID {
		t.Fatalf("metric label %q, want %q", got, op.ID)
	}
	if gauge.GetGauge().GetValue() != 1 {
		t.Fatalf("metric value = %v, want 1", gauge.GetGauge().GetValue())
	}

	// Unsampled operation: no metric is registered, gathering stays empty.
	SetDefaultSampler(NeverSample{})
	ctx2, _ := Start(context.Background(), "op-metrics-unsampled")
	registry2 := prometheus.NewRegistry()
	RegisterOperationMetric(ctx2, registry2)
	if families2, err := registry2.Gather(); err != nil || len(families2) != 0 {
		t.Fatalf("unsampled operation registered metrics: %v (err=%v)", families2, err)
	}

	// Bare context: nothing happens either.
	registry3 := prometheus.NewRegistry()
	RegisterOperationMetric(context.Background(), registry3)
	if families3, err := registry3.Gather(); err != nil || len(families3) != 0 {
		t.Fatalf("bare context registered metrics: %v (err=%v)", families3, err)
	}
}

func TestSamplingDoesNotChangeBusinessResult(t *testing.T) {
	// A "business" computation that retries and returns a result. It must
	// produce exactly the same result and the same number of attempts
	// regardless of the sampling decision.
	run := func(sampler Sampler) (string, int) {
		SetDefaultSampler(sampler)
		ctx, _ := Start(context.Background(), "op-business")
		attempts := 0
		result := ""
		_ = Retry(ctx, 3, func(ctx context.Context, attempt int) error {
			attempts++
			// Logging must also be safe on both paths.
			LoggerWithOperation(ctx, slog.New(slog.DiscardHandler)).Info("working", "attempt", attempt)
			if attempt < 2 {
				return errors.New("transient")
			}
			result = "done"
			return nil
		})
		return result, attempts
	}

	sampledResult, sampledAttempts := run(AlwaysSample{})
	unsampledResult, unsampledAttempts := run(NeverSample{})
	if sampledResult != unsampledResult || sampledAttempts != unsampledAttempts {
		t.Fatalf("business result changed with sampling: sampled=(%q,%d) unsampled=(%q,%d)",
			sampledResult, sampledAttempts, unsampledResult, unsampledAttempts)
	}
	if sampledResult != "done" || sampledAttempts != 2 {
		t.Fatalf("unexpected business outcome: %q after %d attempts", sampledResult, sampledAttempts)
	}
	SetDefaultSampler(AlwaysSample{})
}

func TestAlwaysAndNeverSampler(t *testing.T) {
	if !(AlwaysSample{}).Sample("anything") {
		t.Fatal("AlwaysSample rejected an operation")
	}
	if (NeverSample{}).Sample("anything") {
		t.Fatal("NeverSample accepted an operation")
	}
}

func TestRatioSamplerBoundaries(t *testing.T) {
	if NewRatioSampler(0).Sample("some-id") {
		t.Fatal("ratio 0 must sample nothing")
	}
	if !NewRatioSampler(1).Sample("some-id") {
		t.Fatal("ratio 1 must sample everything")
	}
	// Out-of-range and NaN ratios are clamped.
	if NewRatioSampler(-0.5).Sample("some-id") {
		t.Fatal("negative ratio must be clamped to 0")
	}
	if !NewRatioSampler(1.5).Sample("some-id") {
		t.Fatal("ratio above 1 must be clamped to 1")
	}
	if NewRatioSampler(math.NaN()).Sample("some-id") {
		t.Fatal("NaN ratio must be clamped to 0")
	}
}

func TestRatioSamplerDeterministicPerID(t *testing.T) {
	s := NewRatioSampler(0.5)
	for _, id := range []string{"a", "b", "c", "retry-op", "1234567890"} {
		first := s.Sample(id)
		for range 100 {
			if s.Sample(id) != first {
				t.Fatalf("sampling decision for %q not stable", id)
			}
		}
	}
}

func TestRatioSamplerDistribution(t *testing.T) {
	const n = 20000
	s := NewRatioSampler(0.5)
	sampled := 0
	for i := range n {
		if s.Sample("op-dist-" + itoa(i)) {
			sampled++
		}
	}
	ratio := float64(sampled) / n
	if ratio < 0.45 || ratio > 0.55 {
		t.Fatalf("sampled ratio %.3f outside tolerance [0.45, 0.55]", ratio)
	}
}

func TestRatioSamplerConcurrentAccess(t *testing.T) {
	s := NewRatioSampler(0.5)
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			for i := range 200 {
				if i%2 == 0 {
					s.SetRatio(0.25 + 0.5*float64(i%2))
				} else {
					_ = s.Sample("op-race-" + itoa(i))
					_ = s.Ratio()
				}
			}
		})
	}
	wg.Wait()
	if r := s.Ratio(); r < 0 || r > 1 {
		t.Fatalf("ratio out of range after concurrent sets: %v", r)
	}
}

func TestDefaultSamplerConcurrentAccess(t *testing.T) {
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			for i := range 200 {
				if i%2 == 0 {
					SetDefaultSampler(NewRatioSampler(float64(i % 2)))
				} else {
					_ = DefaultSampler()
					_, _ = Start(context.Background(), "op-default-race-"+itoa(i))
				}
			}
		})
	}
	wg.Wait()
	SetDefaultSampler(AlwaysSample{})
	if _, ok := DefaultSampler().(AlwaysSample); !ok {
		t.Fatal("default sampler not reset to AlwaysSample")
	}
}

func TestSetDefaultSamplerNilResets(t *testing.T) {
	SetDefaultSampler(NeverSample{})
	if _, ok := DefaultSampler().(NeverSample); !ok {
		t.Fatal("expected NeverSample")
	}
	SetDefaultSampler(nil)
	if _, ok := DefaultSampler().(AlwaysSample); !ok {
		t.Fatal("nil sampler should reset to AlwaysSample")
	}
}

func findGauge(families []*dto.MetricFamily, name string) *dto.Metric {
	for _, f := range families {
		if f.GetName() == name {
			if len(f.Metric) != 1 {
				return nil
			}
			return f.Metric[0]
		}
	}
	return nil
}

func gaugeLabel(m *dto.Metric, name string) string {
	for _, lp := range m.GetLabel() {
		if lp.GetName() == name {
			return lp.GetValue()
		}
	}
	return ""
}

// itoa avoids pulling in strconv with a dedicated helper name to keep the
// test's intent obvious.
func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var b [20]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		b[pos] = '-'
	}
	return string(b[pos:])
}
