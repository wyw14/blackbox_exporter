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

// Package tracing provides lightweight, dependency-free operation
// correlation for blackbox_exporter.
//
// Every logical operation (a /probe scrape, a configuration reload, ...) is
// assigned an operation ID that is carried through:
//
//   - synchronous call chains via context.Context,
//   - asynchronous goroutines via Detach/Go,
//   - retry loops via WithAttempt/Retry,
//
// so that all logs and metrics emitted for one operation can be aggregated
// under the same operation ID.
//
// Sampling only decides whether the operation ID is emitted on logs and
// metrics. The sampling decision is taken once per operation and never
// changes along sync/async/retry boundaries, and business code must never
// branch on it: toggling sampling cannot alter probe results.
package tracing

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"hash/fnv"
	"log/slog"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	// HeaderOperationID is the inbound HTTP header used to continue an
	// operation started by an upstream caller. When absent, a new operation
	// ID is generated.
	HeaderOperationID = "X-Operation-Id"

	// LogKeyOperationID is the slog attribute carrying the operation ID.
	LogKeyOperationID = "operation_id"

	// LogKeyAttempt is the slog attribute carrying the 1-based attempt
	// number within an operation.
	LogKeyAttempt = "attempt"

	// MetricName is the name of the info metric tying all metrics of one
	// operation to its operation ID.
	MetricName = "probe_operation_info"

	// MetricLabelOperationID is the label of MetricName.
	MetricLabelOperationID = "operation_id"
)

type ctxKey struct{}

// state is the immutable tracing payload stored in the context. WithAttempt
// derives a new state instead of mutating the existing one, so the same
// context can safely be shared across goroutines and retry branches.
type state struct {
	id      string
	sampled bool
	attempt int
}

// Operation describes a running operation.
type Operation struct {
	// ID is the identifier shared by every log line and metric of the
	// operation, including its async and retry continuations.
	ID string
	// Sampled reports whether the operation ID is emitted on logs and
	// metrics for this operation. It is constant for the whole operation.
	Sampled bool
	// Attempt is the 1-based attempt number within the operation.
	Attempt int
}

func (s *state) operation() Operation {
	return Operation{ID: s.id, Sampled: s.sampled, Attempt: s.attempt}
}

// NewID returns a new random, URL/label-safe operation ID. It panics only if
// the system's cryptographic randomness source is broken, in which case the
// process cannot be trusted to run an exporter anyway.
func NewID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("tracing: unable to read random bytes for operation id: " + err.Error())
	}
	return hex.EncodeToString(b[:])
}

// Start begins an operation. If operationID is empty a new ID is generated,
// otherwise the inbound ID is reused. The sampling decision is taken once
// here from the default sampler and is preserved across every derived
// context (async continuations and retries). The returned Operation reports
// the resolved ID and sampling decision.
func Start(ctx context.Context, operationID string) (context.Context, Operation) {
	if operationID == "" {
		operationID = NewID()
	}
	s := &state{id: operationID, sampled: currentSampler().Sample(operationID), attempt: 1}
	return context.WithValue(ctx, ctxKey{}, s), s.operation()
}

// FromContext returns the operation carried by ctx, if any.
func FromContext(ctx context.Context) (Operation, bool) {
	s, ok := ctx.Value(ctxKey{}).(*state)
	if !ok {
		return Operation{}, false
	}
	return s.operation(), true
}

// OperationID returns the operation ID carried by ctx, or "" if ctx carries
// no operation.
func OperationID(ctx context.Context) string {
	if s, ok := ctx.Value(ctxKey{}).(*state); ok {
		return s.id
	}
	return ""
}

// Sampled reports whether the operation carried by ctx is sampled. A context
// without an operation is considered sampled so that non-instrumented code
// paths keep emitting logs unchanged.
func Sampled(ctx context.Context) bool {
	if s, ok := ctx.Value(ctxKey{}).(*state); ok {
		return s.sampled
	}
	return true
}

// Attempt returns the 1-based attempt number carried by ctx, or 0 if ctx
// carries no operation.
func Attempt(ctx context.Context) int {
	if s, ok := ctx.Value(ctxKey{}).(*state); ok {
		return s.attempt
	}
	return 0
}

// WithAttempt returns a context carrying the same operation ID and sampling
// decision as ctx but with the given attempt number. It is used to correlate
// retry paths. If ctx carries no operation it is returned unchanged.
func WithAttempt(ctx context.Context, attempt int) context.Context {
	s, ok := ctx.Value(ctxKey{}).(*state)
	if !ok {
		return ctx
	}
	return context.WithValue(ctx, ctxKey{}, &state{id: s.id, sampled: s.sampled, attempt: attempt})
}

// Detach returns a context that keeps the correlation state of ctx (operation
// ID, sampling decision and attempt) but is not cancelled when ctx is and
// carries no deadline. It is meant for handing an operation off to another
// goroutine whose lifetime is independent of the caller's (for example an
// HTTP request handed to a background worker through a channel). If ctx
// carries no operation it is returned unchanged.
func Detach(ctx context.Context) context.Context {
	s, ok := ctx.Value(ctxKey{}).(*state)
	if !ok {
		return ctx
	}
	detached := &state{id: s.id, sampled: s.sampled, attempt: s.attempt}
	return context.WithValue(context.Background(), ctxKey{}, detached)
}

// Go runs fn in a new goroutine with a detached copy of ctx: the operation
// correlation is preserved while cancellation and deadlines of the caller no
// longer apply. It is the asynchronous counterpart of Start.
func Go(ctx context.Context, fn func(ctx context.Context)) {
	go fn(Detach(ctx))
}

// LoggerWithOperation returns a logger that annotates every record with the
// operation ID carried by ctx, but only when the operation is sampled. The
// attempt number is added for retries (attempt > 1); the initial attempt is
// not annotated to keep the common-path logs unchanged. A context without an
// operation, or an unsampled operation, returns logger unchanged.
func LoggerWithOperation(ctx context.Context, logger *slog.Logger) *slog.Logger {
	if logger == nil {
		return nil
	}
	s, ok := ctx.Value(ctxKey{}).(*state)
	if !ok || !s.sampled {
		return logger
	}
	args := []any{LogKeyOperationID, s.id}
	if s.attempt > 1 {
		args = append(args, LogKeyAttempt, s.attempt)
	}
	return logger.With(args...)
}

// RegisterOperationMetric registers an info metric on registry that ties all
// metrics gathered during the operation to its operation ID, so that the
// scraped metrics can be aggregated together with the correlated logs.
//
// Nothing is registered when ctx carries no operation or when the operation
// is not sampled. Registering (or not) never affects the probe itself, so
// toggling sampling cannot change business results.
func RegisterOperationMetric(ctx context.Context, registry prometheus.Registerer) {
	s, ok := ctx.Value(ctxKey{}).(*state)
	if !ok || !s.sampled {
		return
	}
	gauge := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: MetricName,
		Help: "Contains the operation ID shared by all logs and metrics emitted for a single operation; the value 1 marks a sampled operation.",
	}, []string{MetricLabelOperationID})
	registry.MustRegister(gauge)
	gauge.WithLabelValues(s.id).Set(1)
}

// Retry runs fn up to attempts times until it returns nil. Every attempt
// runs under the same operation ID and sampling decision as ctx; fn receives
// a context annotated with the 1-based attempt number and the attempt number
// itself. Retry stops early when ctx is cancelled or expires, in which case
// the context error is returned. The last non-nil error is returned when all
// attempts fail.
//
// Whether an attempt is made must never depend on the sampling decision:
// retries are business behavior, sampling is observability only.
func Retry(ctx context.Context, attempts int, fn func(ctx context.Context, attempt int) error) error {
	if attempts < 1 {
		attempts = 1
	}
	var err error
	for attempt := 1; attempt <= attempts; attempt++ {
		if err = ctx.Err(); err != nil {
			return err
		}
		err = fn(WithAttempt(ctx, attempt), attempt)
		if err == nil {
			return nil
		}
	}
	return err
}

// Sampler decides whether an operation is sampled. Implementations must be
// safe for concurrent use and must return a stable decision for a given
// operation ID: retries and asynchronous continuations of the same operation
// have to observe the same decision.
type Sampler interface {
	Sample(operationID string) bool
}

// AlwaysSample is a Sampler that samples every operation.
type AlwaysSample struct{}

// Sample implements Sampler.
func (AlwaysSample) Sample(string) bool { return true }

// NeverSample is a Sampler that samples no operation.
type NeverSample struct{}

// Sample implements Sampler.
func (NeverSample) Sample(string) bool { return false }

// RatioSampler samples a deterministic fraction of operations. The decision
// for a given operation ID is a stable hash of the ID, so all retries and
// asynchronous continuations of the operation agree on it regardless of
// which goroutine evaluates it.
type RatioSampler struct {
	mu    sync.RWMutex
	ratio float64
}

// NewRatioSampler returns a RatioSampler with the given ratio. ratio is
// clamped to [0, 1]: 0 samples nothing, 1 samples everything.
func NewRatioSampler(ratio float64) *RatioSampler {
	return &RatioSampler{ratio: clampRatio(ratio)}
}

// Sample implements Sampler.
func (s *RatioSampler) Sample(operationID string) bool {
	s.mu.RLock()
	ratio := s.ratio
	s.mu.RUnlock()

	if ratio <= 0 {
		return false
	}
	if ratio >= 1 {
		return true
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(operationID))
	// Uniformly map the 32-bit hash into [0, 1).
	bucket := float64(h.Sum32()) / float64(1<<32)
	return bucket < ratio
}

// SetRatio updates the sampling ratio, clamped to [0, 1]. It is safe to call
// concurrently with Sample.
func (s *RatioSampler) SetRatio(ratio float64) {
	s.mu.Lock()
	s.ratio = clampRatio(ratio)
	s.mu.Unlock()
}

// Ratio returns the current sampling ratio.
func (s *RatioSampler) Ratio() float64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.ratio
}

func clampRatio(ratio float64) float64 {
	switch {
	case ratio < 0 || ratio != ratio: // negative or NaN
		return 0
	case ratio > 1:
		return 1
	default:
		return ratio
	}
}

var (
	defaultSamplerMu sync.RWMutex
	defaultSampler   Sampler = AlwaysSample{}
)

// SetDefaultSampler sets the sampler used by Start. A nil sampler resets to
// AlwaysSample.
func SetDefaultSampler(sampler Sampler) {
	if sampler == nil {
		sampler = AlwaysSample{}
	}
	defaultSamplerMu.Lock()
	defaultSampler = sampler
	defaultSamplerMu.Unlock()
}

// DefaultSampler returns the sampler currently used by Start.
func DefaultSampler() Sampler {
	defaultSamplerMu.RLock()
	defer defaultSamplerMu.RUnlock()
	return defaultSampler
}

func currentSampler() Sampler {
	defaultSamplerMu.RLock()
	defer defaultSamplerMu.RUnlock()
	return defaultSampler
}
