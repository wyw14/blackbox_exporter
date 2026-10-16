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

package config

import (
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

// Severity classifies a Diagnostic.
type Severity int

const (
	// SeverityWarning reports a suspicious, but still loadable, configuration.
	SeverityWarning Severity = 1
	// SeverityError rejects the configuration.
	SeverityError Severity = 2
)

// String returns the textual representation of the severity.
func (s Severity) String() string {
	switch s {
	case SeverityWarning:
		return "warning"
	case SeverityError:
		return "error"
	default:
		return "unknown"
	}
}

// Diagnostic is a single validation finding. It is locatable: Rule names the
// validation rule that produced it, Module the offending module (empty for
// global findings) and Field a dotted path within the module (for example
// "http.valid_status_codes[0]").
type Diagnostic struct {
	Rule     string
	Severity Severity
	Module   string
	Field    string
	Message  string
}

// Location renders the configuration path the diagnostic points at, for
// example "modules.http_2xx.http.fail_if_ssl".
func (d Diagnostic) Location() string {
	switch {
	case d.Module != "" && d.Field != "":
		return "modules." + d.Module + "." + d.Field
	case d.Module != "":
		return "modules." + d.Module
	case d.Field != "":
		return d.Field
	default:
		return "<global>"
	}
}

// Error implements the error interface.
func (d Diagnostic) Error() string {
	return fmt.Sprintf("[%s] rule %q (%s): %s", d.Severity, d.Rule, d.Location(), d.Message)
}

// Diagnostics is a collection of Diagnostic values. Its Error method only
// reports error-severity findings; warnings are available via Warnings.
type Diagnostics []Diagnostic

// Error joins all error-severity diagnostics.
func (ds Diagnostics) Error() string {
	var b strings.Builder
	for _, d := range ds {
		if d.Severity != SeverityError {
			continue
		}
		if b.Len() > 0 {
			b.WriteByte('\n')
		}
		b.WriteString(d.Error())
	}
	return b.String()
}

// HasErrors reports whether the collection contains an error-severity finding.
func (ds Diagnostics) HasErrors() bool {
	for _, d := range ds {
		if d.Severity == SeverityError {
			return true
		}
	}
	return false
}

// Errors returns the error-severity diagnostics.
func (ds Diagnostics) Errors() []Diagnostic {
	var out []Diagnostic
	for _, d := range ds {
		if d.Severity == SeverityError {
			out = append(out, d)
		}
	}
	return out
}

// Warnings returns the warning-severity diagnostics.
func (ds Diagnostics) Warnings() []Diagnostic {
	var out []Diagnostic
	for _, d := range ds {
		if d.Severity == SeverityWarning {
			out = append(out, d)
		}
	}
	return out
}

// AsDiagnostics extracts the Diagnostics carried by err, if any.
func AsDiagnostics(err error) (Diagnostics, bool) {
	if ds, ok := errors.AsType[Diagnostics](err); ok {
		return ds, true
	}
	return nil, false
}

// Errorf builds an error-severity diagnostic.
func Errorf(rule, module, field, format string, args ...any) Diagnostic {
	return Diagnostic{Rule: rule, Severity: SeverityError, Module: module, Field: field, Message: fmt.Sprintf(format, args...)}
}

// Warningf builds a warning-severity diagnostic.
func Warningf(rule, module, field, format string, args ...any) Diagnostic {
	return Diagnostic{Rule: rule, Severity: SeverityWarning, Module: module, Field: field, Message: fmt.Sprintf(format, args...)}
}

// ValidationContext is passed to every validation rule.
type ValidationContext struct {
	// Config is the candidate configuration that is being validated. Rules
	// must not mutate it.
	Config *Config
	// Logger may be nil.
	Logger *slog.Logger
}

// Rule is a pluggable configuration validation rule.
//
// Rules with a lower Priority value run first. Validate returns diagnostics
// rather than an error for invalid configuration: a non-nil error is reserved
// for internal rule failures (a panic while evaluating, a broken regexp
// compiler, ...).
type Rule interface {
	Name() string
	Priority() int
	Validate(*ValidationContext) (Diagnostics, error)
}

// FuncRule adapts a function to the Rule interface.
type FuncRule struct {
	name     string
	priority int
	fn       func(*ValidationContext) (Diagnostics, error)
}

// NewRuleFunc creates a Rule from a name, priority and validation function.
func NewRuleFunc(name string, priority int, fn func(*ValidationContext) (Diagnostics, error)) Rule {
	return &FuncRule{name: name, priority: priority, fn: fn}
}

// Name implements Rule.
func (r *FuncRule) Name() string { return r.name }

// Priority implements Rule.
func (r *FuncRule) Priority() int { return r.priority }

// Validate implements Rule.
func (r *FuncRule) Validate(ctx *ValidationContext) (Diagnostics, error) {
	return r.fn(ctx)
}

// ruleSnapshot is an immutable, concurrency-safe view of the registered
// rules. In-flight validations keep using the snapshot they started with even
// while the registry is hot-updated.
type ruleSnapshot struct {
	// ordered is sorted by ascending priority, then by name for determinism.
	ordered []Rule
	byName  map[string]Rule
}

// Registry is a pluggable, hot-updatable set of validation rules.
//
// Read operations (Validate, Rules, Get) are lock-free and always operate on
// an immutable snapshot; write operations (Register, Unregister, Replace)
// build a new snapshot that is published atomically. A failed write never
// touches the active snapshot, and Replace returns a rollback function that
// restores the previous snapshot.
type Registry struct {
	mu     sync.Mutex
	logger *slog.Logger
	snap   atomic.Pointer[ruleSnapshot]
}

// NewRegistry creates an empty rule registry.
func NewRegistry(logger *slog.Logger) *Registry {
	r := &Registry{logger: logger}
	r.snap.Store(&ruleSnapshot{byName: map[string]Rule{}})
	return r
}

// NewDefaultRegistry creates a registry pre-loaded with the built-in rules.
func NewDefaultRegistry(logger *slog.Logger) *Registry {
	r := NewRegistry(logger)
	if err := r.replace(BuiltInRules()); err != nil {
		// BuiltInRules is static and always valid; reaching this point is a
		// programming error.
		panic(err)
	}
	return r
}

func (r *Registry) log() *slog.Logger {
	if r.logger != nil {
		return r.logger
	}
	return slog.Default()
}

// compileRules builds an immutable snapshot from the supplied rules, rejecting
// nil rules, empty names and duplicate names.
func compileRules(rules []Rule) (*ruleSnapshot, error) {
	snap := &ruleSnapshot{
		ordered: make([]Rule, 0, len(rules)),
		byName:  make(map[string]Rule, len(rules)),
	}
	for i, rule := range rules {
		if rule == nil {
			return nil, fmt.Errorf("cannot register nil rule at position %d", i)
		}
		name := rule.Name()
		if name == "" {
			return nil, fmt.Errorf("cannot register rule at position %d with an empty name", i)
		}
		if _, exists := snap.byName[name]; exists {
			return nil, fmt.Errorf("duplicate validation rule %q", name)
		}
		snap.byName[name] = rule
		snap.ordered = append(snap.ordered, rule)
	}
	sort.SliceStable(snap.ordered, func(i, j int) bool {
		pi, pj := snap.ordered[i].Priority(), snap.ordered[j].Priority()
		if pi != pj {
			return pi < pj
		}
		return snap.ordered[i].Name() < snap.ordered[j].Name()
	})
	return snap, nil
}

// replace publishes snap as the active snapshot. The caller must hold r.mu.
func (r *Registry) publish(snap *ruleSnapshot) {
	r.snap.Store(snap)
}

// replace installs rules as the new active snapshot. It is the unchecked
// building block used by the exported mutators.
func (r *Registry) replace(rules []Rule) error {
	snap, err := compileRules(rules)
	if err != nil {
		return err
	}
	r.mu.Lock()
	r.publish(snap)
	r.mu.Unlock()
	return nil
}

// Register adds a rule or hot-replaces the existing rule carrying the same
// name. The replacement becomes visible atomically; validations already in
// flight finish against the previous snapshot.
func (r *Registry) Register(rule Rule) error {
	if rule == nil {
		return errors.New("cannot register nil rule")
	}
	if rule.Name() == "" {
		return errors.New("cannot register a rule with an empty name")
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	current := r.snap.Load()
	rules := make([]Rule, 0, len(current.ordered)+1)
	replaced := false
	for _, existing := range current.ordered {
		if existing.Name() == rule.Name() {
			rules = append(rules, rule)
			replaced = true
			continue
		}
		rules = append(rules, existing)
	}
	if !replaced {
		rules = append(rules, rule)
	}

	snap, err := compileRules(rules)
	if err != nil {
		return err
	}
	r.publish(snap)
	return nil
}

// Unregister removes the rule with the given name. It reports whether a rule
// was removed. The change is published atomically.
func (r *Registry) Unregister(name string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	current := r.snap.Load()
	if _, exists := current.byName[name]; !exists {
		return false
	}
	rules := make([]Rule, 0, len(current.ordered)-1)
	for _, rule := range current.ordered {
		if rule.Name() != name {
			rules = append(rules, rule)
		}
	}
	// compileRules cannot fail: it is a subset of an already-valid snapshot.
	snap, err := compileRules(rules)
	if err != nil {
		panic(err)
	}
	r.publish(snap)
	return true
}

// Replace atomically hot-swaps the whole rule set. If candidates do not
// compile (nil rule, empty or duplicate name) the active rule set is left
// untouched and a non-nil error is returned. On success the returned function
// rolls back to the previous snapshot; it is safe to call once.
func (r *Registry) Replace(candidates []Rule) (func(), error) {
	snap, err := compileRules(candidates)
	if err != nil {
		return nil, err
	}

	r.mu.Lock()
	previous := r.snap.Load()
	r.publish(snap)
	r.mu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			r.mu.Lock()
			// Only restore if nobody replaced the rule set in the meantime.
			if r.snap.Load() == snap {
				r.publish(previous)
			}
			r.mu.Unlock()
		})
	}, nil
}

// Get returns the active rule with the given name.
func (r *Registry) Get(name string) (Rule, bool) {
	rule, ok := r.snap.Load().byName[name]
	return rule, ok
}

// Rules returns a copy of the active rules ordered by ascending priority.
// Callers may retain the slice: it is not affected by later hot updates.
func (r *Registry) Rules() []Rule {
	current := r.snap.Load()
	out := make([]Rule, len(current.ordered))
	copy(out, current.ordered)
	return out
}

// validateWith runs every rule in snap in priority order and aggregates the
// diagnostics. An internal rule error aborts validation and is reported.
func (r *Registry) validateWith(snap *ruleSnapshot, cfg *Config) (Diagnostics, error) {
	if cfg == nil {
		return nil, errors.New("cannot validate a nil configuration")
	}

	ctx := &ValidationContext{Config: cfg, Logger: r.logger}
	var diags Diagnostics
	for _, rule := range snap.ordered {
		ruleDiags, err := rule.Validate(ctx)
		if err != nil {
			return diags, fmt.Errorf("validation rule %q failed: %w", rule.Name(), err)
		}
		for _, d := range ruleDiags {
			if d.Rule == "" {
				d.Rule = rule.Name()
			}
			if d.Severity == SeverityWarning && r.logger != nil {
				r.log().Warn("Configuration validation warning",
					"rule", d.Rule, "module", d.Module, "field", d.Field, "message", d.Message)
			}
			diags = append(diags, d)
		}
	}
	return diags, nil
}

// Validate runs the active rule set against cfg in priority order. It returns
// all collected diagnostics (errors and warnings) and a non-nil error if any
// error-severity diagnostic was produced or a rule failed internally.
func (r *Registry) Validate(cfg *Config) (Diagnostics, error) {
	diags, err := r.validateWith(r.snap.Load(), cfg)
	if err != nil {
		return diags, err
	}
	if diags.HasErrors() {
		return diags, diags
	}
	return diags, nil
}

// DryRun compiles candidates and validates cfg against them without changing
// the active rule set. It is used to check a hot update before committing it.
func (r *Registry) DryRun(candidates []Rule, cfg *Config) (Diagnostics, error) {
	snap, err := compileRules(candidates)
	if err != nil {
		return nil, err
	}
	diags, err := r.validateWith(snap, cfg)
	if err != nil {
		return diags, err
	}
	if diags.HasErrors() {
		return diags, diags
	}
	return diags, nil
}
