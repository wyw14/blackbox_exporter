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
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.yaml.in/yaml/v3"
)

// reloadSuccessGauge reads the config_last_reload_successful gauge registered
// on reg, without pulling in the client_golang testutil dependency.
func reloadSuccessGauge(t *testing.T, reg prometheus.Gatherer) float64 {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	for _, family := range families {
		if family.GetName() == "blackbox_exporter_config_last_reload_successful" {
			for _, metric := range family.GetMetric() {
				return metric.GetGauge().GetValue()
			}
		}
	}
	t.Fatal("config_last_reload_successful gauge not found")
	return 0
}

// --- test helpers -----------------------------------------------------------

func mustDecodeConfig(t *testing.T, raw string) *Config {
	t.Helper()
	c := &Config{}
	if err := yaml.Unmarshal([]byte(raw), c); err != nil {
		t.Fatalf("failed to decode config: %v", err)
	}
	return c
}

func writeTempConfig(t *testing.T, name, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("failed to write %s: %v", path, err)
	}
	return path
}

// recordingRule appends its name to order every time it runs.
func recordingRule(name string, priority int, order *[]string, mu *sync.Mutex) Rule {
	return NewRuleFunc(name, priority, func(*ValidationContext) (Diagnostics, error) {
		mu.Lock()
		*order = append(*order, name)
		mu.Unlock()
		return nil, nil
	})
}

func staticRule(name string, priority int, diags Diagnostics) Rule {
	return NewRuleFunc(name, priority, func(*ValidationContext) (Diagnostics, error) {
		return diags, nil
	})
}

// --- priority ---------------------------------------------------------------

func TestRegistryPriorityOrder(t *testing.T) {
	var (
		mu    sync.Mutex
		order []string
	)
	r := NewRegistry(nil)
	for _, rule := range []Rule{
		recordingRule("late", 300, &order, &mu),
		recordingRule("zeta-same-prio", 100, &order, &mu),
		recordingRule("early", 110, &order, &mu),
		recordingRule("alpha-same-prio", 100, &order, &mu),
	} {
		if err := r.Register(rule); err != nil {
			t.Fatalf("Register: %v", err)
		}
	}

	if _, err := r.Validate(&Config{Modules: map[string]Module{}}); err != nil {
		t.Fatalf("Validate: %v", err)
	}

	want := []string{"alpha-same-prio", "zeta-same-prio", "early", "late"}
	if !slices.Equal(order, want) {
		t.Fatalf("execution order = %v, want %v", order, want)
	}

	got := r.Rules()
	gotNames := make([]string, len(got))
	for i, rule := range got {
		gotNames[i] = rule.Name()
	}
	if !slices.Equal(gotNames, want) {
		t.Fatalf("Rules() order = %v, want %v", gotNames, want)
	}
}

// --- hot registration / replacement / removal ------------------------------

func TestRegistryRegisterHotReplaceAndUnregister(t *testing.T) {
	r := NewRegistry(nil)
	v1 := staticRule("custom", 100, nil)
	if err := r.Register(v1); err != nil {
		t.Fatalf("Register v1: %v", err)
	}
	got, ok := r.Get("custom")
	if !ok || got != v1 {
		t.Fatal("Get did not return the registered rule")
	}

	v2 := staticRule("custom", 100, Diagnostics{Errorf("custom", "m", "f", "v2 rejected")})
	if err := r.Register(v2); err != nil {
		t.Fatalf("Register v2: %v", err)
	}
	if got, _ := r.Get("custom"); got != v2 {
		t.Fatal("Get still returns the old rule after hot replacement")
	}

	diags, err := r.Validate(&Config{Modules: map[string]Module{}})
	if err == nil {
		t.Fatal("expected validation error from v2 rule, got nil")
	}
	if len(diags) != 1 || diags[0].Message != "v2 rejected" {
		t.Fatalf("unexpected diagnostics from replacement rule: %+v", diags)
	}

	if !r.Unregister("custom") {
		t.Fatal("Unregister reported no removal")
	}
	if _, ok := r.Get("custom"); ok {
		t.Fatal("rule still present after Unregister")
	}
	if r.Unregister("custom") {
		t.Fatal("Unregister on missing rule reported removal")
	}
}

func TestRegistryRegisterValidation(t *testing.T) {
	r := NewRegistry(nil)
	if err := r.Register(nil); err == nil {
		t.Fatal("Register(nil) must fail")
	}
	if err := r.Register(NewRuleFunc("", 0, nil)); err == nil {
		t.Fatal("registering a rule with an empty name must fail")
	}
}

// --- Replace + rollback -----------------------------------------------------

func TestRegistryReplaceAndRollback(t *testing.T) {
	r := NewRegistry(nil)
	if err := r.Register(staticRule("a", 100, nil)); err != nil {
		t.Fatal(err)
	}
	if err := r.Register(staticRule("b", 100, nil)); err != nil {
		t.Fatal(err)
	}

	rollback, err := r.Replace([]Rule{staticRule("c", 100, nil)})
	if err != nil {
		t.Fatalf("Replace: %v", err)
	}
	if names := ruleNames(r.Rules()); !slices.Equal(names, []string{"c"}) {
		t.Fatalf("active rules after Replace = %v, want [c]", names)
	}

	rollback()
	if names := ruleNames(r.Rules()); !slices.Equal(names, []string{"a", "b"}) {
		t.Fatalf("active rules after rollback = %v, want [a b]", names)
	}

	// Rollback is idempotent.
	rollback()
	if names := ruleNames(r.Rules()); !slices.Equal(names, []string{"a", "b"}) {
		t.Fatalf("active rules after second rollback = %v, want [a b]", names)
	}
}

func TestRegistryReplaceRejectedLeavesSnapshotUntouched(t *testing.T) {
	r := NewRegistry(nil)
	if err := r.Register(staticRule("a", 100, nil)); err != nil {
		t.Fatal(err)
	}
	original := r.Rules()

	// Duplicate names cannot be compiled.
	if _, err := r.Replace([]Rule{staticRule("x", 1, nil), staticRule("x", 2, nil)}); err == nil {
		t.Fatal("Replace with duplicate rule names must fail")
	}
	// Nil rules cannot be compiled.
	if _, err := r.Replace([]Rule{nil}); err == nil {
		t.Fatal("Replace with nil rule must fail")
	}
	// Empty names cannot be compiled.
	if _, err := r.Replace([]Rule{staticRule("", 1, nil)}); err == nil {
		t.Fatal("Replace with an empty rule name must fail")
	}

	if names := ruleNames(r.Rules()); !slices.Equal(names, []string{"a"}) {
		t.Fatalf("active rules = %v, want [a] untouched", names)
	}
	if !slices.Equal(ruleNames(original), ruleNames(r.Rules())) {
		t.Fatal("snapshot captured before the failed Replace must keep resolving identically")
	}
}

func TestRegistryRollbackSkippedAfterNewerReplace(t *testing.T) {
	r := NewRegistry(nil)
	rollbackA, err := r.Replace([]Rule{staticRule("a", 1, nil)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Replace([]Rule{staticRule("b", 1, nil)}); err != nil {
		t.Fatal(err)
	}
	// Stale rollback must not clobber the newer "b" snapshot.
	rollbackA()
	if names := ruleNames(r.Rules()); !slices.Equal(names, []string{"b"}) {
		t.Fatalf("active rules = %v, want [b]", names)
	}
}

// --- dry-run ----------------------------------------------------------------

func TestRegistryDryRunDoesNotChangeActiveRules(t *testing.T) {
	r := NewRegistry(nil)
	if err := r.Register(staticRule("active", 100, nil)); err != nil {
		t.Fatal(err)
	}

	candidate := []Rule{staticRule("candidate", 100, Diagnostics{
		Errorf("candidate", "m", "f", "nope"),
	})}
	diags, err := r.DryRun(candidate, &Config{Modules: map[string]Module{}})
	if err == nil {
		t.Fatal("DryRun of a rejecting candidate must return an error")
	}
	if len(diags.Errors()) != 1 {
		t.Fatalf("DryRun diagnostics = %+v, want one error", diags)
	}

	if names := ruleNames(r.Rules()); !slices.Equal(names, []string{"active"}) {
		t.Fatalf("active rules changed after DryRun: %v", names)
	}
	if _, ok := r.Get("candidate"); ok {
		t.Fatal("candidate rule leaked into the active registry")
	}

	// Compile failures during dry-run also leave the registry untouched.
	if _, err := r.DryRun([]Rule{staticRule("x", 1, nil), staticRule("x", 2, nil)}, &Config{}); err == nil {
		t.Fatal("DryRun with duplicate rule names must fail")
	}
	if names := ruleNames(r.Rules()); !slices.Equal(names, []string{"active"}) {
		t.Fatalf("active rules changed after failed DryRun: %v", names)
	}
}

func TestRegistryDryRunThenCommitWorkflow(t *testing.T) {
	r := NewRegistry(nil)
	current := &Config{Modules: map[string]Module{}}
	if err := r.Register(staticRule("strict-http", 100, nil)); err != nil {
		t.Fatal(err)
	}

	candidate := []Rule{
		staticRule("new-rule-1", 100, nil),
		staticRule("new-rule-2", 200, Diagnostics{Warningf("new-rule-2", "m", "f", "heads up")}),
	}
	// The candidate accepts the running config (warning only), so commit.
	diags, err := r.DryRun(candidate, current)
	if err != nil {
		t.Fatalf("DryRun: %v", err)
	}
	if len(diags.Warnings()) != 1 {
		t.Fatalf("want one warning diagnostic, got %+v", diags)
	}
	if _, err := r.Replace(candidate); err != nil {
		t.Fatalf("Replace: %v", err)
	}
	if names := ruleNames(r.Rules()); !slices.Equal(names, []string{"new-rule-1", "new-rule-2"}) {
		t.Fatalf("active rules = %v", names)
	}
}

// --- stable snapshots -------------------------------------------------------

func TestRegistryInFlightSnapshotStaysStable(t *testing.T) {
	r := NewDefaultRegistry(nil)
	inFlight := r.Rules()
	before := ruleNames(inFlight)

	rollback, err := r.Replace([]Rule{staticRule("replacement", 1, nil)})
	if err != nil {
		t.Fatal(err)
	}

	// The snapshot the in-flight request holds is immutable and still works.
	if !slices.Equal(ruleNames(inFlight), before) {
		t.Fatalf("in-flight snapshot mutated: %v", inFlight)
	}
	ctx := &ValidationContext{Config: mustDecodeConfig(t, "modules: {}\n")}
	for _, rule := range inFlight {
		if _, err := rule.Validate(ctx); err != nil {
			t.Fatalf("in-flight rule %q stopped working after Replace: %v", rule.Name(), err)
		}
	}

	// The new snapshot is visible to fresh readers.
	if names := ruleNames(r.Rules()); !slices.Equal(names, []string{"replacement"}) {
		t.Fatalf("fresh readers should see the replacement, got %v", names)
	}
	rollback()
}

// --- diagnostics ------------------------------------------------------------

func TestDiagnosticLocation(t *testing.T) {
	tests := []struct {
		name string
		d    Diagnostic
		want string
	}{
		{"module and field", Errorf("r", "http_2xx", "http.fail_if_ssl", "msg"), "modules.http_2xx.http.fail_if_ssl"},
		{"module only", Errorf("r", "http_2xx", "", "msg"), "modules.http_2xx"},
		{"global field", Errorf("r", "", "modules", "msg"), "modules"},
		{"fully global", Errorf("r", "", "", "msg"), "<global>"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.d.Location(); got != tc.want {
				t.Fatalf("Location() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestDiagnosticsAggregation(t *testing.T) {
	ds := Diagnostics{
		Errorf("r1", "m1", "f1", "boom"),
		Warningf("r2", "m2", "f2", "careful"),
		Errorf("r3", "", "", "global boom"),
	}
	if !ds.HasErrors() {
		t.Fatal("HasErrors must be true")
	}
	if len(ds.Errors()) != 2 || len(ds.Warnings()) != 1 {
		t.Fatalf("Errors/Warnings partition wrong: %d errors, %d warnings", len(ds.Errors()), len(ds.Warnings()))
	}
	msg := ds.Error()
	if !strings.Contains(msg, `rule "r1" (modules.m1.f1): boom`) {
		t.Fatalf("error text missing locatable r1 line: %q", msg)
	}
	if strings.Contains(msg, "careful") {
		t.Fatalf("warnings must not appear in Error(): %q", msg)
	}
	if !strings.Contains(msg, "(<global>): global boom") {
		t.Fatalf("error text missing global line: %q", msg)
	}
}

func TestRegistryInternalRuleError(t *testing.T) {
	r := NewRegistry(nil)
	boom := errors.New("compiler exploded")
	r.Register(NewRuleFunc("broken", 1, func(*ValidationContext) (Diagnostics, error) {
		return nil, boom
	}))
	_, err := r.Validate(&Config{})
	if err == nil || !strings.Contains(err.Error(), `validation rule "broken" failed`) {
		t.Fatalf("expected wrapped internal rule error, got %v", err)
	}
	if !errors.Is(err, boom) {
		t.Fatal("internal rule error must be unwrappable with errors.Is")
	}
}

// --- built-in rules ---------------------------------------------------------

func TestBuiltInRulesAcceptGoodConfig(t *testing.T) {
	r := NewDefaultRegistry(nil)
	diags, err := r.Validate(mustDecodeConfig(t, goodConfigYAML))
	if err != nil {
		t.Fatalf("built-in rules rejected a valid config:\n%v", err)
	}
	if len(diags) != 0 {
		t.Fatalf("expected no diagnostics for a clean config, got %+v", diags)
	}
}

func TestBuiltInRuleFindings(t *testing.T) {
	r := NewDefaultRegistry(nil)

	t.Run("empty module name", func(t *testing.T) {
		cfg := mustDecodeConfig(t, "modules:\n  \"\":\n    prober: http\n")
		diags, err := r.Validate(cfg)
		if err == nil {
			t.Fatal("expected error")
		}
		d := singleError(t, diags)
		if d.Rule != RuleModuleName || d.Field != "modules" {
			t.Fatalf("unexpected diagnostic: %+v", d)
		}
	})

	t.Run("negative timeout", func(t *testing.T) {
		cfg := mustDecodeConfig(t, "modules:\n  slow_mo:\n    prober: http\n    timeout: -5s\n")
		diags, err := r.Validate(cfg)
		if err == nil {
			t.Fatal("expected error")
		}
		d := singleError(t, diags)
		if d.Rule != RuleModuleTimeout || d.Module != "slow_mo" || d.Field != "timeout" {
			t.Fatalf("unexpected diagnostic: %+v", d)
		}
		if d.Location() != "modules.slow_mo.timeout" {
			t.Fatalf("location = %q", d.Location())
		}
	})

	t.Run("http ssl contradiction", func(t *testing.T) {
		cfg := mustDecodeConfig(t, `
modules:
  confused:
    prober: http
    http:
      fail_if_ssl: true
      fail_if_not_ssl: true
`)
		diags, err := r.Validate(cfg)
		if err == nil {
			t.Fatal("expected error")
		}
		d := singleError(t, diags)
		if d.Rule != RuleHTTPSSLConflict || d.Field != "http.fail_if_ssl" {
			t.Fatalf("unexpected diagnostic: %+v", d)
		}
	})

	t.Run("foreign probe section is a warning", func(t *testing.T) {
		cfg := mustDecodeConfig(t, `
modules:
  weird:
    prober: tcp
    timeout: 5s
    tcp:
    http:
      method: GET
`)
		diags, err := r.Validate(cfg)
		if err != nil {
			t.Fatalf("warnings must not fail validation: %v", err)
		}
		warnings := diags.Warnings()
		if len(warnings) != 1 {
			t.Fatalf("want exactly one warning, got %+v", diags)
		}
		w := warnings[0]
		if w.Rule != RuleProbeSectionMismatch || w.Module != "weird" || w.Field != "http" {
			t.Fatalf("unexpected warning: %+v", w)
		}
	})

	t.Run("diagnostics are priority ordered", func(t *testing.T) {
		cfg := mustDecodeConfig(t, `
modules:
  bad:
    prober: http
    timeout: -5s
    http:
      fail_if_ssl: true
      fail_if_not_ssl: true
`)
		diags, err := r.Validate(cfg)
		if err == nil {
			t.Fatal("expected error")
		}
		errs := diags.Errors()
		if len(errs) != 2 {
			t.Fatalf("want 2 errors, got %+v", errs)
		}
		if errs[0].Rule != RuleModuleTimeout || errs[1].Rule != RuleHTTPSSLConflict {
			t.Fatalf("expected timeout (prio %d) before ssl conflict (prio %d), got %q then %q",
				PriorityModuleTimeout, PriorityHTTPSSLConflict, errs[0].Rule, errs[1].Rule)
		}
	})
}

func singleError(t *testing.T, diags Diagnostics) Diagnostic {
	t.Helper()
	errs := diags.Errors()
	if len(errs) != 1 {
		t.Fatalf("want exactly one error diagnostic, got %+v", diags)
	}
	return errs[0]
}

const goodConfigYAML = `
modules:
  http_2xx:
    prober: http
    timeout: 5s
    http:
  tcp_connect:
    prober: tcp
    timeout: 5s
`

// --- concurrency ------------------------------------------------------------

func TestRegistryConcurrentValidationAndHotUpdates(t *testing.T) {
	r := NewDefaultRegistry(nil)
	// Zero-value modules are fine for the custom mutating rules below, which
	// only iterate module names.
	cfg := &Config{Modules: map[string]Module{"http_2xx": {Prober: "http"}}}

	const readers = 8
	const writers = 4
	var (
		wg    sync.WaitGroup
		stop  atomic.Bool
		reads atomic.Int64
	)

	for range readers {
		wg.Go(func() {
			for !stop.Load() {
				if diags, err := r.Validate(cfg); err != nil {
					// Built-in rules must never flag this fixed valid config.
					t.Errorf("concurrent Validate failed: %v (%+v)", err, diags)
					return
				}
				_ = r.Rules()
				if _, ok := r.Get(RuleModuleTimeout); ok {
					_ = reads.Add(1)
				}
			}
		})
	}

	for w := range writers {
		wg.Go(func() {
			for j := 0; j < 100 && !stop.Load(); j++ {
				name := fmt.Sprintf("hot-rule-%d", j%4)
				if err := r.Register(staticRule(name, (w+j)%5, nil)); err != nil {
					t.Errorf("concurrent Register: %v", err)
					return
				}
				_ = r.Unregister(name)
				rollback, err := r.Replace(BuiltInRules())
				if err != nil {
					t.Errorf("concurrent Replace: %v", err)
					return
				}
				rollback()
			}
		})
	}

	// Give the readers a chance to observe many write generations.
	time.Sleep(100 * time.Millisecond)
	stop.Store(true)
	wg.Wait()

	if reads.Load() == 0 {
		t.Fatal("no concurrent reads observed the rule set")
	}
	// Writers finish without stop being set mid-iteration; registry must be usable.
	if _, err := r.Validate(mustDecodeConfig(t, goodConfigYAML)); err != nil {
		t.Fatalf("registry unusable after concurrent hot updates: %v", err)
	}
}

func TestSafeConfigConcurrentReadAndReload(t *testing.T) {
	sc := NewSafeConfig(prometheus.NewRegistry())

	// Several valid generations: each shares http_2xx and carries exactly one
	// distinct marker module, so readers can assert snapshots are never torn.
	var goodPaths []string
	for i := range 4 {
		content := fmt.Sprintf(`
modules:
  http_2xx:
    prober: http
    timeout: 5s
  generation_%d:
    prober: tcp
    timeout: 5s
`, i)
		goodPaths = append(goodPaths, writeTempConfig(t, fmt.Sprintf("good%d.yml", i), content))
	}
	badPath := writeTempConfig(t, "bad.yml", "modules:\n  bad:\n    prober: http\n    timeout: -5s\n")

	if err := sc.ReloadConfig(goodPaths[0], nil); err != nil {
		t.Fatal(err)
	}

	const n = 8
	var wg sync.WaitGroup
	var reads atomic.Int64

	for range n {
		wg.Go(func() {
			snap := sc.GetConfig()
			for range 1000 {
				c := sc.GetConfig()
				if c == nil {
					t.Error("GetConfig returned nil during concurrent reload")
					return
				}
				// Every installed generation contains http_2xx and exactly one
				// generation_* marker: a torn/half-installed snapshot would fail this.
				if _, ok := c.Modules["http_2xx"]; !ok {
					t.Error("snapshot is missing the common module")
					return
				}
				markers := 0
				for name := range c.Modules {
					if strings.HasPrefix(name, "generation_") {
						markers++
					}
				}
				if markers != 1 {
					t.Errorf("snapshot has %d generation markers, want exactly 1", markers)
					return
				}
				// The snapshot captured at request start stays immutable forever.
				if _, ok := snap.Modules["http_2xx"]; !ok {
					t.Error("in-flight snapshot lost the originally loaded module")
					return
				}
				_ = sc.Registry().Rules()
				reads.Add(1)
			}
		})
	}

	wg.Go(func() {
		for i := range 200 {
			// Invalid candidates must always roll back.
			if err := sc.ReloadConfig(badPath, nil); err == nil {
				t.Error("reload of the invalid config unexpectedly succeeded")
				return
			}
			// Each valid generation differs from its siblings, so this performs
			// a real atomic swap.
			if err := sc.ReloadConfig(goodPaths[i%len(goodPaths)], nil); err != nil {
				t.Error(err)
				return
			}
		}
	})

	wg.Wait()
	if reads.Load() == 0 {
		t.Fatal("no concurrent reads happened")
	}
}

// --- SafeConfig reload rollback / dry-run -----------------------------------

func TestSafeConfigReloadRollbackOnInvalidCandidate(t *testing.T) {
	reg := prometheus.NewRegistry()
	sc := NewSafeConfig(reg)
	goodPath := writeTempConfig(t, "good.yml", `
modules:
  keep_me:
    prober: http
    timeout: 5s
`)
	if err := sc.ReloadConfig(goodPath, nil); err != nil {
		t.Fatalf("initial load: %v", err)
	}
	activeBefore := sc.GetConfig()
	if reloadSuccessGauge(t, reg) != 1 {
		t.Fatal("reload-success gauge must be 1 after a successful load")
	}

	badPath := writeTempConfig(t, "bad.yml", `
modules:
  bad:
    prober: http
    timeout: -5s
`)
	err := sc.ReloadConfig(badPath, nil)
	if err == nil {
		t.Fatal("invalid config must fail to reload")
	}

	// Diagnostics must be locatable and extractable.
	diags, ok := AsDiagnostics(err)
	if !ok {
		t.Fatalf("reload error does not carry diagnostics: %v", err)
	}
	d := singleError(t, diags)
	if d.Rule != RuleModuleTimeout || d.Module != "bad" || d.Field != "timeout" {
		t.Fatalf("unexpected diagnostic: %+v", d)
	}
	if !strings.Contains(err.Error(), "modules.bad.timeout") {
		t.Fatalf("reload error must show the offending location: %v", err)
	}

	// Rollback: the previous snapshot stays installed, pointer-identically.
	activeAfter := sc.GetConfig()
	if activeAfter != activeBefore {
		t.Fatal("active config pointer changed despite the failed reload (rollback broken)")
	}
	if _, ok := activeAfter.Modules["keep_me"]; !ok {
		t.Fatal("previously active module vanished after failed reload")
	}
	if _, ok := activeAfter.Modules["bad"]; ok {
		t.Fatal("invalid module leaked into the active config")
	}
	if reloadSuccessGauge(t, reg) != 0 {
		t.Fatal("reload-success gauge must be 0 after a failed load")
	}

	// A repeated load of the unchanged good file is a checksum no-op and succeeds.
	if err := sc.ReloadConfig(goodPath, nil); err != nil {
		t.Fatalf("good config must still load after rollback: %v", err)
	}
}

func TestSafeConfigSuccessfulReloadSwapsSnapshotAtomically(t *testing.T) {
	sc := NewSafeConfig(prometheus.NewRegistry())
	path1 := writeTempConfig(t, "c1.yml", `
modules:
  first:
    prober: http
    timeout: 5s
`)
	path2 := writeTempConfig(t, "c2.yml", `
modules:
  second:
    prober: tcp
    timeout: 5s
`)
	if err := sc.ReloadConfig(path1, nil); err != nil {
		t.Fatal(err)
	}
	inFlight := sc.GetConfig()

	if err := sc.ReloadConfig(path2, nil); err != nil {
		t.Fatalf("second load: %v", err)
	}

	// Fresh requests see the new generation; the in-flight request keeps the old.
	if _, ok := sc.GetConfig().Modules["second"]; !ok {
		t.Fatal("new generation not visible after successful reload")
	}
	if _, ok := inFlight.Modules["first"]; !ok {
		t.Fatal("in-flight snapshot mutated by the successful reload")
	}
	if _, ok := inFlight.Modules["second"]; ok {
		t.Fatal("in-flight snapshot must not contain the newly loaded module")
	}
}

func TestSafeConfigDryRunConfigDoesNotInstall(t *testing.T) {
	sc := NewSafeConfig(prometheus.NewRegistry())
	goodPath := writeTempConfig(t, "good.yml", goodConfigYAML)

	diags, err := sc.DryRunConfig(goodPath, nil)
	if err != nil {
		t.Fatalf("DryRunConfig: %v", err)
	}
	if len(diags) != 0 {
		t.Fatalf("unexpected dry-run diagnostics: %+v", diags)
	}
	if len(sc.GetConfig().Modules) != 0 {
		t.Fatal("DryRunConfig must not install the candidate config")
	}

	badPath := writeTempConfig(t, "bad.yml", `
modules:
  bad:
    prober: http
    http:
      fail_if_ssl: true
      fail_if_not_ssl: true
`)
	diags, err = sc.DryRunConfig(badPath, nil)
	if err == nil {
		t.Fatal("DryRunConfig must reject the invalid config")
	}
	d := singleError(t, diags)
	if d.Location() != "modules.bad.http.fail_if_ssl" {
		t.Fatalf("dry-run diagnostic not locatable: %+v", d)
	}
	if len(sc.GetConfig().Modules) != 0 {
		t.Fatal("failed DryRunConfig must not install the candidate config")
	}
}

func TestSafeConfigWarningsDoNotBlockReload(t *testing.T) {
	sc := NewSafeConfig(prometheus.NewRegistry())
	path := writeTempConfig(t, "warn.yml", `
modules:
  weird:
    prober: tcp
    timeout: 5s
    tcp:
    http:
      method: GET
`)
	diags, err := sc.DryRunConfig(path, nil)
	if err != nil {
		t.Fatalf("warnings must not fail dry-run: %v", err)
	}
	if len(diags.Warnings()) != 1 {
		t.Fatalf("want one warning, got %+v", diags)
	}
	if err := sc.ReloadConfig(path, nil); err != nil {
		t.Fatalf("warnings must not block reload: %v", err)
	}
	if _, ok := sc.GetConfig().Modules["weird"]; !ok {
		t.Fatal("warned module must be installed after reload")
	}
}

func TestSafeConfigPluggableRegistry(t *testing.T) {
	sc := NewSafeConfig(prometheus.NewRegistry())
	// Start from an empty registry: everything that parses is accepted.
	sc.SetRegistry(NewRegistry(nil))
	path := writeTempConfig(t, "bad.yml", `
modules:
  bad:
    prober: http
    timeout: -5s
`)
	if err := sc.ReloadConfig(path, nil); err != nil {
		t.Fatalf("empty registry must accept the config, got: %v", err)
	}

	// Plug in a strict custom rule and make the next reload fail, proving the
	// registry itself is hot-pluggable.
	if err := sc.Registry().Register(NewRuleFunc("only-http", 1, func(ctx *ValidationContext) (Diagnostics, error) {
		var diags Diagnostics
		for name, module := range ctx.Config.Modules {
			if module.Prober != "http" {
				diags = append(diags, Errorf("only-http", name, "prober", "only http probers allowed"))
			}
		}
		return diags, nil
	})); err != nil {
		t.Fatal(err)
	}
	tcpPath := writeTempConfig(t, "tcp.yml", `
modules:
  t:
    prober: tcp
    timeout: 5s
`)
	err := sc.ReloadConfig(tcpPath, nil)
	if err == nil || !strings.Contains(err.Error(), `rule "only-http" (modules.t.prober)`) {
		t.Fatalf("custom rule did not take effect: %v", err)
	}
}

func ruleNames(rules []Rule) []string {
	names := make([]string, 0, len(rules))
	for _, rule := range rules {
		names = append(names, rule.Name())
	}
	return names
}
