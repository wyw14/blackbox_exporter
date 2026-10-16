// Copyright 2025 The Prometheus Authors
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
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

const (
	configWithTwoModules = `modules:
  http_2xx:
    prober: http
    timeout: 5s
  dns_test:
    prober: dns
    dns:
      query_name: example.com
`
	// dns_test is deleted, http_2xx is kept and a new module is added.
	configWithDeletedModule = `modules:
  http_2xx:
    prober: http
    timeout: 5s
  tcp_connect:
    prober: tcp
    timeout: 5s
`
	invalidConfig = "modules:\n  broken:\n    prober: not-a-real-prober\n"
)

func writeTempConfig(t *testing.T, content string) string {
	t.Helper()
	filePath := filepath.Join(t.TempDir(), "blackbox.yml")
	if err := os.WriteFile(filePath, []byte(content), 0o644); err != nil {
		t.Fatalf("Could not write config file: %v", err)
	}
	return filePath
}

// TestReloadConfigParseErrorKeepsOldConfig verifies that a new
// configuration which fails to parse never replaces the configuration
// that is currently serving probes, and that the failure is reported via
// the reload metric.
func TestReloadConfigParseErrorKeepsOldConfig(t *testing.T) {
	sc := NewSafeConfig(prometheus.NewRegistry())
	confFile := writeTempConfig(t, configWithTwoModules)

	if err := sc.ReloadConfig(confFile, nil); err != nil {
		t.Fatalf("initial ReloadConfig failed: %v", err)
	}
	oldSnapshot := sc.GetConfig()
	if _, ok := oldSnapshot.Modules["dns_test"]; !ok {
		t.Fatal("expected dns_test module in initial configuration")
	}
	if got := testutil.ToFloat64(sc.configReloadSuccess); got != 1 {
		t.Fatalf("config_last_reload_successful = %f after successful load, want 1", got)
	}

	if err := os.WriteFile(confFile, []byte(invalidConfig), 0o644); err != nil {
		t.Fatalf("Could not overwrite config file: %v", err)
	}

	if err := sc.ReloadConfig(confFile, nil); err == nil {
		t.Fatal("expected an error when reloading an invalid configuration, got nil")
	}

	// The active snapshot must be unchanged.
	if sc.GetConfig() != oldSnapshot {
		t.Fatal("active configuration was replaced despite a parse error")
	}
	if _, ok := sc.GetConfig().Modules["dns_test"]; !ok {
		t.Fatal("old configuration stopped serving after a failed reload")
	}
	if got := testutil.ToFloat64(sc.configReloadSuccess); got != 0 {
		t.Fatalf("config_last_reload_successful = %f after failed reload, want 0", got)
	}

	// Once a valid configuration is published again, probing recovers.
	if err := os.WriteFile(confFile, []byte(configWithTwoModules), 0o644); err != nil {
		t.Fatalf("Could not restore config file: %v", err)
	}
	if err := sc.ReloadConfig(confFile, nil); err != nil {
		t.Fatalf("ReloadConfig after restoring valid config failed: %v", err)
	}
	if got := testutil.ToFloat64(sc.configReloadSuccess); got != 1 {
		t.Fatalf("config_last_reload_successful = %f after recovery, want 1", got)
	}
}

// TestReloadConfigModuleDeletionAndInFlightSnapshot verifies that deleting
// a module from the configuration publishes a new snapshot which no longer
// contains it, while probes that captured the previous snapshot keep using
// the deleted module until they finish.
func TestReloadConfigModuleDeletionAndInFlightSnapshot(t *testing.T) {
	sc := NewSafeConfig(prometheus.NewRegistry())
	confFile := writeTempConfig(t, configWithTwoModules)

	if err := sc.ReloadConfig(confFile, nil); err != nil {
		t.Fatalf("initial ReloadConfig failed: %v", err)
	}
	// Simulate a probe that started before the reload holding on to its
	// configuration snapshot.
	inFlightSnapshot := sc.GetConfig()

	if err := os.WriteFile(confFile, []byte(configWithDeletedModule), 0o644); err != nil {
		t.Fatalf("Could not overwrite config file: %v", err)
	}
	if err := sc.ReloadConfig(confFile, nil); err != nil {
		t.Fatalf("ReloadConfig after deleting a module failed: %v", err)
	}

	newSnapshot := sc.GetConfig()
	if newSnapshot == inFlightSnapshot {
		t.Fatal("expected a new configuration snapshot after reload")
	}
	if _, ok := newSnapshot.Modules["dns_test"]; ok {
		t.Fatal("deleted module dns_test is still served by the new configuration")
	}
	if _, ok := newSnapshot.Modules["tcp_connect"]; !ok {
		t.Fatal("new module tcp_connect missing from the new configuration")
	}
	if _, ok := newSnapshot.Modules["http_2xx"]; !ok {
		t.Fatal("unchanged module http_2xx missing from the new configuration")
	}

	// The probe already in flight keeps the full old configuration.
	if _, ok := inFlightSnapshot.Modules["dns_test"]; !ok {
		t.Fatal("in-flight probe lost its module after the module was deleted")
	}
}

// TestReloadConfigUnchangedFileIsNoOp verifies that reloading an unchanged
// file does not replace the running configuration.
func TestReloadConfigUnchangedFileIsNoOp(t *testing.T) {
	sc := NewSafeConfig(prometheus.NewRegistry())
	confFile := writeTempConfig(t, configWithTwoModules)

	if err := sc.ReloadConfig(confFile, nil); err != nil {
		t.Fatalf("initial ReloadConfig failed: %v", err)
	}
	snapshot := sc.GetConfig()

	if err := sc.ReloadConfig(confFile, nil); err != nil {
		t.Fatalf("reloading an unchanged configuration failed: %v", err)
	}
	if sc.GetConfig() != snapshot {
		t.Fatal("unchanged configuration was needlessly replaced")
	}
}

// TestReloadConfigConcurrent exercises concurrent reloads, including
// alternating valid and invalid configurations, alongside concurrent
// readers. It is meant to be run with the race detector: every reader
// must always observe a complete, immutable snapshot.
func TestReloadConfigConcurrent(t *testing.T) {
	sc := NewSafeConfig(prometheus.NewRegistry())
	confFile := writeTempConfig(t, configWithTwoModules)
	if err := sc.ReloadConfig(confFile, nil); err != nil {
		t.Fatalf("initial ReloadConfig failed: %v", err)
	}

	contents := []string{configWithTwoModules, configWithDeletedModule, invalidConfig}

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Writers/reloaders: each picks a configuration variant, writes it
	// and asks for a reload.
	for id := range 4 {
		wg.Go(func() {
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				content := contents[(id+i)%len(contents)]
				if err := os.WriteFile(confFile, []byte(content), 0o644); err != nil {
					t.Errorf("Could not write config file: %v", err)
					return
				}
				// Errors are expected for the invalid variant; the old
				// configuration must survive them.
				_ = sc.ReloadConfig(confFile, nil)
			}
		})
	}

	// Readers: grab a snapshot and read it entirely. A snapshot is
	// immutable, so a concurrent reload must never disturb it.
	for range 8 {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				c := sc.GetConfig()
				for name, module := range c.Modules {
					_ = name
					_ = module.Prober
				}
			}
		})
	}

	time.Sleep(500 * time.Millisecond)
	close(stop)
	wg.Wait()

	// Leave the exporter with a known-good configuration.
	if err := os.WriteFile(confFile, []byte(configWithTwoModules), 0o644); err != nil {
		t.Fatalf("Could not restore config file: %v", err)
	}
	if err := sc.ReloadConfig(confFile, nil); err != nil {
		t.Fatalf("final ReloadConfig failed: %v", err)
	}
}
