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
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

const configWithICMPModule = `modules:
  http_2xx:
    prober: http
    timeout: 5s
  icmp_test:
    prober: icmp
`

func waitFor(t *testing.T, what string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func hasModule(sc *SafeConfig, name string) bool {
	_, ok := sc.GetConfig().Modules[name]
	return ok
}

// TestWatchConfigReloadsOnChange verifies that the file watcher reloads
// the configuration both for an in-place rewrite and for an atomic save
// (write to a temporary file then rename), and that a configuration
// written through the watcher which fails to parse leaves the previously
// active configuration in place.
func TestWatchConfigReloadsOnChange(t *testing.T) {
	sc := NewSafeConfig(prometheus.NewRegistry())
	confFile := writeTempConfig(t, configWithTwoModules)
	if err := sc.ReloadConfig(confFile, nil); err != nil {
		t.Fatalf("initial ReloadConfig failed: %v", err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	if err := sc.WatchConfig(ctx, confFile, 0, slog.Default()); err != nil {
		t.Fatalf("WatchConfig failed to start: %v", err)
	}

	// In-place rewrite.
	if err := os.WriteFile(confFile, []byte(configWithDeletedModule), 0o644); err != nil {
		t.Fatalf("Could not overwrite config file: %v", err)
	}
	waitFor(t, "configuration reloaded after in-place rewrite", func() bool {
		return hasModule(sc, "tcp_connect") && !hasModule(sc, "dns_test")
	})

	// Atomic save via temporary file and rename.
	tmpFile := confFile + ".tmp"
	if err := os.WriteFile(tmpFile, []byte(configWithICMPModule), 0o600); err != nil {
		t.Fatalf("Could not write temporary config file: %v", err)
	}
	if err := os.Rename(tmpFile, confFile); err != nil {
		t.Fatalf("Could not atomically replace config file: %v", err)
	}
	waitFor(t, "configuration reloaded after atomic rename", func() bool {
		return hasModule(sc, "icmp_test") && !hasModule(sc, "tcp_connect")
	})
	goodSnapshot := sc.GetConfig()

	// Invalid configuration delivered through the watcher must not
	// replace the configuration currently serving probes.
	if err := os.WriteFile(confFile, []byte(invalidConfig), 0o644); err != nil {
		t.Fatalf("Could not overwrite config file with invalid content: %v", err)
	}
	waitFor(t, "failed reload to be reported", func() bool {
		return testutil.ToFloat64(sc.configReloadSuccess) == 0
	})
	if sc.GetConfig() != goodSnapshot {
		t.Fatal("active configuration was replaced despite a parse error from the watcher")
	}

	// A valid file is picked up again and the metric recovers.
	if err := os.WriteFile(confFile, []byte(configWithTwoModules), 0o644); err != nil {
		t.Fatalf("Could not restore config file: %v", err)
	}
	waitFor(t, "successful reload to be reported", func() bool {
		return testutil.ToFloat64(sc.configReloadSuccess) == 1 && hasModule(sc, "dns_test")
	})
}

// TestWatchConfigPollingFallback verifies that, when fsnotify events are
// not delivered, the periodic poll detects the changed checksum and
// reloads the configuration.
func TestWatchConfigPollingFallback(t *testing.T) {
	sc := NewSafeConfig(prometheus.NewRegistry())
	confFile := writeTempConfig(t, configWithTwoModules)
	if err := sc.ReloadConfig(confFile, nil); err != nil {
		t.Fatalf("initial ReloadConfig failed: %v", err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	if err := sc.WatchConfig(ctx, confFile, 50*time.Millisecond, slog.Default()); err != nil {
		t.Fatalf("WatchConfig failed to start: %v", err)
	}

	if err := os.WriteFile(confFile, []byte(configWithDeletedModule), 0o644); err != nil {
		t.Fatalf("Could not overwrite config file: %v", err)
	}
	waitFor(t, "configuration reloaded via polling fallback", func() bool {
		return hasModule(sc, "tcp_connect")
	})
}

func TestFileWatcherIsTargetEvent(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "blackbox.yml")
	fw := &fileWatcher{target: filepath.Clean(target)}

	tests := []struct {
		name string
		ev   fsnotify.Event
		want bool
	}{
		{"write to target", fsnotify.Event{Name: target, Op: fsnotify.Write}, true},
		{"atomic create of target", fsnotify.Event{Name: target, Op: fsnotify.Create}, true},
		{"removal of target", fsnotify.Event{Name: target, Op: fsnotify.Remove}, true},
		{"rename of target", fsnotify.Event{Name: target, Op: fsnotify.Rename}, true},
		{"chmod of target", fsnotify.Event{Name: target, Op: fsnotify.Chmod}, false},
		{"write to another file", fsnotify.Event{Name: filepath.Join(dir, "other.yml"), Op: fsnotify.Write}, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := fw.isTargetEvent(tc.ev); got != tc.want {
				t.Errorf("isTargetEvent(%v) = %t, want %t", tc.ev, got, tc.want)
			}
		})
	}
}
