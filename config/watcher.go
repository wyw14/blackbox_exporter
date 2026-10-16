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
	"path/filepath"
	"time"

	"github.com/fsnotify/fsnotify"
)

const (
	// watchDebounceInterval coalesces the bursts of file system events
	// emitted while the configuration is saved. Saving via truncate and
	// rewrite, or via a temporary file followed by a rename, typically
	// produces several events in quick succession.
	watchDebounceInterval = 100 * time.Millisecond
)

// WatchConfig starts a background goroutine that reloads sc whenever
// confFile changes.
//
// The configuration file and its containing directory are watched with
// fsnotify, so both in-place rewrites and atomic saves (write-to-temp +
// rename) are detected on every supported platform. File system events
// are debounced. A reload triggered by the watcher goes through
// ReloadConfig, therefore a configuration that fails to parse leaves the
// previously active configuration untouched.
//
// A periodic poll at pollInterval complements the watcher for filesystems
// that do not deliver change notifications reliably. Set pollInterval to
// zero to disable polling. Watching stops when ctx is canceled.
//
// WatchConfig returns after the watcher has been started; it only returns
// an error when neither file watching nor polling could be set up.
func (sc *SafeConfig) WatchConfig(ctx context.Context, confFile string, pollInterval time.Duration, logger *slog.Logger) error {
	if logger == nil {
		logger = slog.Default()
	}

	target, err := filepath.Abs(confFile)
	if err != nil {
		return err
	}
	fw := &fileWatcher{
		sc:           sc,
		target:       filepath.Clean(target),
		pollInterval: pollInterval,
		logger:       logger,
	}

	watcher, watchErr := fsnotify.NewWatcher()
	if watchErr == nil {
		// Watching the directory is required to follow atomic saves:
		// after a rename-over the old watch may point at a stale inode,
		// but the directory still reports the Create event.
		if err := watcher.Add(filepath.Dir(fw.target)); err != nil {
			watchErr = err
		} else if err := watcher.Add(fw.target); err != nil {
			// The directory watch alone is sufficient, a missing file
			// watch is not fatal.
			logger.Debug("Could not watch configuration file directly, watching its directory instead.", "file", fw.target, "err", err)
		}
	}
	if watchErr != nil {
		if pollInterval <= 0 {
			return watchErr
		}
		logger.Warn("Could not start configuration file watcher, falling back to periodic polling.", "err", watchErr)
	} else {
		fw.watcher = watcher
	}

	go fw.run(ctx)
	return nil
}

type fileWatcher struct {
	sc           *SafeConfig
	watcher      *fsnotify.Watcher
	target       string
	pollInterval time.Duration
	logger       *slog.Logger
}

func (fw *fileWatcher) run(ctx context.Context) {
	if fw.watcher != nil {
		defer fw.watcher.Close()
	}

	debounce := time.NewTimer(watchDebounceInterval)
	if !debounce.Stop() {
		<-debounce.C
	}
	armed := false
	scheduleReload := func() {
		if !armed {
			debounce.Reset(watchDebounceInterval)
			armed = true
		}
	}

	var poll <-chan time.Time
	if fw.pollInterval > 0 {
		ticker := time.NewTicker(fw.pollInterval)
		defer ticker.Stop()
		poll = ticker.C
	}

	events := fw.events()
	watcherErrors := fw.errors()

	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-events:
			if !ok {
				return
			}
			if !fw.isTargetEvent(ev) {
				continue
			}
			if ev.Op&fsnotify.Create != 0 {
				// The file may have been recreated after an atomic
				// rename; reattach the direct watch. Ignore errors: the
				// directory watch still covers this case.
				if err := fw.watcher.Add(fw.target); err != nil {
					fw.logger.Debug("Could not re-add watch for configuration file.", "file", fw.target, "err", err)
				}
			}
			scheduleReload()
		case err, ok := <-watcherErrors:
			if !ok {
				return
			}
			fw.logger.Error("Error watching configuration file.", "err", err)
		case <-poll:
			// Fallback for filesystems without reliable notifications.
			// ReloadConfig is a no-op when the checksum is unchanged.
			fw.reload()
		case <-debounce.C:
			armed = false
			fw.reload()
		}
	}
}

func (fw *fileWatcher) events() <-chan fsnotify.Event {
	if fw.watcher == nil {
		return nil
	}
	return fw.watcher.Events
}

func (fw *fileWatcher) errors() <-chan error {
	if fw.watcher == nil {
		return nil
	}
	return fw.watcher.Errors
}

// isTargetEvent reports whether ev concerns the watched configuration file
// and represents a change that may require a reload.
func (fw *fileWatcher) isTargetEvent(ev fsnotify.Event) bool {
	if filepath.Clean(ev.Name) != fw.target {
		return false
	}
	return ev.Op&(fsnotify.Write|fsnotify.Create|fsnotify.Remove|fsnotify.Rename) != 0
}

func (fw *fileWatcher) reload() {
	if err := fw.sc.ReloadConfig(fw.target, fw.logger); err != nil {
		fw.logger.Error("Error reloading config", "err", err)
		return
	}
	fw.logger.Info("Reloaded config file")
}
