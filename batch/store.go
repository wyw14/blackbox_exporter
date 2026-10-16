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
	"sync"

	"github.com/prometheus/blackbox_exporter/config"
)

// ModuleStore is the storage backend for the modules a batch job imports
// or exports. Put is an idempotent upsert, which makes retries and
// interrupted-job recovery safe.
type ModuleStore interface {
	// Has reports whether a module with the given name exists.
	Has(name string) bool
	// Get returns a copy of a module and whether it exists.
	Get(name string) (config.Module, bool)
	// Put creates or replaces a module.
	Put(name string, module config.Module)
	// List returns the names of all stored modules.
	List() []string
}

// MemoryStore is a concurrency-safe in-memory ModuleStore. It is the
// default backend and is also useful in tests.
type MemoryStore struct {
	mu      sync.RWMutex
	modules map[string]config.Module
}

// NewMemoryStore returns an empty MemoryStore.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{modules: map[string]config.Module{}}
}

// Has implements ModuleStore.
func (s *MemoryStore) Has(name string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.modules[name]
	return ok
}

// Get implements ModuleStore.
func (s *MemoryStore) Get(name string) (config.Module, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	module, ok := s.modules[name]
	return module, ok
}

// Put implements ModuleStore.
func (s *MemoryStore) Put(name string, module config.Module) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.modules[name] = module
}

// List implements ModuleStore.
func (s *MemoryStore) List() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	names := make([]string, 0, len(s.modules))
	for name := range s.modules {
		names = append(names, name)
	}
	return names
}
