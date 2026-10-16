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
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/prometheus/blackbox_exporter/config"
)

// State is the explicit lifecycle state of a batch job.
type State string

const (
	// StatePending means the job was accepted but processing has not started.
	StatePending State = "pending"
	// StateRunning means the job is currently processing modules.
	StateRunning State = "running"
	// StateCancelling means cancellation was requested and the worker stops
	// before processing further modules.
	StateCancelling State = "cancelling"
	// StateCompleted means every module in the batch succeeded.
	StateCompleted State = "completed"
	// StateCompletedWithErrors means the job finished but at least one
	// module failed (partial success).
	StateCompletedWithErrors State = "completed_with_errors"
	// StateCancelled means the job was stopped before all modules were
	// processed.
	StateCancelled State = "cancelled"
	// StateFailed means the job as a whole could not run (for example its
	// payload could not be read after a restart).
	StateFailed State = "failed"
)

// IsTerminal reports whether the state will not change anymore.
func (s State) IsTerminal() bool {
	switch s {
	case StateCompleted, StateCompletedWithErrors, StateCancelled, StateFailed:
		return true
	default:
		return false
	}
}

// Per-item processing outcomes.
const (
	// ItemApplied means the module was validated and stored.
	ItemApplied = "applied"
	// ItemFailed means the module was rejected and not stored.
	ItemFailed = "failed"
)

// Import modes control what happens when a module already exists.
const (
	// ModeUpsert creates or replaces the module (default).
	ModeUpsert = "upsert"
	// ModeCreate fails when the module already exists.
	ModeCreate = "create"
	// ModeUpdate fails when the module does not exist yet.
	ModeUpdate = "update"
)

// Sentinel errors returned by the Manager.
var (
	// ErrJobNotFound is returned for unknown job IDs.
	ErrJobNotFound = errors.New("batch job not found")
	// ErrJobFinished is returned when cancelling a job that already reached
	// a terminal state.
	ErrJobFinished = errors.New("batch job is already finished")
	// ErrIdempotencyConflict is returned when an idempotency key is reused
	// with a different request.
	ErrIdempotencyConflict = errors.New("idempotency key was already used with a different request")
)

// ItemStatus is the per-module outcome of a batch job.
type ItemStatus struct {
	// Name is the module name.
	Name string `json:"name"`
	// Status is either ItemApplied or ItemFailed.
	Status string `json:"status"`
	// Error holds the validation or conflict message for failed items.
	Error string `json:"error,omitempty"`
}

// JobStatus is a snapshot of a batch job, safe to serialize and return to
// clients.
type JobStatus struct {
	ID             string       `json:"id"`
	Op             string       `json:"op"`
	State          State        `json:"state"`
	Mode           string       `json:"mode"`
	IdempotencyKey string       `json:"idempotency_key,omitempty"`
	Total          int          `json:"total"`
	Processed      int          `json:"processed"`
	Succeeded      int          `json:"succeeded"`
	Failed         int          `json:"failed"`
	Results        []ItemStatus `json:"results"`
	CreatedAt      time.Time    `json:"created_at"`
	UpdatedAt      time.Time    `json:"updated_at"`
	FinishedAt     *time.Time   `json:"finished_at,omitempty"`
	Error          string       `json:"error,omitempty"`
}

// ImportRequest is a batch import submission.
type ImportRequest struct {
	// Payload is the YAML or JSON encoded envelope.
	Payload []byte
	// Format is FormatYAML (default) or FormatJSON.
	Format Format
	// Mode is one of ModeUpsert, ModeCreate, ModeUpdate.
	Mode string
	// IdempotencyKey optionally makes the submission idempotent.
	IdempotencyKey string
}

// ItemHookAction tells the worker how to proceed after an item hook call.
type ItemHookAction int

const (
	// HookContinue processes the item normally.
	HookContinue ItemHookAction = iota
	// HookAbruptStop stops the worker without recording a terminal state.
	// It simulates an abrupt process termination and is mainly useful in
	// recovery tests.
	HookAbruptStop
)

// ItemHook is invoked before every item of a job. The index is the
// zero-based position within the batch.
type ItemHook func(jobID string, index, total int, name string) ItemHookAction

// Option customizes a Manager.
type Option func(*Manager)

// WithStateFile enables durable journaling at the given path so jobs can
// be resumed after a restart.
func WithStateFile(path string) Option {
	return func(m *Manager) { m.statePath = path }
}

// WithLogger sets the logger used by the manager.
func WithLogger(logger *slog.Logger) Option {
	return func(m *Manager) {
		if logger != nil {
			m.logger = logger
		}
	}
}

// WithItemHook installs a hook invoked before each processed item.
func WithItemHook(hook ItemHook) Option {
	return func(m *Manager) { m.hook = hook }
}

// preparedItem is a validated (or known-invalid) module ready for the
// worker to apply.
type preparedItem struct {
	name   string
	module config.Module
	ok     bool
	err    string
}

// job is the internal representation of a batch operation.
type job struct {
	mu          sync.Mutex
	id          string
	op          string
	mode        string
	key         string
	format      Format
	payload     []byte
	state       State
	items       []preparedItem
	results     map[int]ItemStatus
	errMsg      string
	cancelReq   bool
	createdAt   time.Time
	updatedAt   time.Time
	finishedAt  time.Time
	done        chan struct{}
}

// Manager runs batch import/export jobs against a ModuleStore.
type Manager struct {
	store     ModuleStore
	statePath string
	logger    *slog.Logger
	hook      ItemHook

	journal *journal

	mu   sync.Mutex
	jobs map[string]*job
	// idem maps idempotency keys to job IDs.
	idem map[string]string

	wg sync.WaitGroup
}

// NewManager creates a Manager and, when WithStateFile was provided,
// replays the journal and resumes any interrupted job.
func NewManager(store ModuleStore, opts ...Option) (*Manager, error) {
	if store == nil {
		store = NewMemoryStore()
	}
	m := &Manager{
		store:  store,
		logger: slog.Default(),
		jobs:   map[string]*job{},
		idem:   map[string]string{},
	}
	for _, opt := range opts {
		opt(m)
	}

	if m.statePath != "" {
		j, err := openJournal(m.statePath)
		if err != nil {
			return nil, err
		}
		m.journal = j
		if err := m.recover(); err != nil {
			return nil, err
		}
	}

	return m, nil
}

// SubmitImport validates and enqueues a batch import job. When the
// request carries an idempotency key that was used before, the original
// job is returned without applying anything again; the boolean result is
// true in that case.
func (m *Manager) SubmitImport(req ImportRequest) (*JobStatus, bool, error) {
	mode := req.Mode
	if mode == "" {
		mode = ModeUpsert
	}
	if mode != ModeUpsert && mode != ModeCreate && mode != ModeUpdate {
		return nil, false, fmt.Errorf("invalid import mode %q", req.Mode)
	}

	parsed, err := ParseEnvelope(req.Payload, req.Format)
	if err != nil {
		return nil, false, err
	}
	if len(parsed.Modules) == 0 {
		return nil, false, fmt.Errorf("batch payload does not contain any module")
	}

	items := make([]preparedItem, 0, len(parsed.Modules)+len(parsed.DuplicateNames))
	for _, raw := range parsed.Modules {
		module, verr := ValidateModule(raw.Raw)
		if verr != nil {
			items = append(items, preparedItem{name: raw.Name, ok: false, err: verr.Error()})
			continue
		}
		items = append(items, preparedItem{name: raw.Name, module: module, ok: true})
	}
	for _, name := range parsed.DuplicateNames {
		items = append(items, preparedItem{
			name: name,
			ok:   false,
			err:  fmt.Sprintf("module %q is defined more than once in the request", name),
		})
	}

	now := time.Now()
	j := &job{
		id:        newJobID(),
		op:        "import",
		mode:      mode,
		key:       req.IdempotencyKey,
		format:    req.Format,
		payload:   bytes.Clone(req.Payload),
		state:     StatePending,
		items:     items,
		results:   map[int]ItemStatus{},
		createdAt: now,
		updatedAt: now,
		done:      make(chan struct{}),
	}

	m.mu.Lock()
	if req.IdempotencyKey != "" {
		if existingID, ok := m.idem[req.IdempotencyKey]; ok {
			existing := m.jobs[existingID]
			m.mu.Unlock()
			if existing != nil && !existing.sameRequest(j) {
				return nil, false, ErrIdempotencyConflict
			}
			return m.Get(existingID), true, nil
		}
	}
	m.jobs[j.id] = j
	if req.IdempotencyKey != "" {
		m.idem[req.IdempotencyKey] = j.id
	}

	if m.journal != nil {
		if err := m.journal.append(journalEvent{
			Kind:    evtIdem,
			Key:     req.IdempotencyKey,
			JobID:   j.id,
		}); err != nil {
			m.mu.Unlock()
			return nil, false, err
		}
		if err := m.journal.append(journalEvent{
			Kind:    evtSubmit,
			JobID:   j.id,
			Op:      j.op,
			Mode:    j.mode,
			Format:  j.format,
			Key:     j.key,
			Payload: j.payload,
		}); err != nil {
			m.mu.Unlock()
			return nil, false, err
		}
	}
	m.mu.Unlock()

	m.start(j)
	return m.Get(j.id), false, nil
}

// Get returns a snapshot of a job.
func (m *Manager) Get(id string) *JobStatus {
	m.mu.Lock()
	j := m.jobs[id]
	m.mu.Unlock()
	if j == nil {
		return nil
	}
	return j.status()
}

// List returns the status of all jobs, oldest first.
func (m *Manager) List() []JobStatus {
	m.mu.Lock()
	jobs := make([]*job, 0, len(m.jobs))
	for _, j := range m.jobs {
		jobs = append(jobs, j)
	}
	m.mu.Unlock()
	sort.Slice(jobs, func(i, j int) bool {
		return jobs[i].createdAt.Before(jobs[j].createdAt)
	})
	statuses := make([]JobStatus, 0, len(jobs))
	for _, j := range jobs {
		statuses = append(statuses, *j.status())
	}
	return statuses
}

// Cancel requests cancellation of a running or pending job. The job moves
// to StateCancelling and then StateCancelled once the worker observed the
// request. Already finished jobs return ErrJobFinished and unknown IDs
// return ErrJobNotFound.
func (m *Manager) Cancel(id string) (*JobStatus, error) {
	m.mu.Lock()
	j := m.jobs[id]
	m.mu.Unlock()
	if j == nil {
		return nil, ErrJobNotFound
	}

	j.mu.Lock()
	if j.state.IsTerminal() {
		j.mu.Unlock()
		return j.status(), ErrJobFinished
	}
	j.cancelReq = true
	if j.state == StatePending || j.state == StateRunning {
		j.state = StateCancelling
		j.updatedAt = time.Now()
	}
	j.mu.Unlock()

	if m.journal != nil {
		if err := m.journal.append(journalEvent{Kind: evtState, JobID: j.id, State: StateCancelling}); err != nil {
			return nil, err
		}
	}
	return j.status(), nil
}

// Wait blocks until the job reaches a terminal state, the context is
// cancelled, or the job is unknown.
func (m *Manager) Wait(ctx context.Context, id string) (*JobStatus, error) {
	m.mu.Lock()
	j := m.jobs[id]
	m.mu.Unlock()
	if j == nil {
		return nil, ErrJobNotFound
	}
	select {
	case <-j.done:
		return j.status(), nil
	case <-ctx.Done():
		return j.status(), ctx.Err()
	}
}

// Export returns a serialized snapshot of the named modules, or of all
// modules when names is empty. Unknown names are reported as an error.
func (m *Manager) Export(names []string, versioned bool) ([]byte, error) {
	modules := map[string]config.Module{}
	if len(names) == 0 {
		for _, name := range m.store.List() {
			module, ok := m.store.Get(name)
			if !ok {
				continue
			}
			modules[name] = module
		}
	} else {
		var missing []string
		for _, name := range names {
			module, ok := m.store.Get(name)
			if !ok {
				missing = append(missing, name)
				continue
			}
			modules[name] = module
		}
		if len(missing) > 0 {
			sort.Strings(missing)
			return nil, fmt.Errorf("module(s) not found: %v", missing)
		}
	}
	return MarshalExport(modules, versioned)
}

// Close waits for running workers to finish and releases resources.
func (m *Manager) Close() error {
	m.wg.Wait()
	if m.journal != nil {
		return m.journal.Close()
	}
	return nil
}

// start launches the worker goroutine for a job.
func (m *Manager) start(j *job) {
	m.wg.Add(1)
	go m.runJob(j)
}

// runJob is the per-job worker. It is idempotent: items already present
// in j.results (which happens after journal recovery) are skipped, and
// Put is an upsert, so resuming a job never duplicates modules.
func (m *Manager) runJob(j *job) {
	defer m.wg.Done()

	// A recovered job may already be cancelling; it must not regress to
	// "running".
	j.mu.Lock()
	if j.state == StatePending {
		j.state = StateRunning
		j.updatedAt = time.Now()
		j.mu.Unlock()
		if m.journal != nil {
			if err := m.journal.append(journalEvent{Kind: evtState, JobID: j.id, State: StateRunning}); err != nil {
				m.logger.Error("Error persisting batch job state", "job", j.id, "err", err)
			}
		}
	} else {
		j.mu.Unlock()
	}

	for idx, item := range j.items {
		if m.hook != nil {
			if m.hook(j.id, idx, len(j.items), item.name) == HookAbruptStop {
				// Simulate an abrupt process exit: no terminal state is
				// recorded; the journal contains the durable progress.
				return
			}
		}

		j.mu.Lock()
		cancel := j.cancelReq
		_, processed := j.results[idx]
		j.mu.Unlock()
		if cancel {
			m.finish(j, StateCancelled)
			return
		}
		if processed {
			continue
		}

		status := ItemStatus{Name: item.name}
		if !item.ok {
			status.Status = ItemFailed
			status.Error = item.err
		} else {
			switch {
			case j.mode == ModeCreate && m.store.Has(item.name):
				status.Status = ItemFailed
				status.Error = fmt.Sprintf("module %q already exists", item.name)
			case j.mode == ModeUpdate && !m.store.Has(item.name):
				status.Status = ItemFailed
				status.Error = fmt.Sprintf("module %q does not exist", item.name)
			default:
				m.store.Put(item.name, item.module)
				status.Status = ItemApplied
			}
		}

		j.mu.Lock()
		if _, exists := j.results[idx]; !exists {
			j.results[idx] = status
			j.updatedAt = time.Now()
		}
		j.mu.Unlock()

		if m.journal != nil {
			if err := m.journal.append(journalEvent{
				Kind:  evtItem,
				JobID: j.id,
				Index: idx,
				Item:  status,
			}); err != nil {
				m.logger.Error("Error persisting batch item", "job", j.id, "err", err)
			}
		}
	}

	j.mu.Lock()
	cancel := j.cancelReq
	failed := 0
	for _, result := range j.results {
		if result.Status != ItemApplied {
			failed++
		}
	}
	j.mu.Unlock()
	if cancel {
		// Cancellation arrived while the last items were processed.
		m.finish(j, StateCancelled)
		return
	}
	if failed > 0 {
		m.finish(j, StateCompletedWithErrors)
		return
	}
	m.finish(j, StateCompleted)
}

// finish records a terminal state and wakes up anyone waiting on the job.
func (m *Manager) finish(j *job, state State) {
	j.mu.Lock()
	if j.state.IsTerminal() {
		j.mu.Unlock()
		return
	}
	j.state = state
	now := time.Now()
	j.updatedAt = now
	j.finishedAt = now
	done := j.done
	j.mu.Unlock()

	if m.journal != nil {
		if err := m.journal.append(journalEvent{Kind: evtState, JobID: j.id, State: state}); err != nil {
			m.logger.Error("Error persisting batch job state", "job", j.id, "err", err)
		}
	}
	close(done)
}

// status returns a serializable snapshot of the job.
func (j *job) status() *JobStatus {
	j.mu.Lock()
	defer j.mu.Unlock()

	succeeded, failed, processed := 0, 0, 0
	results := make([]ItemStatus, 0, len(j.results))
	for idx := 0; idx < len(j.items); idx++ {
		result, ok := j.results[idx]
		if !ok {
			continue
		}
		results = append(results, result)
		if result.Status == ItemApplied {
			succeeded++
		} else {
			failed++
		}
	}
	processed = len(results)

	st := &JobStatus{
		ID:             j.id,
		Op:             j.op,
		State:          j.state,
		Mode:           j.mode,
		IdempotencyKey: j.key,
		Total:          len(j.items),
		Processed:      processed,
		Succeeded:      succeeded,
		Failed:         failed,
		Results:        results,
		CreatedAt:      j.createdAt,
		UpdatedAt:      j.updatedAt,
		Error:          j.errMsg,
	}
	if !j.finishedAt.IsZero() {
		finished := j.finishedAt
		st.FinishedAt = &finished
	}
	return st
}

// sameRequest reports whether two submissions share the idempotent
// identity (same payload and mode).
func (j *job) sameRequest(other *job) bool {
	return j.mode == other.mode && bytes.Equal(j.payload, other.payload)
}

// recover replays the journal and resumes interrupted jobs.
func (m *Manager) recover() error {
	if err := m.journal.replay(func(evt journalEvent) error {
		switch evt.Kind {
		case evtIdem:
			if evt.Key != "" && evt.JobID != "" {
				m.idem[evt.Key] = evt.JobID
			}
		case evtSubmit:
			now := time.Now()
			j := &job{
				id:        evt.JobID,
				op:        evt.Op,
				mode:      evt.Mode,
				key:       evt.Key,
				format:    evt.Format,
				payload:   bytes.Clone(evt.Payload),
				state:     StatePending,
				results:   map[int]ItemStatus{},
				createdAt: now,
				updatedAt: now,
				done:      make(chan struct{}),
			}
			parsed, err := ParseEnvelope(evt.Payload, evt.Format)
			if err != nil {
				j.state = StateFailed
				j.errMsg = fmt.Sprintf("error recovering job: %s", err)
				close(j.done)
				m.jobs[j.id] = j
				return nil
			}
			for _, raw := range parsed.Modules {
				module, verr := ValidateModule(raw.Raw)
				if verr != nil {
					j.items = append(j.items, preparedItem{name: raw.Name, ok: false, err: verr.Error()})
					continue
				}
				j.items = append(j.items, preparedItem{name: raw.Name, module: module, ok: true})
			}
			for _, name := range parsed.DuplicateNames {
				j.items = append(j.items, preparedItem{
					name: name,
					ok:   false,
					err:  fmt.Sprintf("module %q is defined more than once in the request", name),
				})
			}
			m.jobs[j.id] = j
		case evtItem:
			if j, ok := m.jobs[evt.JobID]; ok {
				j.mu.Lock()
				j.results[evt.Index] = evt.Item
				j.mu.Unlock()
			}
		case evtState:
			if j, ok := m.jobs[evt.JobID]; ok {
				j.mu.Lock()
				j.state = evt.State
				if evt.State.IsTerminal() && j.finishedAt.IsZero() {
					j.finishedAt = time.Now()
					close(j.done)
				}
				if evt.State == StateCancelling {
					j.cancelReq = true
				}
				j.mu.Unlock()
			}
		}
		return nil
	}); err != nil {
		return err
	}
	// The replay is complete; restart jobs that never reached a terminal
	// state. Worker startup is deferred until replay finished so that
	// replayed results are visible before the first item runs.
	var toResume []*job
	m.mu.Lock()
	for _, j := range m.jobs {
		j.mu.Lock()
		terminal := j.state.IsTerminal()
		j.mu.Unlock()
		if !terminal {
			// A recovered job that was cancelling stays cancellable.
			toResume = append(toResume, j)
		}
	}
	m.mu.Unlock()
	for _, j := range toResume {
		m.logger.Info("Resuming interrupted batch job", "job", j.id)
		m.start(j)
	}
	return nil
}

// newJobID generates a random, URL-safe job identifier.
func newJobID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// rand.Read should never fail; fall back to a time-based id.
		return fmt.Sprintf("job-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}
