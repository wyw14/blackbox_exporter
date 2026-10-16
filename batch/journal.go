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
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"sync"
)

// Journal event kinds.
const (
	evtSubmit = "submit"
	evtState  = "state"
	evtItem   = "item"
	evtIdem   = "idempotency"
)

// journalEvent is a single append-only record in the state file.
type journalEvent struct {
	Kind    string     `json:"kind"`
	JobID   string     `json:"job_id,omitempty"`
	State   State      `json:"state,omitempty"`
	Op      string     `json:"op,omitempty"`
	Mode    string     `json:"mode,omitempty"`
	Format  Format     `json:"format,omitempty"`
	Key     string     `json:"key,omitempty"`
	Payload []byte     `json:"payload,omitempty"`
	Index   int        `json:"index,omitempty"`
	Item    ItemStatus `json:"item,omitzero"`
}

// journal is an append-only JSON lines file recording batch job progress.
// Replaying it reconstructs the idempotency table and the state of every
// job, which allows a batch interrupted by a crash to be resumed.
type journal struct {
	mu   sync.Mutex
	f    *os.File
	path string
}

// openJournal opens (creating if needed) the state file at path and keeps
// it open for appending.
func openJournal(path string) (*journal, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("error opening batch state file %q: %w", path, err)
	}
	return &journal{f: f, path: path}, nil
}

// append writes one event as a single JSON line.
func (j *journal) append(evt journalEvent) error {
	if j == nil {
		return nil
	}
	line, err := json.Marshal(evt)
	if err != nil {
		return fmt.Errorf("error encoding batch event: %w", err)
	}
	line = append(line, '\n')

	j.mu.Lock()
	defer j.mu.Unlock()
	if _, err := j.f.Write(line); err != nil {
		return fmt.Errorf("error writing batch state file: %w", err)
	}
	return nil
}

// Close releases the underlying file.
func (j *journal) Close() error {
	if j == nil {
		return nil
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.f.Close()
}

// replay reads all events from the state file in order. A torn trailing
// line caused by a crash while writing is ignored.
func (j *journal) replay(handle func(journalEvent) error) error {
	content, err := os.ReadFile(j.path)
	if err != nil {
		return fmt.Errorf("error reading batch state file: %w", err)
	}

	scanner := bufio.NewScanner(bytes.NewReader(content))
	// A batch payload can be large; give the scanner enough headroom.
	scanner.Buffer(make([]byte, 0, 64*1024), 64*1024*1024)
	var lines [][]byte
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		lines = append(lines, bytes.Clone(line))
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error scanning batch state file: %w", err)
	}

	for i, line := range lines {
		var evt journalEvent
		if err := json.Unmarshal(line, &evt); err != nil {
			// The final record may be partially written after a crash.
			if i == len(lines)-1 {
				break
			}
			return fmt.Errorf("corrupt batch state file at line %d: %w", i+1, err)
		}
		if err := handle(evt); err != nil {
			return err
		}
	}
	return nil
}
