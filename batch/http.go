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
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
)

// maxBatchBodyBytes caps the size of an import payload accepted over HTTP.
const maxBatchBodyBytes = 64 * 1024 * 1024

// Header names controlling batch import requests.
const (
	// HeaderIdempotencyKey makes retried submissions idempotent.
	HeaderIdempotencyKey = "Idempotency-Key"
	// HeaderImportMode selects upsert (default), create or update.
	HeaderImportMode = "X-Batch-Mode"
)

// Handler returns the HTTP handler exposing the batch API. The returned
// handler serves the rooted paths /import, /export and /jobs and is meant
// to be mounted under a versioned prefix such as /api/v1/batch.
func (m *Manager) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /import", m.handleImport)
	mux.HandleFunc("GET /export", m.handleExport)
	mux.HandleFunc("GET /jobs", m.handleListJobs)
	mux.HandleFunc("GET /jobs/{id}", m.handleGetJob)
	mux.HandleFunc("POST /jobs/{id}/cancel", m.handleCancel)
	return mux
}

func (m *Manager) handleImport(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxBatchBodyBytes)
	payload, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	status, duplicated, err := m.SubmitImport(ImportRequest{
		Payload:        payload,
		Format:         formatFromContentType(r.Header.Get("Content-Type")),
		Mode:           r.Header.Get(HeaderImportMode),
		IdempotencyKey: r.Header.Get(HeaderIdempotencyKey),
	})
	if err != nil {
		code := http.StatusBadRequest
		if errors.Is(err, ErrIdempotencyConflict) {
			code = http.StatusConflict
		}
		writeError(w, code, err.Error())
		return
	}

	// 200 for a replayed idempotent request, 202 for a newly accepted job.
	code := http.StatusAccepted
	if duplicated {
		code = http.StatusOK
	}
	writeJSON(w, code, status)
}

func (m *Manager) handleExport(w http.ResponseWriter, r *http.Request) {
	var names []string
	if raw := strings.TrimSpace(r.URL.Query().Get("names")); raw != "" {
		for name := range strings.SplitSeq(raw, ",") {
			if name = strings.TrimSpace(name); name != "" {
				names = append(names, name)
			}
		}
	}
	versioned := r.URL.Query().Get("versioned") == "true"

	data, err := m.Export(names, versioned)
	if err != nil {
		// Missing modules surface as 404, every other failure as 400.
		code := http.StatusBadRequest
		if strings.Contains(err.Error(), "not found") {
			code = http.StatusNotFound
		}
		writeError(w, code, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/yaml; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

func (m *Manager) handleListJobs(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, m.List())
}

func (m *Manager) handleGetJob(w http.ResponseWriter, r *http.Request) {
	status := m.Get(r.PathValue("id"))
	if status == nil {
		writeError(w, http.StatusNotFound, "batch job not found")
		return
	}
	writeJSON(w, http.StatusOK, status)
}

func (m *Manager) handleCancel(w http.ResponseWriter, r *http.Request) {
	status, err := m.Cancel(r.PathValue("id"))
	switch {
	case errors.Is(err, ErrJobNotFound):
		writeError(w, http.StatusNotFound, err.Error())
	case errors.Is(err, ErrJobFinished):
		writeJSON(w, http.StatusConflict, status)
	case err != nil:
		writeError(w, http.StatusInternalServerError, err.Error())
	default:
		writeJSON(w, http.StatusOK, status)
	}
}

func formatFromContentType(contentType string) Format {
	if strings.Contains(strings.ToLower(contentType), "json") {
		return FormatJSON
	}
	return FormatYAML
}

type errorResponse struct {
	Error string `json:"error"`
}

func writeError(w http.ResponseWriter, code int, message string) {
	writeJSON(w, code, errorResponse{Error: message})
}

func writeJSON(w http.ResponseWriter, code int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(payload)
}
