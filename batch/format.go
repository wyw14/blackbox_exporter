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

// Package batch implements bulk import/export of blackbox_exporter
// modules.
//
// Import jobs run asynchronously and report an explicit state for the
// whole batch (pending, running, cancelling, completed,
// completed_with_errors, cancelled, failed). Every module is validated
// independently so that a batch can partially succeed. Submissions may
// carry an idempotency key: retrying the very same request never applies
// the modules twice. When a state file is configured, job progress is
// journaled and an interrupted batch is resumed on the next start.
//
// Both the historical configuration format
//
//	modules:
//	  http_2xx:
//	    prober: http
//
// and the new, versioned envelope
//
//	version: 1
//	kind: modules
//	modules:
//	  http_2xx:
//	    prober: http
//
// are accepted on import. Exports use the historical format by default so
// older blackbox_exporter versions keep being able to read them.
package batch

import (
	"bytes"
	"encoding/json"
	"fmt"

	"go.yaml.in/yaml/v3"

	"github.com/prometheus/blackbox_exporter/config"
)

// Format selects the serialization used by a batch request.
type Format string

const (
	// FormatYAML is the default format and matches the blackbox_exporter
	// configuration file syntax.
	FormatYAML Format = "yaml"
	// FormatJSON accepts the same layout serialized as JSON.
	FormatJSON Format = "json"

	// FormatVersion is the current envelope version.
	FormatVersion = 1
	// KindModules is the only payload kind supported today.
	KindModules = "modules"
)

// Envelope is the versioned import/export container.
type Envelope struct {
	Version int                      `yaml:"version,omitempty" json:"version,omitempty"`
	Kind    string                   `yaml:"kind,omitempty" json:"kind,omitempty"`
	Modules map[string]config.Module `yaml:"modules" json:"modules"`
}

// RawModule is a single, not yet validated module of an import request.
type RawModule struct {
	// Name is the module name as it appears in the mapping.
	Name string
	// Raw is the YAML encoding of the module body.
	Raw []byte
}

// ParsedEnvelope is the result of decoding an import payload.
type ParsedEnvelope struct {
	// Version is 0 for the historical unversioned format.
	Version int
	// Modules holds the first definition of every module name, in the
	// order in which they appear in the request.
	Modules []RawModule
	// DuplicateNames lists names that appear more than once. The first
	// definition is kept; every additional occurrence is reported as a
	// failed item when the job runs.
	DuplicateNames []string
}

// ParseEnvelope decodes an import payload without validating the
// individual modules. Both the historical `modules:` mapping and the
// versioned envelope are accepted. Unknown top-level fields are rejected.
func ParseEnvelope(data []byte, format Format) (*ParsedEnvelope, error) {
	yamlData, err := normalizeToYAML(data, format)
	if err != nil {
		return nil, err
	}

	var doc yaml.Node
	decoder := yaml.NewDecoder(bytes.NewReader(yamlData))
	if err := decoder.Decode(&doc); err != nil {
		return nil, fmt.Errorf("error parsing batch payload: %w", err)
	}
	if doc.Kind == 0 {
		return nil, fmt.Errorf("empty batch payload")
	}
	if doc.Kind != yaml.DocumentNode || len(doc.Content) != 1 {
		return nil, fmt.Errorf("batch payload must be a YAML mapping")
	}
	body := doc.Content[0]
	if body.Kind != yaml.MappingNode {
		return nil, fmt.Errorf("batch payload must be a YAML mapping with a \"modules\" key")
	}

	parsed := &ParsedEnvelope{}
	var modulesNode *yaml.Node
	seenKeys := map[string]struct{}{}

	for i := 0; i+1 < len(body.Content); i += 2 {
		keyNode, valueNode := body.Content[i], body.Content[i+1]
		var key string
		if err := keyNode.Decode(&key); err != nil {
			return nil, fmt.Errorf("error decoding payload key: %w", err)
		}
		if _, dup := seenKeys[key]; dup {
			return nil, fmt.Errorf("top-level key %q is defined more than once", key)
		}
		seenKeys[key] = struct{}{}

		switch key {
		case "version":
			if err := valueNode.Decode(&parsed.Version); err != nil {
				return nil, fmt.Errorf("error decoding version: %w", err)
			}
			if parsed.Version != FormatVersion {
				return nil, fmt.Errorf("unsupported batch format version %d, supported version is %d", parsed.Version, FormatVersion)
			}
		case "kind":
			var kind string
			if err := valueNode.Decode(&kind); err != nil {
				return nil, fmt.Errorf("error decoding kind: %w", err)
			}
			if kind != KindModules {
				return nil, fmt.Errorf("unsupported batch payload kind %q, supported kind is %q", kind, KindModules)
			}
		case "modules":
			if valueNode.Kind != yaml.MappingNode {
				return nil, fmt.Errorf("\"modules\" must be a mapping of module names to module definitions")
			}
			modulesNode = valueNode
		default:
			return nil, fmt.Errorf("unknown field %q in batch payload", key)
		}
	}

	if modulesNode == nil {
		return nil, fmt.Errorf("batch payload must contain a \"modules\" key")
	}

	seenNames := map[string]struct{}{}
	for i := 0; i+1 < len(modulesNode.Content); i += 2 {
		nameNode, moduleNode := modulesNode.Content[i], modulesNode.Content[i+1]
		var name string
		if err := nameNode.Decode(&name); err != nil {
			return nil, fmt.Errorf("error decoding module name: %w", err)
		}
		if name == "" {
			return nil, fmt.Errorf("module name must not be empty")
		}
		raw, err := yaml.Marshal(moduleNode)
		if err != nil {
			return nil, fmt.Errorf("error re-encoding module %q: %w", name, err)
		}
		if _, ok := seenNames[name]; ok {
			parsed.DuplicateNames = append(parsed.DuplicateNames, name)
			continue
		}
		seenNames[name] = struct{}{}
		parsed.Modules = append(parsed.Modules, RawModule{Name: name, Raw: raw})
	}

	return parsed, nil
}

// ValidateModule decodes a single module body with strict field checking
// and runs the same validation the configuration loader uses.
func ValidateModule(raw []byte) (config.Module, error) {
	var module config.Module
	decoder := yaml.NewDecoder(bytes.NewReader(raw))
	decoder.KnownFields(true)
	if err := decoder.Decode(&module); err != nil {
		return config.Module{}, err
	}
	return module, nil
}

// MarshalExport serializes modules for export. When versioned is false
// (the default) the historical configuration layout is used, which older
// blackbox_exporter versions can read directly.
func MarshalExport(modules map[string]config.Module, versioned bool) ([]byte, error) {
	sanitized := make(map[string]config.Module, len(modules))
	for name, module := range modules {
		sanitized[name] = sanitizeModule(module)
	}
	if versioned {
		return yaml.Marshal(Envelope{
			Version: FormatVersion,
			Kind:    KindModules,
			Modules: sanitized,
		})
	}
	return yaml.Marshal(config.Config{Modules: sanitized})
}

// sanitizeModule clears probe sections that do not match the configured
// prober. Validation populates every section with its default values;
// emitting the unrelated sections (in particular a default "dns" section,
// whose validation requires query_name) would make the exported document
// fail to reload. Only the section the prober actually uses is relevant.
func sanitizeModule(module config.Module) config.Module {
	switch module.Prober {
	case "http":
		module.TCP = config.TCPProbe{}
		module.ICMP = config.ICMPProbe{}
		module.DNS = config.DNSProbe{}
		module.GRPC = config.GRPCProbe{}
		module.Unix = config.UnixProbe{}
		module.Websocket = config.WebsocketProbe{}
	case "tcp":
		module.HTTP = config.HTTPProbe{}
		module.ICMP = config.ICMPProbe{}
		module.DNS = config.DNSProbe{}
		module.GRPC = config.GRPCProbe{}
		module.Unix = config.UnixProbe{}
		module.Websocket = config.WebsocketProbe{}
	case "icmp":
		module.HTTP = config.HTTPProbe{}
		module.TCP = config.TCPProbe{}
		module.DNS = config.DNSProbe{}
		module.GRPC = config.GRPCProbe{}
		module.Unix = config.UnixProbe{}
		module.Websocket = config.WebsocketProbe{}
	case "dns":
		module.HTTP = config.HTTPProbe{}
		module.TCP = config.TCPProbe{}
		module.ICMP = config.ICMPProbe{}
		module.GRPC = config.GRPCProbe{}
		module.Unix = config.UnixProbe{}
		module.Websocket = config.WebsocketProbe{}
	case "grpc":
		module.HTTP = config.HTTPProbe{}
		module.TCP = config.TCPProbe{}
		module.ICMP = config.ICMPProbe{}
		module.DNS = config.DNSProbe{}
		module.Unix = config.UnixProbe{}
		module.Websocket = config.WebsocketProbe{}
	case "unix":
		module.HTTP = config.HTTPProbe{}
		module.TCP = config.TCPProbe{}
		module.ICMP = config.ICMPProbe{}
		module.DNS = config.DNSProbe{}
		module.GRPC = config.GRPCProbe{}
		module.Websocket = config.WebsocketProbe{}
	case "websocket":
		module.HTTP = config.HTTPProbe{}
		module.TCP = config.TCPProbe{}
		module.ICMP = config.ICMPProbe{}
		module.DNS = config.DNSProbe{}
		module.GRPC = config.GRPCProbe{}
		module.Unix = config.UnixProbe{}
	}
	return module
}

// normalizeToYAML converts JSON payloads to YAML so that validation and
// strict field checking go through a single code path.
func normalizeToYAML(data []byte, format Format) ([]byte, error) {
	switch format {
	case "", FormatYAML:
		return data, nil
	case FormatJSON:
		var decoded any
		if err := json.Unmarshal(data, &decoded); err != nil {
			return nil, fmt.Errorf("error parsing JSON batch payload: %w", err)
		}
		yamlData, err := yaml.Marshal(decoded)
		if err != nil {
			return nil, fmt.Errorf("error converting JSON payload to YAML: %w", err)
		}
		return yamlData, nil
	default:
		return nil, fmt.Errorf("unsupported batch format %q", format)
	}
}
