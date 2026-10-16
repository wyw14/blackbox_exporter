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
	"reflect"
)

// Priorities of the built-in rules. They are exported so that externally
// registered rules can reliably run before, between or after the built-ins.
const (
	PriorityModuleName           = 100
	PriorityModuleTimeout        = 110
	PriorityHTTPSSLConflict      = 120
	PriorityProbeSectionMismatch = 200
)

// Names of the built-in rules.
const (
	RuleModuleName           = "module-name"
	RuleModuleTimeout        = "module-timeout"
	RuleHTTPSSLConflict      = "http-ssl-conflict"
	RuleProbeSectionMismatch = "probe-section-mismatch"
)

// BuiltInRules returns the validation rules enabled by default. The returned
// slice is a fresh copy and may be amended by callers.
func BuiltInRules() []Rule {
	return []Rule{
		moduleNameRule(),
		moduleTimeoutRule(),
		httpSSLConflictRule(),
		probeSectionMismatchRule(),
	}
}

// moduleNameRule rejects modules with an empty name, which cannot be selected
// by a scrape request.
func moduleNameRule() Rule {
	return NewRuleFunc(RuleModuleName, PriorityModuleName, func(ctx *ValidationContext) (Diagnostics, error) {
		var diags Diagnostics
		for name := range ctx.Config.Modules {
			if name == "" {
				diags = append(diags, Errorf(RuleModuleName, "", "modules", "module name must not be empty"))
			}
		}
		return diags, nil
	})
}

// moduleTimeoutRule rejects negative module timeouts (a zero timeout means
// "use the default" and stays valid).
func moduleTimeoutRule() Rule {
	return NewRuleFunc(RuleModuleTimeout, PriorityModuleTimeout, func(ctx *ValidationContext) (Diagnostics, error) {
		var diags Diagnostics
		for name, module := range ctx.Config.Modules {
			if module.Timeout < 0 {
				diags = append(diags, Errorf(RuleModuleTimeout, name, "timeout",
					"module timeout must not be negative, got %s", module.Timeout))
			}
		}
		return diags, nil
	})
}

// httpSSLConflictRule rejects HTTP modules that request failure on both SSL
// and plaintext, which can never succeed.
func httpSSLConflictRule() Rule {
	return NewRuleFunc(RuleHTTPSSLConflict, PriorityHTTPSSLConflict, func(ctx *ValidationContext) (Diagnostics, error) {
		var diags Diagnostics
		for name, module := range ctx.Config.Modules {
			http := module.HTTP
			if http.FailIfSSL && http.FailIfNotSSL {
				diags = append(diags, Errorf(RuleHTTPSSLConflict, name, "http.fail_if_ssl",
					"fail_if_ssl and fail_if_not_ssl are mutually exclusive and cannot both be enabled"))
			}
		}
		return diags, nil
	})
}

// probeSectionMismatchRule warns when a module carries configuration for a
// prober section that does not match its prober; such a section is silently
// ignored.
func probeSectionMismatchRule() Rule {
	return NewRuleFunc(RuleProbeSectionMismatch, PriorityProbeSectionMismatch, func(ctx *ValidationContext) (Diagnostics, error) {
		var diags Diagnostics
		for name, module := range ctx.Config.Modules {
			for section := range configuredProbeSections(module) {
				if section == module.Prober {
					continue
				}
				diags = append(diags, Warningf(RuleProbeSectionMismatch, name, section,
					"prober is %q but the %q section is configured and will be ignored", module.Prober, section))
			}
		}
		return diags, nil
	})
}

// configuredProbeSections reports the probe sections whose values differ from
// the value an absent section would have after decoding, i.e. the sections
// that the operator actually configured.
func configuredProbeSections(m Module) map[string]bool {
	sections := map[string]bool{
		"http": !reflect.DeepEqual(m.HTTP, DefaultHTTPProbe),
		"tcp":  !reflect.DeepEqual(m.TCP, DefaultTCPProbe),
		"icmp": !reflect.DeepEqual(m.ICMP, DefaultICMPProbe),
		"dns":  !reflect.DeepEqual(m.DNS, DefaultDNSProbe),
		// GRPCProbe is the only probe section without its own UnmarshalYAML,
		// so an absent or empty section decodes to the zero value rather than
		// DefaultGRPCProbe.
		"grpc":      !reflect.ValueOf(m.GRPC).IsZero(),
		"unix":      !reflect.DeepEqual(m.Unix, DefaultUnixProbe),
		"websocket": !reflect.DeepEqual(m.Websocket, DefaultWebsocketProbe),
	}
	for section, configured := range sections {
		if !configured {
			delete(sections, section)
		}
	}
	return sections
}
