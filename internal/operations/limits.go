package operations

import (
	"fmt"
	"math"
	"time"

	"tick-data-platform/internal/protocol"
)

const (
	ResourceLimitsVersion = "m4-operational-limits-v1"

	maxPruneCandidates    = uint64(1 << 20)
	maxProofObjects       = uint64(1 << 20)
	maxProofBytes         = uint64(1 << 30)
	maxManifestNodes      = uint64(1 << 20)
	maxAPIRequestBytes    = uint64(64 << 20)
	maxAPIResponseItems   = uint64(1 << 20)
	maxConcurrentRequests = uint64(4096)
	maxRequestTimeoutMS   = uint64(math.MaxInt64) / uint64(time.Millisecond)
)

// ResourceLimits is the bounded resource policy shared by M4 operation
// adapters. RequestTimeoutMS is deliberately an integer millisecond value in
// the contract so Go, Python, and configuration files cannot disagree about
// duration encoding.
type ResourceLimits struct {
	MaxPruneCandidates    uint64 `json:"max_prune_candidates" toml:"max_prune_candidates"`
	MaxProofObjects       uint64 `json:"max_proof_objects" toml:"max_proof_objects"`
	MaxProofBytes         uint64 `json:"max_proof_bytes" toml:"max_proof_bytes"`
	MaxManifestNodes      uint64 `json:"max_manifest_nodes" toml:"max_manifest_nodes"`
	MaxAPIRequestBytes    uint64 `json:"max_api_request_bytes" toml:"max_api_request_bytes"`
	MaxAPIResponseItems   uint64 `json:"max_api_response_items" toml:"max_api_response_items"`
	MaxConcurrentRequests uint64 `json:"max_concurrent_requests" toml:"max_concurrent_requests"`
	RequestTimeoutMS      uint64 `json:"request_timeout_ms" toml:"request_timeout_ms"`
}

// DefaultResourceLimits is the conservative local-operation policy. A caller
// may lower values, but Validate never permits zero or a value above the
// implementation bound.
var DefaultResourceLimits = ResourceLimits{
	MaxPruneCandidates:    1_000,
	MaxProofObjects:       10_000,
	MaxProofBytes:         64 << 20,
	MaxManifestNodes:      10_000,
	MaxAPIRequestBytes:    1 << 20,
	MaxAPIResponseItems:   1_000,
	MaxConcurrentRequests: 32,
	RequestTimeoutMS:      30_000,
}

func (l ResourceLimits) values() []struct {
	name  string
	value uint64
	bound uint64
} {
	return []struct {
		name  string
		value uint64
		bound uint64
	}{
		{"max_prune_candidates", l.MaxPruneCandidates, maxPruneCandidates},
		{"max_proof_objects", l.MaxProofObjects, maxProofObjects},
		{"max_proof_bytes", l.MaxProofBytes, maxProofBytes},
		{"max_manifest_nodes", l.MaxManifestNodes, maxManifestNodes},
		{"max_api_request_bytes", l.MaxAPIRequestBytes, maxAPIRequestBytes},
		{"max_api_response_items", l.MaxAPIResponseItems, maxAPIResponseItems},
		{"max_concurrent_requests", l.MaxConcurrentRequests, maxConcurrentRequests},
		{"request_timeout_ms", l.RequestTimeoutMS, maxRequestTimeoutMS},
	}
}

// ValidateProofLimits validates the subset embedded in a retention proof.
func ValidateProofLimits(maxObjects, maxBytes, maxManifestNodes uint64) error {
	return ResourceLimits{
		MaxProofObjects:  maxObjects,
		MaxProofBytes:    maxBytes,
		MaxManifestNodes: maxManifestNodes,
	}.validateFields(map[string]bool{
		"max_proof_objects":  true,
		"max_proof_bytes":    true,
		"max_manifest_nodes": true,
	})
}

func (l ResourceLimits) validateFields(selected map[string]bool) error {
	for _, item := range l.values() {
		if !selected[item.name] {
			continue
		}
		if item.value == 0 {
			return fmt.Errorf("%s must be nonzero", item.name)
		}
		if item.value > item.bound {
			return fmt.Errorf("%s exceeds implementation bound", item.name)
		}
	}
	return nil
}

// Validate rejects zero, overflow-prone, or implementation-unbounded limits.
func (l ResourceLimits) Validate() error {
	if err := l.validateFields(map[string]bool{
		"max_prune_candidates":    true,
		"max_proof_objects":       true,
		"max_proof_bytes":         true,
		"max_manifest_nodes":      true,
		"max_api_request_bytes":   true,
		"max_api_response_items":  true,
		"max_concurrent_requests": true,
		"request_timeout_ms":      true,
	}); err != nil {
		return err
	}
	if l.MaxAPIRequestBytes == 0 || l.MaxAPIResponseItems == 0 {
		return fmt.Errorf("API limits must be nonzero")
	}
	return nil
}

func (l ResourceLimits) RequestTimeout() (time.Duration, error) {
	if err := l.Validate(); err != nil {
		return 0, err
	}
	if l.RequestTimeoutMS > uint64(math.MaxInt64)/uint64(time.Millisecond) {
		return 0, fmt.Errorf("request timeout overflows time.Duration")
	}
	return time.Duration(l.RequestTimeoutMS) * time.Millisecond, nil
}

func (l ResourceLimits) Value() map[string]any {
	return map[string]any{
		"limits_version":          ResourceLimitsVersion,
		"max_api_request_bytes":   l.MaxAPIRequestBytes,
		"max_api_response_items":  l.MaxAPIResponseItems,
		"max_concurrent_requests": l.MaxConcurrentRequests,
		"max_manifest_nodes":      l.MaxManifestNodes,
		"max_proof_bytes":         l.MaxProofBytes,
		"max_proof_objects":       l.MaxProofObjects,
		"max_prune_candidates":    l.MaxPruneCandidates,
		"request_timeout_ms":      l.RequestTimeoutMS,
	}
}

func (l ResourceLimits) CanonicalJSON() ([]byte, error) {
	if err := l.Validate(); err != nil {
		return nil, err
	}
	return protocol.CanonicalJSON(l.Value())
}

// DecodeResourceLimits strictly verifies one versioned canonical limits
// object. It is intentionally independent from TOML decoding.
func DecodeResourceLimits(data []byte) (ResourceLimits, error) {
	value, err := protocol.DecodeCanonicalJSON(data)
	if err != nil {
		return ResourceLimits{}, fmt.Errorf("decode resource limits: %w", err)
	}
	object, ok := value.(map[string]any)
	if !ok {
		return ResourceLimits{}, fmt.Errorf("resource limits must be an object")
	}
	want := map[string]bool{
		"limits_version": true, "max_api_request_bytes": true, "max_api_response_items": true,
		"max_concurrent_requests": true, "max_manifest_nodes": true,
		"max_proof_bytes": true, "max_proof_objects": true, "max_prune_candidates": true,
		"request_timeout_ms": true,
	}
	if len(object) != len(want) {
		return ResourceLimits{}, fmt.Errorf("resource limits have an incomplete field set")
	}
	for key := range object {
		if !want[key] {
			return ResourceLimits{}, fmt.Errorf("resource limits contain unknown field %q", key)
		}
	}
	version, ok := object["limits_version"].(string)
	if !ok || version != ResourceLimitsVersion {
		return ResourceLimits{}, fmt.Errorf("unsupported resource limits version")
	}
	read := func(name string) (uint64, error) {
		value, ok := object[name].(uint64)
		if !ok {
			return 0, fmt.Errorf("%s must be a non-negative integer", name)
		}
		return value, nil
	}
	var result ResourceLimits
	fields := []struct {
		name string
		out  *uint64
	}{
		{"max_api_request_bytes", &result.MaxAPIRequestBytes},
		{"max_api_response_items", &result.MaxAPIResponseItems},
		{"max_concurrent_requests", &result.MaxConcurrentRequests},
		{"max_manifest_nodes", &result.MaxManifestNodes},
		{"max_proof_bytes", &result.MaxProofBytes},
		{"max_proof_objects", &result.MaxProofObjects},
		{"max_prune_candidates", &result.MaxPruneCandidates},
		{"request_timeout_ms", &result.RequestTimeoutMS},
	}
	for _, field := range fields {
		item, err := read(field.name)
		if err != nil {
			return ResourceLimits{}, err
		}
		*field.out = item
	}
	canonical, err := result.CanonicalJSON()
	if err != nil {
		return ResourceLimits{}, err
	}
	if string(canonical) != string(data) {
		return ResourceLimits{}, fmt.Errorf("resource limits bytes are not canonical")
	}
	return result, nil
}
