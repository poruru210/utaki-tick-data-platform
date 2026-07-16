package operations

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"tick-data-platform/internal/archive"
	"tick-data-platform/internal/protocol"
)

const ScopeOperationsVersion = "m4-scope-operations-v1"

const (
	ScopeInventoryVersion    = "m4-scope-inventory-v1"
	maxScopeInventoryEntries = 4096
)

// ScopeProcessConfig is the non-secret inventory entry for one independent
// MQL service and Gateway process. Paths are configuration identities, never
// credentials or caller authority for archive mutation.
type ScopeProcessConfig struct {
	Scope             archive.ScopeConfig
	GatewayInstanceID string
	ListenAddress     string
	GatewayConfigPath string
	MQLConfigPath     string
	WALRoot           string
	JournalPath       string
	OutboxRoot        string
	ReceiptRoot       string
	LockRoot          string
	CredentialPrefix  string
}

func (c ScopeProcessConfig) ScopeKey() (string, error) {
	return archive.ScopePathKey(c.Scope)
}

func (c ScopeProcessConfig) Validate() error {
	if c.GatewayInstanceID == "" || c.ListenAddress == "" || c.GatewayConfigPath == "" || c.MQLConfigPath == "" || c.WALRoot == "" || c.JournalPath == "" || c.OutboxRoot == "" || c.ReceiptRoot == "" || c.LockRoot == "" || c.CredentialPrefix == "" {
		return fmt.Errorf("scope process configuration is incomplete")
	}
	if c.Scope.PublisherEpoch == 0 {
		return fmt.Errorf("scope publisher epoch is zero")
	}
	if _, err := c.Scope.ConfigHash(); err != nil {
		return err
	}
	host, _, err := normalizeListenAddress(c.ListenAddress)
	if err != nil {
		return err
	}
	if host == "*" {
		return fmt.Errorf("scope listen address must be loopback")
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return fmt.Errorf("scope listen address must be loopback")
	}
	for name, value := range c.pathValues() {
		if err := validateScopePath(name, value); err != nil {
			return err
		}
	}
	if strings.ContainsAny(c.CredentialPrefix, "\x00\r\n") || strings.TrimSpace(c.CredentialPrefix) == "" {
		return fmt.Errorf("credential prefix is invalid")
	}
	return nil
}

func (c ScopeProcessConfig) pathValues() map[string]string {
	return map[string]string{
		"gateway_config_path": c.GatewayConfigPath,
		"journal_path":        c.JournalPath,
		"lock_root":           c.LockRoot,
		"mql_config_path":     c.MQLConfigPath,
		"outbox_root":         c.OutboxRoot,
		"receipt_root":        c.ReceiptRoot,
		"wal_root":            c.WALRoot,
	}
}

func validateScopePath(name, value string) error {
	if value == "" || strings.ContainsRune(value, '\x00') || strings.ContainsAny(value, "\r\n") {
		return fmt.Errorf("%s is invalid", name)
	}
	return nil
}

func normalizedPath(value string) (string, error) {
	abs, err := filepath.Abs(filepath.Clean(value))
	if err != nil {
		return "", err
	}
	if abs == "." || abs == string(filepath.Separator) {
		return abs, nil
	}
	return filepath.Clean(abs), nil
}

func normalizedListenOverlap(left, right string) (bool, error) {
	leftHost, leftPort, err := normalizeListenAddress(left)
	if err != nil {
		return false, err
	}
	rightHost, rightPort, err := normalizeListenAddress(right)
	if err != nil {
		return false, err
	}
	if leftPort != rightPort {
		return false, nil
	}
	return leftHost == rightHost || leftHost == "*" || rightHost == "*", nil
}

func normalizeListenAddress(value string) (string, int, error) {
	host, portText, err := net.SplitHostPort(strings.TrimSpace(value))
	if err != nil {
		return "", 0, fmt.Errorf("listen address %q is invalid: %w", value, err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 1 || port > 65535 {
		return "", 0, fmt.Errorf("listen address %q has invalid port", value)
	}
	host = strings.ToLower(strings.TrimSuffix(host, "."))
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "*"
	} else if host == "localhost" {
		host = "127.0.0.1"
	} else if ip := net.ParseIP(host); ip != nil {
		host = ip.String()
	}
	return host, port, nil
}

type ScopeCollision struct {
	Kind       string
	LeftIndex  int
	RightIndex int
	Value      string
}

func (c ScopeCollision) Error() string {
	return fmt.Sprintf("scope collision %s between entries %d and %d (%s)", c.Kind, c.LeftIndex, c.RightIndex, c.Value)
}

// ValidateScopeInventory rejects collisions before any child service starts.
// It treats nested writable roots and wildcard listeners as collisions too.
func ValidateScopeInventory(configs []ScopeProcessConfig) error {
	for index, config := range configs {
		if err := config.Validate(); err != nil {
			return fmt.Errorf("scope entry %d: %w", index, err)
		}
	}
	scopeKeys := make([]string, len(configs))
	for index, config := range configs {
		key, err := config.ScopeKey()
		if err != nil {
			return fmt.Errorf("scope entry %d key: %w", index, err)
		}
		scopeKeys[index] = key
		for prior := 0; prior < index; prior++ {
			if key == scopeKeys[prior] {
				return ScopeCollision{Kind: "scope_key", LeftIndex: prior, RightIndex: index, Value: key}
			}
			if config.GatewayInstanceID == configs[prior].GatewayInstanceID {
				return ScopeCollision{Kind: "gateway_instance", LeftIndex: prior, RightIndex: index, Value: config.GatewayInstanceID}
			}
			if config.Scope.PublisherID == configs[prior].Scope.PublisherID {
				return ScopeCollision{Kind: "publisher_identity", LeftIndex: prior, RightIndex: index, Value: config.Scope.PublisherID}
			}
			if config.CredentialPrefix == configs[prior].CredentialPrefix {
				return ScopeCollision{Kind: "credential_prefix", LeftIndex: prior, RightIndex: index, Value: config.CredentialPrefix}
			}
			listenOverlap, err := normalizedListenOverlap(config.ListenAddress, configs[prior].ListenAddress)
			if err != nil {
				return err
			}
			if listenOverlap {
				return ScopeCollision{Kind: "listen_address", LeftIndex: prior, RightIndex: index, Value: config.ListenAddress}
			}
		}
	}
	for index, config := range configs {
		leftPaths := config.pathValues()
		for prior := 0; prior < index; prior++ {
			rightPaths := configs[prior].pathValues()
			for leftName, leftValue := range leftPaths {
				leftAbs, err := normalizedPath(leftValue)
				if err != nil {
					return err
				}
				for rightName, rightValue := range rightPaths {
					rightAbs, err := normalizedPath(rightValue)
					if err != nil {
						return err
					}
					if pathsOverlap(leftAbs, rightAbs) {
						return ScopeCollision{Kind: "writable_root:" + leftName + ":" + rightName, LeftIndex: prior, RightIndex: index, Value: leftAbs}
					}
				}
			}
		}
	}
	return nil
}

func pathsOverlap(left, right string) bool {
	if left == right {
		return true
	}
	relative, err := filepath.Rel(left, right)
	if err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return true
	}
	relative, err = filepath.Rel(right, left)
	return err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}

// ScopeInventory is the canonical, secret-free input to the operator layer.
// The scope object is the same archive-config-v1 object used by publication;
// operational paths and listener identities are kept outside that trust
// identity so changing a local process layout cannot silently change archive
// scope identity.
type ScopeInventory struct {
	InventoryVersion string
	Scopes           []ScopeProcessConfig
}

func (i ScopeInventory) Validate() error {
	if i.InventoryVersion != ScopeInventoryVersion {
		return fmt.Errorf("unsupported scope inventory version")
	}
	if len(i.Scopes) == 0 {
		return fmt.Errorf("scope inventory is empty")
	}
	if len(i.Scopes) > maxScopeInventoryEntries {
		return fmt.Errorf("scope inventory exceeds implementation bound")
	}
	return ValidateScopeInventory(i.Scopes)
}

func scopeConfigValue(scope archive.ScopeConfig) (map[string]any, error) {
	canonical, err := scope.CanonicalConfigJSON()
	if err != nil {
		return nil, err
	}
	value, err := protocol.DecodeCanonicalJSON(canonical)
	if err != nil {
		return nil, err
	}
	object, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("scope config is not an object")
	}
	return object, nil
}

func (i ScopeInventory) Value() (map[string]any, error) {
	if err := i.Validate(); err != nil {
		return nil, err
	}
	scopes := append([]ScopeProcessConfig(nil), i.Scopes...)
	sort.Slice(scopes, func(left, right int) bool {
		leftKey, _ := scopes[left].ScopeKey()
		rightKey, _ := scopes[right].ScopeKey()
		return leftKey < rightKey
	})
	items := make([]any, 0, len(scopes))
	for _, scope := range scopes {
		scopeValue, err := scopeConfigValue(scope.Scope)
		if err != nil {
			return nil, err
		}
		items = append(items, map[string]any{
			"credential_prefix":   scope.CredentialPrefix,
			"gateway_config_path": scope.GatewayConfigPath,
			"gateway_instance_id": scope.GatewayInstanceID,
			"journal_path":        scope.JournalPath,
			"listen_address":      scope.ListenAddress,
			"lock_root":           scope.LockRoot,
			"mql_config_path":     scope.MQLConfigPath,
			"outbox_root":         scope.OutboxRoot,
			"receipt_root":        scope.ReceiptRoot,
			"scope":               scopeValue,
			"wal_root":            scope.WALRoot,
		})
	}
	return map[string]any{"inventory_version": ScopeInventoryVersion, "scopes": items}, nil
}

func (i ScopeInventory) CanonicalJSON() ([]byte, error) {
	value, err := i.Value()
	if err != nil {
		return nil, err
	}
	return protocol.CanonicalJSON(value)
}

func readInventoryString(object map[string]any, key string) (string, error) {
	value, ok := object[key]
	if !ok {
		return "", fmt.Errorf("scope inventory field %q is missing", key)
	}
	text, ok := value.(string)
	if !ok || text == "" {
		return "", fmt.Errorf("scope inventory field %q must be a non-empty string", key)
	}
	return text, nil
}

func DecodeScopeInventory(data []byte) (ScopeInventory, error) {
	value, err := protocol.DecodeCanonicalJSON(data)
	if err != nil {
		return ScopeInventory{}, fmt.Errorf("decode scope inventory: %w", err)
	}
	object, ok := value.(map[string]any)
	if !ok {
		return ScopeInventory{}, fmt.Errorf("scope inventory must be an object")
	}
	if len(object) != 2 {
		return ScopeInventory{}, fmt.Errorf("scope inventory has unknown or missing fields")
	}
	version, err := readInventoryString(object, "inventory_version")
	if err != nil {
		return ScopeInventory{}, err
	}
	items, ok := object["scopes"].([]any)
	if !ok {
		return ScopeInventory{}, fmt.Errorf("scope inventory scopes must be an array")
	}
	if len(items) == 0 || len(items) > maxScopeInventoryEntries {
		return ScopeInventory{}, fmt.Errorf("scope inventory scope count is outside bounds")
	}
	result := ScopeInventory{InventoryVersion: version, Scopes: make([]ScopeProcessConfig, 0, len(items))}
	wantEntryKeys := map[string]struct{}{
		"credential_prefix": {}, "gateway_config_path": {}, "gateway_instance_id": {}, "journal_path": {},
		"listen_address": {}, "lock_root": {}, "mql_config_path": {}, "outbox_root": {}, "receipt_root": {},
		"scope": {}, "wal_root": {},
	}
	for index, raw := range items {
		entry, ok := raw.(map[string]any)
		if !ok || len(entry) != len(wantEntryKeys) {
			return ScopeInventory{}, fmt.Errorf("scope inventory entry %d is invalid", index)
		}
		for key := range entry {
			if _, known := wantEntryKeys[key]; !known {
				return ScopeInventory{}, fmt.Errorf("scope inventory entry %d has unknown field %q", index, key)
			}
		}
		read := func(key string) (string, error) { return readInventoryString(entry, key) }
		gatewayInstanceID, err := read("gateway_instance_id")
		if err != nil {
			return ScopeInventory{}, err
		}
		listenAddress, err := read("listen_address")
		if err != nil {
			return ScopeInventory{}, err
		}
		gatewayConfigPath, err := read("gateway_config_path")
		if err != nil {
			return ScopeInventory{}, err
		}
		mqlConfigPath, err := read("mql_config_path")
		if err != nil {
			return ScopeInventory{}, err
		}
		walRoot, err := read("wal_root")
		if err != nil {
			return ScopeInventory{}, err
		}
		journalPath, err := read("journal_path")
		if err != nil {
			return ScopeInventory{}, err
		}
		outboxRoot, err := read("outbox_root")
		if err != nil {
			return ScopeInventory{}, err
		}
		receiptRoot, err := read("receipt_root")
		if err != nil {
			return ScopeInventory{}, err
		}
		lockRoot, err := read("lock_root")
		if err != nil {
			return ScopeInventory{}, err
		}
		credentialPrefix, err := read("credential_prefix")
		if err != nil {
			return ScopeInventory{}, err
		}
		scopeObject, ok := entry["scope"].(map[string]any)
		if !ok {
			return ScopeInventory{}, fmt.Errorf("scope inventory entry %d scope must be an object", index)
		}
		scopeBytes, err := protocol.CanonicalJSON(scopeObject)
		if err != nil {
			return ScopeInventory{}, err
		}
		scope, err := archive.ScopeConfigFromCanonicalJSON(scopeBytes)
		if err != nil {
			return ScopeInventory{}, fmt.Errorf("scope inventory entry %d scope: %w", index, err)
		}
		result.Scopes = append(result.Scopes, ScopeProcessConfig{
			Scope: scope, GatewayInstanceID: gatewayInstanceID, ListenAddress: listenAddress,
			GatewayConfigPath: gatewayConfigPath, MQLConfigPath: mqlConfigPath, WALRoot: walRoot,
			JournalPath: journalPath, OutboxRoot: outboxRoot, ReceiptRoot: receiptRoot, LockRoot: lockRoot,
			CredentialPrefix: credentialPrefix,
		})
	}
	if err := result.Validate(); err != nil {
		return ScopeInventory{}, err
	}
	canonical, err := result.CanonicalJSON()
	if err != nil {
		return ScopeInventory{}, err
	}
	if !bytes.Equal(canonical, data) {
		return ScopeInventory{}, fmt.Errorf("scope inventory bytes are not canonical or sorted")
	}
	return result, nil
}

func LoadScopeInventory(path string) (ScopeInventory, error) {
	if path == "" {
		return ScopeInventory{}, fmt.Errorf("scope inventory path is empty")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return ScopeInventory{}, fmt.Errorf("read scope inventory: %w", err)
	}
	return DecodeScopeInventory(data)
}

type SupervisorUnit struct {
	ScopeKey           string
	GatewayServiceName string
	MQLServiceName     string
	ListenAddress      string
	GatewayConfigPath  string
	MQLConfigPath      string
}

type SupervisorPlan struct {
	PlanVersion string
	Units       []SupervisorUnit
}

func BuildSupervisorPlan(configs []ScopeProcessConfig) (SupervisorPlan, error) {
	if err := ValidateScopeInventory(configs); err != nil {
		return SupervisorPlan{}, err
	}
	units := make([]SupervisorUnit, 0, len(configs))
	for _, config := range configs {
		key, err := config.ScopeKey()
		if err != nil {
			return SupervisorPlan{}, err
		}
		short := key[:16]
		units = append(units, SupervisorUnit{
			ScopeKey: key, GatewayServiceName: "tick-gateway-" + short, MQLServiceName: "tick-mql-" + short,
			ListenAddress: config.ListenAddress, GatewayConfigPath: config.GatewayConfigPath, MQLConfigPath: config.MQLConfigPath,
		})
	}
	sort.Slice(units, func(i, j int) bool { return units[i].ScopeKey < units[j].ScopeKey })
	return SupervisorPlan{PlanVersion: ScopeOperationsVersion, Units: units}, nil
}

func (p SupervisorPlan) Value() map[string]any {
	units := make([]any, len(p.Units))
	for index, unit := range p.Units {
		units[index] = map[string]any{
			"gateway_config_path":  unit.GatewayConfigPath,
			"gateway_service_name": unit.GatewayServiceName,
			"listen_address":       unit.ListenAddress,
			"mql_config_path":      unit.MQLConfigPath,
			"mql_service_name":     unit.MQLServiceName,
			"scope_key":            unit.ScopeKey,
		}
	}
	return map[string]any{"plan_version": p.PlanVersion, "units": units}
}

func (p SupervisorPlan) CanonicalJSON() ([]byte, error) {
	if p.PlanVersion != ScopeOperationsVersion {
		return nil, fmt.Errorf("supervisor plan version is invalid")
	}
	return protocol.CanonicalJSON(p.Value())
}

type ScopeHealthStatus struct {
	ScopeKey                   string
	LastDurableSourceTimeMSC   int64
	CurrentSourceTimeMSC       int64
	UncommittedLagMSC          int64
	GatewayDowntimeSeconds     uint64
	WALFreeBytes               uint64
	TerminalSynchronization    string
	OldestRetrievableTickMSC   int64
	PublisherEpoch             uint64
	LastVerifiedSnapshotDigest [32]byte
	BlockedReason              string
}

type AggregateScopeStatus struct {
	StatusVersion string
	Scopes        []ScopeHealthStatus
}

func (s ScopeHealthStatus) Validate() error {
	key, err := protocol.ParseHashHex(s.ScopeKey)
	if err != nil || key == ([32]byte{}) {
		return fmt.Errorf("scope health scope key is not a nonzero SHA-256")
	}
	if s.PublisherEpoch == 0 {
		return fmt.Errorf("scope health publisher epoch is zero")
	}
	return nil
}

func (s ScopeHealthStatus) Value() map[string]any {
	return map[string]any{
		"blocked_reason":                s.BlockedReason,
		"current_source_time_msc":       s.CurrentSourceTimeMSC,
		"gateway_downtime_seconds":      s.GatewayDowntimeSeconds,
		"last_durable_source_time_msc":  s.LastDurableSourceTimeMSC,
		"last_verified_snapshot_digest": protocol.EncodeHashHex(s.LastVerifiedSnapshotDigest),
		"oldest_retrievable_tick_msc":   s.OldestRetrievableTickMSC,
		"publisher_epoch":               s.PublisherEpoch,
		"scope_key":                     s.ScopeKey,
		"terminal_synchronization":      s.TerminalSynchronization,
		"uncommitted_lag_msc":           s.UncommittedLagMSC,
		"wal_free_bytes":                s.WALFreeBytes,
	}
}

func (s AggregateScopeStatus) CanonicalJSON() ([]byte, error) {
	if s.StatusVersion != ScopeOperationsVersion {
		return nil, fmt.Errorf("scope status version is invalid")
	}
	result, err := AggregateScopeHealth(s.Scopes)
	if err != nil {
		return nil, err
	}
	items := make([]any, 0, len(result.Scopes))
	for _, scope := range result.Scopes {
		items = append(items, scope.Value())
	}
	return protocol.CanonicalJSON(map[string]any{"scopes": items, "status_version": ScopeOperationsVersion})
}

func AggregateScopeHealth(statuses []ScopeHealthStatus) (AggregateScopeStatus, error) {
	seen := make(map[string]struct{}, len(statuses))
	result := append([]ScopeHealthStatus(nil), statuses...)
	for _, status := range result {
		if err := status.Validate(); err != nil {
			return AggregateScopeStatus{}, fmt.Errorf("scope health identity is incomplete: %w", err)
		}
		if _, exists := seen[status.ScopeKey]; exists {
			return AggregateScopeStatus{}, fmt.Errorf("scope health identity is duplicated")
		}
		seen[status.ScopeKey] = struct{}{}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ScopeKey < result[j].ScopeKey })
	return AggregateScopeStatus{StatusVersion: ScopeOperationsVersion, Scopes: result}, nil
}
