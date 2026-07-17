package r2

import (
	"fmt"
	"path"
	"strings"
	"time"

	"tick-data-platform/internal/archive"
	"tick-data-platform/internal/protocol"
)

type Layout struct {
	ImmutableRoot string
	Scope         archive.ScopeConfig
	prefix        string
}

func NewLayout(immutableRoot string, scope archive.ScopeConfig) (Layout, error) {
	if _, err := scope.ConfigHash(); err != nil {
		return Layout{}, err
	}
	if err := validateRemoteRoot(immutableRoot); err != nil {
		return Layout{}, err
	}
	prefix := scopePrefix(scope)
	return Layout{
		ImmutableRoot: strings.TrimRight(immutableRoot, "/"),
		Scope:         scope,
		prefix:        prefix,
	}, nil
}

func ScopePrefix(scope archive.ScopeConfig) string {
	return scopePrefix(scope)
}

func PublicationRunID(now time.Time) string {
	return now.UTC().Format("20060102T150405.000000000Z")
}

func PublicationRunRoot(immutableRoot, gatewayID, runID string) (string, error) {
	immutableRoot = strings.TrimRight(immutableRoot, "/")
	gatewayID = strings.TrimSpace(gatewayID)
	runID = strings.TrimSpace(runID)
	if err := validateRemoteRoot(immutableRoot); err != nil {
		return "", err
	}
	if gatewayID == "" {
		return "", fmt.Errorf("gateway id is empty")
	}
	if runID == "" {
		return "", fmt.Errorf("publication run id is empty")
	}
	for _, part := range strings.Split(immutableRoot, "/") {
		if strings.HasPrefix(part, "gateway=") || strings.HasPrefix(part, "run=") ||
			strings.HasPrefix(part, "source=") || strings.HasPrefix(part, "symbol=") {
			return "", fmt.Errorf("immutable root must not contain generated publication path segments")
		}
	}
	return path.Join(
		immutableRoot,
		"gateway="+exactPathComponent(gatewayID),
		"run="+exactPathComponent(runID),
	), nil
}

func scopePrefix(scope archive.ScopeConfig) string {
	return path.Join(
		"source="+exactPathComponent(scope.ProviderID),
		"symbol="+exactPathComponent(scope.ExactSourceSymbol),
	) + "/"
}

func exactPathComponent(value string) string {
	var encoded strings.Builder
	for _, b := range []byte(value) {
		if (b >= 'A' && b <= 'Z') ||
			(b >= 'a' && b <= 'z') ||
			(b >= '0' && b <= '9') ||
			b == '-' || b == '_' || b == '.' ||
			b == '*' || b == '\'' || b == '(' || b == ')' {
			encoded.WriteByte(b)
			continue
		}
		encoded.WriteString(fmt.Sprintf("!%02X", b))
	}
	return encoded.String()
}

func (l Layout) Prefix() string {
	return l.prefix
}

// ImmutableScopePrefix is the only canonical full-key prefix accepted by
// ReplayPublicationBundle sealing. It excludes a trailing slash so Protocol
// V1 can derive every full key as prefix + "/" + relative key.
func (l Layout) ImmutableScopePrefix() string {
	return strings.TrimSuffix(joinRemoteKey(l.ImmutableRoot, l.prefix, "_"), "/_")
}

func (l Layout) RemoteKey(relative string) (string, error) {
	if err := validateRelativeKey(relative); err != nil {
		return "", err
	}
	return joinRemoteKey(l.ImmutableRoot, l.prefix, relative), nil
}

func (l Layout) ScopeDescriptorKey() (string, error) {
	return l.RemoteKey("scope-descriptor-v1.json")
}

func (l Layout) ClaimKey(epoch uint64) (string, error) {
	return l.RemoteKey(fmt.Sprintf("publisher-claims/epoch=%d.json", epoch))
}

func (l Layout) ManifestPrefix(date string) (string, error) {
	if err := validateManifestDate(date); err != nil {
		return "", fmt.Errorf("manifest date is not YYYY-MM-DD")
	}
	relative := "snapshots/raw/day-definition=" + archive.IdentityPathKey(l.Scope.DayDefinitionID) + "/date=" + date
	return joinRemoteKey(l.ImmutableRoot, l.prefix, relative) + "/", nil
}

func (l Layout) ManifestKey(manifest archive.RawDayManifest) (string, error) {
	relative, err := l.manifestRelativeKey(manifest)
	if err != nil {
		return "", err
	}
	return l.RemoteKey(relative)
}

func (l Layout) manifestRelativeKey(manifest archive.RawDayManifest) (string, error) {
	if err := validateManifestDate(manifest.Date); err != nil {
		return "", err
	}
	digest, err := archive.ManifestDigest(manifest)
	if err != nil {
		return "", err
	}
	if manifest.ManifestSHA256 != ([32]byte{}) && manifest.ManifestSHA256 != digest {
		return "", fmt.Errorf("%w: manifest key digest does not match canonical manifest", archive.ErrIntegrity)
	}
	return fmt.Sprintf(
		"snapshots/raw/day-definition=%s/date=%s/raw-day-%d-%x.json",
		archive.IdentityPathKey(l.Scope.DayDefinitionID),
		manifest.Date,
		manifest.Revision,
		digest,
	), nil
}

func validateManifestDate(date string) error {
	parsed, err := time.Parse("2006-01-02", date)
	if err != nil || parsed.Format("2006-01-02") != date {
		return fmt.Errorf("manifest date is not UTC YYYY-MM-DD")
	}
	return nil
}

func (l Layout) RawObjectKey(object archive.RawChainObject) (string, error) {
	if object.Key != archive.RawWALObjectKey(object.SHA256) {
		return "", fmt.Errorf("%w: raw object key does not match its hash", archive.ErrIntegrity)
	}
	return l.RemoteKey(object.Key)
}

func (l Layout) ManifestObjectKey(manifest archive.RawDayManifest) (string, error) {
	relative, err := l.manifestRelativeKey(manifest)
	if err != nil {
		return "", err
	}
	return l.RemoteKey(relative)
}

func (l Layout) validateDerivativeScope(datasetID, dayDefinitionID string) error {
	if datasetID != l.Scope.DatasetID || dayDefinitionID != l.Scope.DayDefinitionID {
		return fmt.Errorf("derivative scope does not match trusted layout")
	}
	return nil
}

// ReplayPartObjectKey prepends only this trusted Layout's immutable scope root
// to the exact Protocol key.
func (l Layout) ReplayPartObjectKey(part protocol.PartManifest) (string, error) {
	if err := part.Validate(); err != nil {
		return "", err
	}
	if err := l.validateDerivativeScope(part.DatasetID, part.DayDefinitionID); err != nil {
		return "", err
	}
	relative, err := protocol.ReplayPartObjectKey(
		protocol.ReplayScope{
			DatasetID: part.DatasetID, DayDefinitionID: part.DayDefinitionID,
			Date: part.Date, ReplayContractID: part.ReplayContractID, ConversionID: part.ConversionID,
			RawDayManifestKey: part.RawDayManifestKey, RawDayManifestSHA256: part.RawDayManifestSHA256,
		}, part.FirstStreamSequence, part.LastStreamSequence, part.PartSHA256,
	)
	if err != nil || relative != part.PartKey {
		return "", fmt.Errorf("part object key is not the exact Protocol V1 key")
	}
	return l.RemoteKey(relative)
}

func (l Layout) ReplayPartManifestKey(part protocol.PartManifest) (string, error) {
	if err := part.Validate(); err != nil {
		return "", err
	}
	if err := l.validateDerivativeScope(part.DatasetID, part.DayDefinitionID); err != nil {
		return "", err
	}
	relative, err := protocol.PartManifestKey(part)
	if err != nil {
		return "", err
	}
	return l.RemoteKey(relative)
}

func (l Layout) ReplayDayManifestKey(manifest protocol.ReplayDayManifest) (string, error) {
	if err := manifest.Validate(); err != nil {
		return "", err
	}
	if err := l.validateDerivativeScope(manifest.DatasetID, manifest.DayDefinitionID); err != nil {
		return "", err
	}
	relative, err := protocol.ReplayDayManifestKey(manifest)
	if err != nil {
		return "", err
	}
	return l.RemoteKey(relative)
}

// ReplayDerivativePrefix returns the trusted immutable-root prefix for one
// exact scope-relative replay conversion and UTC date. Callers cannot
// supply a full remote key; the Protocol V1 helper derives the relative base.
func (l Layout) ReplayDerivativePrefix(scope protocol.ReplayScope) (string, error) {
	if err := scope.Validate(); err != nil {
		return "", err
	}
	if err := l.validateDerivativeScope(scope.DatasetID, scope.DayDefinitionID); err != nil {
		return "", err
	}
	base, err := protocol.ReplayDerivativeBaseKey(scope)
	if err != nil {
		return "", err
	}
	full, err := l.RemoteKey(base)
	if err != nil {
		return "", err
	}
	return full + "/", nil
}

func (l Layout) VerifyReplayPartObjectKey(part protocol.PartManifest, fullKey string) error {
	want, err := l.ReplayPartObjectKey(part)
	if err != nil {
		return err
	}
	if fullKey != want {
		return fmt.Errorf("replay part object key does not match trusted layout")
	}
	return nil
}

func (l Layout) VerifyReplayPartManifestKey(part protocol.PartManifest, fullKey string) error {
	want, err := l.ReplayPartManifestKey(part)
	if err != nil {
		return err
	}
	if fullKey != want {
		return fmt.Errorf("replay part manifest key does not match trusted layout")
	}
	return nil
}

func (l Layout) VerifyReplayDayManifestKey(manifest protocol.ReplayDayManifest, fullKey string) error {
	want, err := l.ReplayDayManifestKey(manifest)
	if err != nil {
		return err
	}
	if fullKey != want {
		return fmt.Errorf("replay day manifest key does not match trusted layout")
	}
	return nil
}

func validateRemoteRoot(root string) error {
	if root == "" || strings.ContainsAny(root, "\\\r\n") || strings.HasPrefix(root, "//") {
		return fmt.Errorf("remote root is empty or contains forbidden characters")
	}
	if strings.HasPrefix(root, "/") || strings.Contains(root, "/../") || strings.HasSuffix(root, "/..") || strings.Contains(root, "/./") || strings.HasSuffix(root, "/.") {
		return fmt.Errorf("remote root is not canonical")
	}
	if len(root) >= 2 && root[1] == ':' && ((root[0] >= 'A' && root[0] <= 'Z') || (root[0] >= 'a' && root[0] <= 'z')) {
		return fmt.Errorf("remote root must not be a drive path")
	}
	return nil
}

func validateRelativeKey(key string) error {
	if key == "" || strings.HasPrefix(key, "/") || strings.ContainsAny(key, "\\\r\n") {
		return fmt.Errorf("remote relative key is not canonical")
	}
	parts := strings.Split(key, "/")
	for _, part := range parts {
		if part == "" || part == "." || part == ".." {
			return fmt.Errorf("remote relative key contains an empty or dot segment")
		}
	}
	return nil
}

func joinRemoteKey(root, prefix, relative string) string {
	return strings.TrimRight(root, "/") + "/" + strings.Trim(prefix, "/") + "/" + strings.TrimLeft(relative, "/")
}
