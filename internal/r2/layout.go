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
	RcloneRoot    string
	Scope         archive.ScopeConfig
	prefix        string
}

func NewLayout(immutableRoot, rcloneRoot string, scope archive.ScopeConfig) (Layout, error) {
	if _, err := scope.ConfigHash(); err != nil {
		return Layout{}, err
	}
	if err := validateRemoteRoot(immutableRoot); err != nil {
		return Layout{}, err
	}
	if rcloneRoot == "" {
		rcloneRoot = immutableRoot
	}
	if err := validateRemoteRoot(rcloneRoot); err != nil {
		return Layout{}, err
	}
	prefix := campaignPrefix(scope)
	return Layout{
		ImmutableRoot: strings.TrimRight(immutableRoot, "/"),
		RcloneRoot:    strings.TrimRight(rcloneRoot, "/"),
		Scope:         scope,
		prefix:        prefix,
	}, nil
}

func CampaignPrefix(scope archive.ScopeConfig) string {
	return campaignPrefix(scope)
}

func campaignPrefix(scope archive.ScopeConfig) string {
	return path.Join(
		"dataset="+archive.IdentityPathKey(scope.DatasetID),
		"provider="+archive.IdentityPathKey(scope.ProviderID),
		"feed="+archive.IdentityPathKey(scope.StableFeedID),
		"symbol="+archive.IdentityPathKey(scope.ExactSourceSymbol),
		"campaign="+archive.IdentityPathKey(scope.CampaignID),
	) + "/"
}

func (l Layout) Prefix() string {
	return l.prefix
}

// ImmutableCampaignPrefix is the only canonical full-key prefix accepted by
// ReplayPublicationBundle sealing. It excludes a trailing slash so Protocol
// V1 can derive every full key as prefix + "/" + relative key.
func (l Layout) ImmutableCampaignPrefix() string {
	return strings.TrimSuffix(joinRemoteKey(l.ImmutableRoot, l.prefix, "_"), "/_")
}

// RcloneCampaignPrefix returns the corresponding pinned-rclone prefix.
func (l Layout) RcloneCampaignPrefix() string {
	return strings.TrimSuffix(joinRemoteKey(l.RcloneRoot, l.prefix, "_"), "/_")
}

func (l Layout) RemoteKey(relative string) (string, error) {
	if err := validateRelativeKey(relative); err != nil {
		return "", err
	}
	return joinRemoteKey(l.ImmutableRoot, l.prefix, relative), nil
}

func (l Layout) RcloneKey(relative string) (string, error) {
	if err := validateRelativeKey(relative); err != nil {
		return "", err
	}
	return joinRemoteKey(l.RcloneRoot, l.prefix, relative), nil
}

func (l Layout) ScopeDescriptorKey() (string, error) {
	return l.RemoteKey("scope-descriptor-v1.json")
}

func (l Layout) ClaimKey(epoch uint64) (string, error) {
	return l.RemoteKey(fmt.Sprintf("publisher-claims/epoch=%d.json", epoch))
}

func (l Layout) HandoverArtifactKey(nextEpoch uint64) (string, error) {
	return protocol.HandoverArtifactKey(l.ImmutableCampaignPrefix(), nextEpoch)
}

func (l Layout) HandoverTransitionKey(nextEpoch uint64) (string, error) {
	return protocol.HandoverTransitionKey(l.ImmutableCampaignPrefix(), nextEpoch)
}

func (l Layout) HandoverCandidatePrefix(nextEpoch uint64) (string, error) {
	key, err := l.ClaimKey(nextEpoch)
	if err != nil {
		return "", err
	}
	return key, nil
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

func (l Layout) RcloneRawObjectKey(object archive.RawChainObject) (string, error) {
	if object.Key != archive.RawWALObjectKey(object.SHA256) {
		return "", fmt.Errorf("%w: raw object key does not match its hash", archive.ErrIntegrity)
	}
	return l.RcloneKey(object.Key)
}

func (l Layout) RcloneManifestKey(manifest archive.RawDayManifest) (string, error) {
	relative, err := l.manifestRelativeKey(manifest)
	if err != nil {
		return "", err
	}
	return l.RcloneKey(relative)
}

func (l Layout) validateDerivativeScope(datasetID, campaignID, dayDefinitionID string) error {
	if datasetID != l.Scope.DatasetID || campaignID != l.Scope.CampaignID || dayDefinitionID != l.Scope.DayDefinitionID {
		return fmt.Errorf("derivative scope does not match trusted layout")
	}
	return nil
}

// ReplayPartObjectKey prepends only this trusted Layout's immutable campaign
// root to the exact Protocol V1 campaign-relative Parquet key.
func (l Layout) ReplayPartObjectKey(part protocol.PartManifest) (string, error) {
	if err := part.Validate(); err != nil {
		return "", err
	}
	if err := l.validateDerivativeScope(part.DatasetID, part.CampaignID, part.DayDefinitionID); err != nil {
		return "", err
	}
	relative, err := protocol.ReplayPartObjectKey(
		protocol.ReplayScope{
			DatasetID: part.DatasetID, CampaignID: part.CampaignID, DayDefinitionID: part.DayDefinitionID,
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
	if err := l.validateDerivativeScope(part.DatasetID, part.CampaignID, part.DayDefinitionID); err != nil {
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
	if err := l.validateDerivativeScope(manifest.DatasetID, manifest.CampaignID, manifest.DayDefinitionID); err != nil {
		return "", err
	}
	relative, err := protocol.ReplayDayManifestKey(manifest)
	if err != nil {
		return "", err
	}
	return l.RemoteKey(relative)
}

// ReplayDerivativePrefix returns the trusted immutable-root prefix for one
// exact campaign-relative replay conversion and UTC date. Callers cannot
// supply a full remote key; the Protocol V1 helper derives the relative base.
func (l Layout) ReplayDerivativePrefix(scope protocol.ReplayScope) (string, error) {
	if err := scope.Validate(); err != nil {
		return "", err
	}
	if err := l.validateDerivativeScope(scope.DatasetID, scope.CampaignID, scope.DayDefinitionID); err != nil {
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

// RcloneReplayDerivativePrefix is the same trusted relative derivation for
// the pinned rclone remote.
func (l Layout) RcloneReplayDerivativePrefix(scope protocol.ReplayScope) (string, error) {
	if err := scope.Validate(); err != nil {
		return "", err
	}
	if err := l.validateDerivativeScope(scope.DatasetID, scope.CampaignID, scope.DayDefinitionID); err != nil {
		return "", err
	}
	base, err := protocol.ReplayDerivativeBaseKey(scope)
	if err != nil {
		return "", err
	}
	full, err := l.RcloneKey(base)
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

func (l Layout) RcloneReplayPartObjectKey(part protocol.PartManifest) (string, error) {
	if _, err := l.ReplayPartObjectKey(part); err != nil {
		return "", err
	}
	return l.RcloneKey(part.PartKey)
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

func (l Layout) RcloneReplayPartManifestKey(part protocol.PartManifest) (string, error) {
	if _, err := l.ReplayPartManifestKey(part); err != nil {
		return "", err
	}
	key, err := protocol.PartManifestKey(part)
	if err != nil {
		return "", err
	}
	return l.RcloneKey(key)
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

func (l Layout) RcloneReplayDayManifestKey(manifest protocol.ReplayDayManifest) (string, error) {
	if _, err := l.ReplayDayManifestKey(manifest); err != nil {
		return "", err
	}
	key, err := protocol.ReplayDayManifestKey(manifest)
	if err != nil {
		return "", err
	}
	return l.RcloneKey(key)
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
