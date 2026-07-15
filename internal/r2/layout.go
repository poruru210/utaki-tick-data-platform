package r2

import (
	"fmt"
	"path"
	"strings"
	"time"

	"tick-data-platform/internal/archive"
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
