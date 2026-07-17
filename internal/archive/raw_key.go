package archive

import (
	"encoding/hex"
	"fmt"
)

// RawWALObjectKey returns the canonical scope-relative key for a sealed WAL
// object. The key is ASCII and is derived only from the complete object hash.
func RawWALObjectKey(hash [32]byte) string {
	return "objects/raw/wal-" + hex.EncodeToString(hash[:]) + ".rtw"
}

// RawDayManifestRelativeKey returns the canonical scope-relative raw-day
// manifest key. r2.Layout adds the immutable scope prefix and remote root;
// replay input verification uses this relative form so the layout derivation
// remains in one place.
func RawDayManifestRelativeKey(scope ScopeConfig, manifest RawDayManifest) (string, error) {
	if manifest.DatasetID != scope.DatasetID || manifest.DayDefinitionID != scope.DayDefinitionID {
		return "", fmt.Errorf("%w: raw-day manifest key scope mismatch", ErrIntegrity)
	}
	if manifest.Date == "" || manifest.Revision == 0 {
		return "", fmt.Errorf("%w: raw-day manifest key identity is incomplete", ErrIntegrity)
	}
	digest, err := ManifestDigest(manifest)
	if err != nil {
		return "", err
	}
	if manifest.ManifestSHA256 != ([32]byte{}) && manifest.ManifestSHA256 != digest {
		return "", fmt.Errorf("%w: raw-day manifest key digest mismatch", ErrIntegrity)
	}
	return fmt.Sprintf(
		"snapshots/raw/day-definition=%s/date=%s/raw-day-%d-%x.json",
		IdentityPathKey(manifest.DayDefinitionID), manifest.Date, manifest.Revision, digest,
	), nil
}

// VerifyRawDayManifestRelativeKey accepts only the exact scope-relative
// key derived from the manifest. A full remote key requires a trusted
// r2.Layout and is intentionally outside this API's trust boundary.
func VerifyRawDayManifestRelativeKey(scope ScopeConfig, manifest RawDayManifest, key string) error {
	relative, err := RawDayManifestRelativeKey(scope, manifest)
	if err != nil {
		return err
	}
	if key != relative {
		return fmt.Errorf("%w: raw-day manifest relative key mismatch", ErrIntegrity)
	}
	return nil
}

func validateRawWALObjectKey(key string, hash [32]byte) error {
	if key != RawWALObjectKey(hash) {
		return fmt.Errorf("%w: raw WAL object key is not canonical for its SHA-256", ErrIntegrity)
	}
	return nil
}
