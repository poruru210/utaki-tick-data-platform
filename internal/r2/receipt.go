package r2

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"

	"tick-data-platform/internal/archive"
	"tick-data-platform/internal/protocol"
)

type VerificationReceipt struct {
	ReceiptVersion       string
	ClaimHash            [32]byte
	ScopeConfigHash      [32]byte
	ManifestKey          string
	ManifestSHA256       [32]byte
	RawObjects           []PublicationObject
	RcloneVersion        string
	RcloneGOOS           string
	RcloneGOARCH         string
	RcloneBinarySHA256   string
	VerificationComplete bool
}

func (r VerificationReceipt) CanonicalJSON() ([]byte, error) {
	if r.ReceiptVersion != "publication-verification-receipt-v1" || r.ManifestKey == "" || !r.VerificationComplete {
		return nil, fmt.Errorf("verification receipt is incomplete")
	}
	objects := make([]any, len(r.RawObjects))
	for i, object := range r.RawObjects {
		objects[i] = map[string]any{
			"bytes":      object.Bytes,
			"key":        object.Key,
			"remote_key": object.RemoteKey,
			"rclone_key": object.RcloneKey,
			"sha256":     hex.EncodeToString(object.SHA256[:]),
		}
	}
	return protocol.CanonicalJSON(map[string]any{
		"claim_hash":            hex.EncodeToString(r.ClaimHash[:]),
		"manifest_key":          r.ManifestKey,
		"manifest_sha256":       hex.EncodeToString(r.ManifestSHA256[:]),
		"raw_objects":           objects,
		"receipt_version":       r.ReceiptVersion,
		"rclone_binary_sha256":  r.RcloneBinarySHA256,
		"rclone_goarch":         r.RcloneGOARCH,
		"rclone_goos":           r.RcloneGOOS,
		"rclone_version":        r.RcloneVersion,
		"scope_config_hash":     hex.EncodeToString(r.ScopeConfigHash[:]),
		"verification_complete": r.VerificationComplete,
	})
}

func SaveVerificationReceipt(path string, receipt VerificationReceipt) error {
	canonical, err := receipt.CanonicalJSON()
	if err != nil {
		return err
	}
	return saveNoClobberBytes(path, canonical, ".verification-receipt-*.tmp")
}

// saveNoClobberBytes publishes a complete, synced file by linking a same-
// directory temporary inode into the final name.  A process crash can leave
// only the temporary inode; it cannot expose a partially written final file.
func saveNoClobberBytes(path string, canonical []byte, pattern string) error {
	if path == "" {
		return fmt.Errorf("verification receipt path is empty")
	}
	if parent := filepath.Dir(path); parent != "." {
		if err := os.MkdirAll(parent, 0o700); err != nil {
			return fmt.Errorf("create verification receipt directory")
		}
	}
	directory := filepath.Dir(path)
	file, err := os.CreateTemp(directory, pattern)
	if err == nil {
		temporary := file.Name()
		defer os.Remove(temporary)
		if err := file.Chmod(0o600); err != nil {
			_ = file.Close()
			return fmt.Errorf("set verification receipt permissions")
		}
		if _, err := file.Write(canonical); err != nil {
			_ = file.Close()
			return fmt.Errorf("write verification receipt")
		}
		if err := file.Sync(); err != nil {
			_ = file.Close()
			return fmt.Errorf("sync verification receipt")
		}
		if err := file.Close(); err != nil {
			return fmt.Errorf("close verification receipt")
		}
		if err := os.Link(temporary, path); err == nil {
			return nil
		} else if !os.IsExist(err) {
			return fmt.Errorf("publish verification receipt")
		}
	}
	if err != nil && !os.IsExist(err) {
		return fmt.Errorf("create verification receipt")
	}
	existing, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read verification receipt")
	}
	if !bytes.Equal(existing, canonical) {
		return fmt.Errorf("%w: verification receipt content differs", archive.ErrIntegrity)
	}
	return nil
}
