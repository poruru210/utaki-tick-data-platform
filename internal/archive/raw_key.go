package archive

import (
	"encoding/hex"
	"fmt"
)

// RawWALObjectKey returns the canonical campaign-relative key for a sealed WAL
// object. The key is ASCII and is derived only from the complete object hash.
func RawWALObjectKey(hash [32]byte) string {
	return "objects/raw/wal-" + hex.EncodeToString(hash[:]) + ".rtw"
}

func validateRawWALObjectKey(key string, hash [32]byte) error {
	if key != RawWALObjectKey(hash) {
		return fmt.Errorf("%w: raw WAL object key is not canonical for its SHA-256", ErrIntegrity)
	}
	return nil
}
