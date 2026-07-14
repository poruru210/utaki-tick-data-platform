package archive

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	"tick-data-platform/internal/wal"
)

var ErrIntegrity = errors.New("raw archive integrity failure")

// RawObject is an immutable, byte-exact copy of a verified sealed WAL segment.
type RawObject struct {
	Key     string
	Path    string
	SHA256  [32]byte
	Bytes   int64
	Segment wal.VerifiedSegment
}

// PromoteSealedSegment verifies a sealed WAL and publishes its exact bytes to a
// content-addressed local outbox without overwriting an existing object.
func PromoteSealedSegment(outboxRoot, sealedPath string) (RawObject, error) {
	if outboxRoot == "" {
		return RawObject{}, fmt.Errorf("outbox root is empty")
	}
	source, err := wal.VerifySealedSegment(sealedPath)
	if err != nil {
		return RawObject{}, fmt.Errorf("%w: source is not a verified sealed WAL: %v", ErrIntegrity, err)
	}
	key := RawWALObjectKey(source.ObjectSHA256)
	destination := filepath.Join(outboxRoot, filepath.FromSlash(key))
	if err := os.MkdirAll(filepath.Dir(destination), 0o700); err != nil {
		return RawObject{}, fmt.Errorf("create raw outbox directory: %w", err)
	}

	if _, err := os.Stat(destination); err == nil {
		return verifyExistingObject(key, destination, sealedPath, source)
	} else if !errors.Is(err, fs.ErrNotExist) {
		return RawObject{}, fmt.Errorf("stat raw outbox object: %w", err)
	}

	temp, err := os.CreateTemp(filepath.Dir(destination), ".raw-wal-segment-v1-*.tmp")
	if err != nil {
		return RawObject{}, fmt.Errorf("create raw outbox temporary file: %w", err)
	}
	tempPath := temp.Name()
	defer os.Remove(tempPath)
	if err := temp.Chmod(0o600); err != nil {
		_ = temp.Close()
		return RawObject{}, fmt.Errorf("set raw outbox temporary permissions: %w", err)
	}
	sourceFile, err := os.Open(sealedPath)
	if err != nil {
		_ = temp.Close()
		return RawObject{}, fmt.Errorf("open sealed WAL for promote: %w", err)
	}
	_, copyErr := io.Copy(temp, sourceFile)
	closeSourceErr := sourceFile.Close()
	if copyErr != nil {
		_ = temp.Close()
		return RawObject{}, fmt.Errorf("copy sealed WAL to outbox temporary file: %w", copyErr)
	}
	if closeSourceErr != nil {
		_ = temp.Close()
		return RawObject{}, fmt.Errorf("close sealed WAL after promote copy: %w", closeSourceErr)
	}
	if err := temp.Sync(); err != nil {
		_ = temp.Close()
		return RawObject{}, fmt.Errorf("sync raw outbox temporary file: %w", err)
	}
	if err := temp.Close(); err != nil {
		return RawObject{}, fmt.Errorf("close raw outbox temporary file: %w", err)
	}
	temporary, err := wal.VerifySealedSegment(tempPath)
	if err != nil {
		return RawObject{}, fmt.Errorf("%w: temporary outbox object failed sealed verification: %v", ErrIntegrity, err)
	}
	if temporary.ObjectSHA256 != source.ObjectSHA256 || temporary.FileBytes != source.FileBytes {
		return RawObject{}, fmt.Errorf("%w: temporary outbox object hash or size mismatch", ErrIntegrity)
	}
	equal, err := filesEqual(tempPath, sealedPath)
	if err != nil {
		return RawObject{}, err
	}
	if !equal {
		return RawObject{}, fmt.Errorf("%w: temporary outbox object bytes differ", ErrIntegrity)
	}

	if err := os.Link(tempPath, destination); err != nil {
		if errors.Is(err, fs.ErrExist) {
			return verifyExistingObject(key, destination, sealedPath, source)
		}
		return RawObject{}, fmt.Errorf("atomically publish raw outbox object: %w", err)
	}
	if err := os.Remove(tempPath); err != nil {
		return RawObject{}, fmt.Errorf("remove linked raw outbox temporary file: %w", err)
	}
	return verifyExistingObject(key, destination, sealedPath, source)
}

func verifyExistingObject(
	key string,
	destination string,
	sourcePath string,
	source wal.VerifiedSegment,
) (RawObject, error) {
	existing, err := wal.VerifySealedSegment(destination)
	if err != nil {
		return RawObject{}, fmt.Errorf("%w: existing outbox object is invalid: %v", ErrIntegrity, err)
	}
	if existing.ObjectSHA256 != source.ObjectSHA256 || existing.FileBytes != source.FileBytes {
		return RawObject{}, fmt.Errorf("%w: existing outbox object has different content", ErrIntegrity)
	}
	equal, err := filesEqual(destination, sourcePath)
	if err != nil {
		return RawObject{}, err
	}
	if !equal {
		return RawObject{}, fmt.Errorf("%w: existing outbox object bytes differ", ErrIntegrity)
	}
	return RawObject{
		Key:     key,
		Path:    destination,
		SHA256:  existing.ObjectSHA256,
		Bytes:   existing.FileBytes,
		Segment: existing,
	}, nil
}

func filesEqual(left, right string) (bool, error) {
	leftBytes, err := os.ReadFile(left)
	if err != nil {
		return false, fmt.Errorf("read %s: %w", left, err)
	}
	rightBytes, err := os.ReadFile(right)
	if err != nil {
		return false, fmt.Errorf("read %s: %w", right, err)
	}
	return bytes.Equal(leftBytes, rightBytes), nil
}
