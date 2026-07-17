package retention

import (
	"crypto/sha256"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"tick-data-platform/internal/archive"
	"tick-data-platform/internal/wal"
)

const defaultWALInventoryBytes = uint64(1 << 30)

// InventoryWALSegments reopens every sealed segment below the configured WAL
// root and returns only byte-exact, trailer-verified identities. It never
// follows a child symlink and never returns the active WAL as a candidate.
func InventoryWALSegments(root string, maxObjects uint64, maxBytes ...uint64) ([]LocalArtifact, error) {
	if root == "" || maxObjects == 0 {
		return nil, fmt.Errorf("WAL inventory root and object limit are required")
	}
	byteLimit := defaultWALInventoryBytes
	if len(maxBytes) > 1 {
		return nil, fmt.Errorf("WAL inventory accepts at most one byte limit")
	}
	if len(maxBytes) == 1 {
		byteLimit = maxBytes[0]
	}
	if byteLimit == 0 {
		return nil, fmt.Errorf("WAL inventory byte limit is required")
	}
	rootInfo, err := os.Lstat(root)
	if err != nil {
		return nil, fmt.Errorf("stat WAL inventory root: %w", err)
	}
	if rootInfo.Mode()&os.ModeSymlink != 0 || !rootInfo.IsDir() {
		return nil, fmt.Errorf("WAL inventory root is not a trusted directory")
	}
	sealedRoot := filepath.Join(root, "sealed")
	sealedInfo, err := os.Lstat(sealedRoot)
	if err != nil {
		return nil, fmt.Errorf("stat sealed WAL inventory: %w", err)
	}
	if sealedInfo.Mode()&os.ModeSymlink != 0 || !sealedInfo.IsDir() {
		return nil, fmt.Errorf("sealed WAL inventory is not a trusted directory")
	}
	entries, err := os.ReadDir(sealedRoot)
	if err != nil {
		return nil, fmt.Errorf("read sealed WAL inventory: %w", err)
	}
	result := make([]LocalArtifact, 0, len(entries))
	var totalBytes uint64
	for _, entry := range entries {
		if entry.Type()&os.ModeSymlink != 0 {
			return nil, fmt.Errorf("sealed WAL inventory contains a symlink: %s", entry.Name())
		}
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".wal") {
			continue
		}
		if uint64(len(result)) >= maxObjects {
			return nil, fmt.Errorf("sealed WAL inventory exceeds object limit")
		}
		path := filepath.Join(sealedRoot, entry.Name())
		info, err := entry.Info()
		if err != nil {
			return nil, fmt.Errorf("stat sealed WAL %s: %w", entry.Name(), err)
		}
		if info.Size() < 0 || uint64(info.Size()) > byteLimit-totalBytes {
			return nil, fmt.Errorf("sealed WAL inventory exceeds byte limit")
		}
		segment, err := wal.VerifySealedSegment(path)
		if err != nil {
			return nil, fmt.Errorf("verify sealed WAL %s: %w", entry.Name(), err)
		}
		if entry.Name() != SegmentNameFromRange(segment.StartSequence, segment.LastSequence) {
			return nil, fmt.Errorf("sealed WAL filename does not match its verified range: %s", entry.Name())
		}
		relative, err := trustedRelative(root, path)
		if err != nil {
			return nil, err
		}
		result = append(result, LocalArtifact{
			Kind: ArtifactWALSegment, TrustedPath: relative, Bytes: uint64(segment.FileBytes), ContentSHA256: segment.ObjectSHA256,
			WALRange: &WALRange{StartSequence: segment.StartSequence, EndSequence: segment.LastSequence, StartChainRoot: segment.ChainStart, EndChainRoot: segment.ChainRoot},
		})
		totalBytes += uint64(segment.FileBytes)
		if totalBytes > byteLimit {
			return nil, fmt.Errorf("sealed WAL inventory exceeds byte limit")
		}
	}
	return result, nil
}

// InventoryFiles inventories regular files below one configured root for an
// outbox or cache policy. It hashes the exact bytes and treats symlinks,
// directories that escape the root, and oversize files as integrity stops.
func InventoryFiles(root, kind string, maxObjects, maxBytes uint64) ([]LocalArtifact, error) {
	if root == "" || maxObjects == 0 || maxBytes == 0 {
		return nil, fmt.Errorf("file inventory limits are required")
	}
	if kind != ArtifactRawOutbox && kind != ArtifactReplayOutbox && kind != ArtifactCache {
		return nil, fmt.Errorf("unsupported file inventory kind %q", kind)
	}
	result := make([]LocalArtifact, 0)
	var totalBytes uint64
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relativeToRoot, relativeErr := filepath.Rel(root, path)
		if relativeErr != nil {
			return relativeErr
		}
		if kind == ArtifactRawOutbox && relativeToRoot == "prune-completions" {
			if entry.Type()&os.ModeSymlink != 0 || !entry.IsDir() {
				return fmt.Errorf("raw outbox completion metadata directory is unsafe")
			}
			return fs.SkipDir
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("file inventory contains a symlink: %s", path)
		}
		if entry.IsDir() {
			return nil
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("file inventory contains a non-regular file: %s", path)
		}
		if uint64(len(result)) >= maxObjects {
			return fmt.Errorf("file inventory exceeds object limit")
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if info.Size() < 0 || uint64(info.Size()) > maxBytes {
			return fmt.Errorf("file inventory object exceeds byte limit: %s", path)
		}
		bytes := uint64(info.Size())
		if totalBytes > maxBytes || bytes > maxBytes-totalBytes {
			return fmt.Errorf("file inventory exceeds cumulative byte limit: %s", path)
		}
		digest, err := hashFile(path)
		if err != nil {
			return err
		}
		relative, err := trustedRelative(root, path)
		if err != nil {
			return err
		}
		artifact := LocalArtifact{Kind: kind, TrustedPath: relative, Bytes: uint64(info.Size()), ContentSHA256: digest}
		if kind == ArtifactRawOutbox {
			if relative != archive.RawWALObjectKey(digest) {
				return fmt.Errorf("raw outbox path does not match its content digest: %s", relative)
			}
			body, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			segment, err := wal.VerifySealedBytes(relative, body)
			if err != nil || segment.ObjectSHA256 != digest || uint64(segment.FileBytes) != uint64(info.Size()) {
				return fmt.Errorf("raw outbox object is not a verified sealed WAL: %s", relative)
			}
			artifact.WALRange = &WALRange{StartSequence: segment.StartSequence, EndSequence: segment.LastSequence, StartChainRoot: segment.ChainStart, EndChainRoot: segment.ChainRoot}
		}
		result = append(result, artifact)
		totalBytes += bytes
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("inventory files: %w", err)
	}
	return result, nil
}

func trustedRelative(root, path string) (string, error) {
	absoluteRoot, err := filepath.Abs(root)
	if err != nil {
		return "", fmt.Errorf("resolve inventory root: %w", err)
	}
	absolutePath, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("resolve inventory path: %w", err)
	}
	relative, err := filepath.Rel(absoluteRoot, absolutePath)
	if err != nil || relative == "." || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("inventory path escapes configured root")
	}
	return filepath.ToSlash(relative), nil
}

func hashFile(path string) ([32]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return [32]byte{}, fmt.Errorf("open inventory object: %w", err)
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return [32]byte{}, fmt.Errorf("hash inventory object: %w", err)
	}
	var result [32]byte
	copy(result[:], hash.Sum(nil))
	return result, nil
}
