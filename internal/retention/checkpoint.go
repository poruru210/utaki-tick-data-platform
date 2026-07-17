package retention

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"

	"tick-data-platform/internal/wal"
)

var (
	ErrCheckpointAbsent  = errors.New("prune checkpoint is absent")
	ErrPruneIntegrity    = errors.New("local prune integrity failure")
	ErrPruneAvailability = errors.New("local prune availability failure")
)

const maxCheckpointFiles = uint64(1 << 20)

// CheckpointRecord keeps the canonical bytes' digest beside the decoded
// checkpoint so chain validation never reuses an unverified filename or path.
type CheckpointRecord struct {
	Checkpoint PruneCheckpoint
	Digest     [32]byte
}

func CheckpointDirectory(root string) string {
	return filepath.Join(root, "checkpoints")
}

func TrashDirectory(root string) string {
	return filepath.Join(root, "trash")
}

func trustedPruneDirectory(root, name string, create bool) (string, error) {
	if root == "" || name == "" || strings.ContainsAny(name, "/\\\r\n") || name == "." || name == ".." {
		return "", fmt.Errorf("prune directory identity is invalid")
	}
	absoluteRoot, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	rootInfo, err := os.Lstat(absoluteRoot)
	if err != nil {
		return "", err
	}
	if rootInfo.Mode()&os.ModeSymlink != 0 || !rootInfo.IsDir() {
		return "", fmt.Errorf("prune root is not trusted")
	}
	directory := filepath.Join(absoluteRoot, name)
	info, err := os.Lstat(directory)
	if errors.Is(err, os.ErrNotExist) && create {
		if mkdirErr := os.Mkdir(directory, 0o700); mkdirErr != nil && !errors.Is(mkdirErr, os.ErrExist) {
			return "", mkdirErr
		}
		info, err = os.Lstat(directory)
	}
	if err != nil {
		return "", err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return "", fmt.Errorf("prune child directory is not trusted")
	}
	return directory, nil
}

func checkpointFilename(endSequence uint64) string {
	return fmt.Sprintf("checkpoint-%020d.json", endSequence)
}

// PublishCheckpoint durably publishes one append-only checkpoint without
// clobbering an existing sequence. A retry with identical canonical bytes is
// idempotent; different bytes at the same sequence are an integrity stop.
func PublishCheckpoint(root string, checkpoint PruneCheckpoint) error {
	canonical, err := checkpoint.CanonicalJSON()
	if err != nil {
		return fmt.Errorf("%w: checkpoint validation: %v", ErrPruneIntegrity, err)
	}
	directory, err := trustedPruneDirectory(root, "checkpoints", true)
	if err != nil {
		return fmt.Errorf("%w: create checkpoint directory: %v", ErrPruneAvailability, err)
	}
	destination := filepath.Join(directory, checkpointFilename(checkpoint.EndSequence))
	if existing, readErr := os.ReadFile(destination); readErr == nil {
		if bytes.Equal(existing, canonical) {
			return nil
		}
		return fmt.Errorf("%w: checkpoint sequence already has different bytes", ErrPruneIntegrity)
	} else if !errors.Is(readErr, os.ErrNotExist) {
		return fmt.Errorf("%w: inspect checkpoint destination: %v", ErrPruneAvailability, readErr)
	}
	chain, chainErr := LoadCheckpointChain(root)
	if chainErr != nil && !errors.Is(chainErr, ErrCheckpointAbsent) {
		return chainErr
	}
	if len(chain) == 0 {
		if checkpoint.PreviousCheckpointDigest != ([32]byte{}) {
			return fmt.Errorf("%w: genesis checkpoint has a predecessor", ErrPruneIntegrity)
		}
	} else {
		previous := chain[len(chain)-1]
		if checkpoint.EndSequence <= previous.Checkpoint.EndSequence || checkpoint.PreviousCheckpointDigest != previous.Digest {
			return fmt.Errorf("%w: checkpoint chain is not append-only", ErrPruneIntegrity)
		}
	}
	temporary, err := os.CreateTemp(directory, ".checkpoint-*.tmp")
	if err != nil {
		return fmt.Errorf("%w: create checkpoint temporary file: %v", ErrPruneAvailability, err)
	}
	temporaryName := temporary.Name()
	defer func() {
		_ = os.Remove(temporaryName)
	}()
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("%w: set checkpoint permissions: %v", ErrPruneAvailability, err)
	}
	if _, err := temporary.Write(canonical); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("%w: write checkpoint: %v", ErrPruneAvailability, err)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("%w: sync checkpoint: %v", ErrPruneAvailability, err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("%w: close checkpoint: %v", ErrPruneAvailability, err)
	}
	if err := os.Link(temporaryName, destination); err != nil {
		if errors.Is(err, os.ErrExist) {
			existing, readErr := os.ReadFile(destination)
			if readErr == nil && bytes.Equal(existing, canonical) {
				return nil
			}
			return fmt.Errorf("%w: checkpoint publish raced with different bytes", ErrPruneIntegrity)
		}
		return fmt.Errorf("%w: no-clobber checkpoint publish: %v", ErrPruneAvailability, err)
	}
	if err := syncDirectory(directory); err != nil {
		return fmt.Errorf("%w: sync checkpoint directory: %v", ErrPruneAvailability, err)
	}
	if err := syncDirectory(filepath.Dir(directory)); err != nil {
		return fmt.Errorf("%w: sync checkpoint root directory: %v", ErrPruneAvailability, err)
	}
	return nil
}

// LoadCheckpointChain strictly loads and validates the complete local chain.
// Temporary files left by a crash are uncommitted and ignored; unknown
// committed-looking files, symlinks, malformed JSON, gaps, or branches stop.
func LoadCheckpointChain(root string) ([]CheckpointRecord, error) {
	directory, err := trustedPruneDirectory(root, "checkpoints", false)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, ErrCheckpointAbsent
		}
		return nil, fmt.Errorf("%w: inspect checkpoint directory: %v", ErrPruneIntegrity, err)
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil, fmt.Errorf("%w: read checkpoint directory: %v", ErrPruneAvailability, err)
	}
	if uint64(len(entries)) > maxCheckpointFiles {
		return nil, fmt.Errorf("%w: checkpoint count exceeds limit", ErrPruneIntegrity)
	}
	type checkpointFile struct {
		end  uint64
		path string
	}
	files := make([]checkpointFile, 0, len(entries))
	for _, entry := range entries {
		info, infoErr := entry.Info()
		if infoErr != nil {
			return nil, fmt.Errorf("%w: stat checkpoint entry: %v", ErrPruneAvailability, infoErr)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return nil, fmt.Errorf("%w: checkpoint directory contains symlink %s", ErrPruneIntegrity, entry.Name())
		}
		if entry.IsDir() {
			return nil, fmt.Errorf("%w: checkpoint directory contains child directory %s", ErrPruneIntegrity, entry.Name())
		}
		if strings.HasPrefix(entry.Name(), ".checkpoint-") && strings.HasSuffix(entry.Name(), ".tmp") {
			continue
		}
		end, ok := parseCheckpointFilename(entry.Name())
		if !ok || !info.Mode().IsRegular() {
			return nil, fmt.Errorf("%w: unknown checkpoint entry %s", ErrPruneIntegrity, entry.Name())
		}
		files = append(files, checkpointFile{end: end, path: filepath.Join(directory, entry.Name())})
	}
	if len(files) == 0 {
		return nil, ErrCheckpointAbsent
	}
	sort.Slice(files, func(i, j int) bool { return files[i].end < files[j].end })
	chain := make([]CheckpointRecord, 0, len(files))
	for _, file := range files {
		data, readErr := os.ReadFile(file.path)
		if readErr != nil {
			return nil, fmt.Errorf("%w: read checkpoint: %v", ErrPruneAvailability, readErr)
		}
		checkpoint, decodeErr := DecodePruneCheckpoint(data)
		if decodeErr != nil || checkpoint.EndSequence != file.end {
			return nil, fmt.Errorf("%w: checkpoint file identity is invalid", ErrPruneIntegrity)
		}
		canonical, canonicalErr := checkpoint.CanonicalJSON()
		if canonicalErr != nil || !bytes.Equal(canonical, data) {
			return nil, fmt.Errorf("%w: checkpoint bytes are not canonical", ErrPruneIntegrity)
		}
		digest := PruneCheckpointDigest(canonical)
		if len(chain) == 0 {
			if checkpoint.PreviousCheckpointDigest != ([32]byte{}) {
				return nil, fmt.Errorf("%w: first checkpoint has a predecessor", ErrPruneIntegrity)
			}
		} else {
			previous := chain[len(chain)-1]
			if checkpoint.EndSequence <= previous.Checkpoint.EndSequence || checkpoint.PreviousCheckpointDigest != previous.Digest {
				return nil, fmt.Errorf("%w: checkpoint chain predecessor mismatch", ErrPruneIntegrity)
			}
		}
		chain = append(chain, CheckpointRecord{Checkpoint: checkpoint, Digest: digest})
	}
	return chain, nil
}

func LoadLatestCheckpoint(root string) (PruneCheckpoint, error) {
	chain, err := LoadCheckpointChain(root)
	if err != nil {
		return PruneCheckpoint{}, err
	}
	return chain[len(chain)-1].Checkpoint, nil
}

func parseCheckpointFilename(name string) (uint64, bool) {
	if !strings.HasPrefix(name, "checkpoint-") || !strings.HasSuffix(name, ".json") {
		return 0, false
	}
	digits := strings.TrimSuffix(strings.TrimPrefix(name, "checkpoint-"), ".json")
	if len(digits) != 20 {
		return 0, false
	}
	value, err := strconv.ParseUint(digits, 10, 64)
	return value, err == nil && checkpointFilename(value) == name
}

// VerifyRetainedWAL proves that the remaining sealed inventory starts at the
// checkpoint anchor and has one continuous hash-chain order. It does not
// mutate the filesystem and rejects a missing segment or a chain-root hole.
func VerifyRetainedWAL(root string, checkpoint *PruneCheckpoint, maxObjects uint64, maxBytes ...uint64) ([]wal.VerifiedSegment, error) {
	segments, err := InventoryWALSegments(root, maxObjects, maxBytes...)
	if err != nil {
		return nil, err
	}
	verified := make([]wal.VerifiedSegment, 0, len(segments))
	for _, artifact := range segments {
		segment, verifyErr := wal.VerifySealedSegment(filepath.Join(root, filepath.FromSlash(artifact.TrustedPath)))
		if verifyErr != nil {
			return nil, fmt.Errorf("%w: retained WAL verification: %v", ErrPruneIntegrity, verifyErr)
		}
		verified = append(verified, segment)
	}
	sort.Slice(verified, func(i, j int) bool { return verified[i].StartSequence < verified[j].StartSequence })
	expected := uint64(1)
	var previousRoot [32]byte
	if checkpoint != nil {
		if err := checkpoint.Validate(); err != nil {
			return nil, fmt.Errorf("%w: retained WAL checkpoint: %v", ErrPruneIntegrity, err)
		}
		if checkpoint.EndSequence == ^uint64(0) {
			if len(verified) != 0 {
				return nil, fmt.Errorf("%w: retained WAL exists after terminal checkpoint", ErrPruneIntegrity)
			}
			return verified, nil
		}
		expected = checkpoint.EndSequence + 1
		previousRoot = checkpoint.RetainedChainRoot
	}
	for _, segment := range verified {
		if segment.StartSequence != expected || segment.ChainStart != previousRoot {
			return nil, fmt.Errorf("%w: retained WAL has a sequence or chain-root hole", ErrPruneIntegrity)
		}
		if segment.LastSequence == ^uint64(0) {
			expected = 0
		} else {
			expected = segment.LastSequence + 1
		}
		previousRoot = segment.ChainRoot
	}
	return verified, nil
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil && runtime.GOOS != "windows" {
		return err
	}
	return nil
}
