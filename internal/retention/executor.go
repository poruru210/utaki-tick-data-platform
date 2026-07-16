package retention

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"tick-data-platform/internal/archive"
	"tick-data-platform/internal/wal"
)

type PruneRoots struct {
	WALRoot          string
	RawOutboxRoot    string
	ReplayOutboxRoot string
	CacheRoot        string
}

func (r PruneRoots) rootFor(kind string) (string, error) {
	var root string
	switch kind {
	case ArtifactWALSegment:
		root = r.WALRoot
	case ArtifactRawOutbox:
		root = r.RawOutboxRoot
	case ArtifactReplayOutbox:
		root = r.ReplayOutboxRoot
	case ArtifactCache:
		root = r.CacheRoot
	default:
		return "", fmt.Errorf("unknown prune artifact kind %q", kind)
	}
	if root == "" {
		return "", fmt.Errorf("prune root for %s is not configured", kind)
	}
	return root, nil
}

type PruneFaultPoint string

const (
	FaultBeforeCheckpointPublish PruneFaultPoint = "before_checkpoint_publish"
	FaultAfterCheckpointPublish  PruneFaultPoint = "after_checkpoint_publish"
	FaultBeforeTrashRename       PruneFaultPoint = "before_trash_rename"
	FaultAfterTrashRename        PruneFaultPoint = "after_trash_rename"
	FaultBeforeUnlink            PruneFaultPoint = "before_unlink"
	FaultAfterUnlink             PruneFaultPoint = "after_unlink"
	FaultCheckpointDirectorySync PruneFaultPoint = "checkpoint_directory_sync"
	FaultTrashDirectorySync      PruneFaultPoint = "trash_directory_sync"
	FaultOutboxDirectorySync     PruneFaultPoint = "outbox_directory_sync"
)

type PruneFaultInjector interface {
	Inject(point PruneFaultPoint, artifactID string) error
}

type PruneExecutor struct {
	roots PruneRoots
	fault PruneFaultInjector
}

func NewPruneExecutor(roots PruneRoots, fault PruneFaultInjector) (*PruneExecutor, error) {
	if roots.WALRoot == "" && roots.RawOutboxRoot == "" && roots.ReplayOutboxRoot == "" && roots.CacheRoot == "" {
		return nil, fmt.Errorf("prune roots are empty")
	}
	return &PruneExecutor{roots: roots, fault: fault}, nil
}

type PruneExecutionReport struct {
	PlanDigest [32]byte
	Completed  []string
}

// Execute validates the sealed plan and candidate facts again immediately
// before every local mutation. It never accepts a caller-selected absolute
// path, remote key, or unverified candidate.
func (e *PruneExecutor) Execute(ctx context.Context, plan PrunePlan, candidates []CandidateFact) (PruneExecutionReport, error) {
	report := PruneExecutionReport{PlanDigest: plan.PlanDigest}
	canonical, err := plan.CanonicalJSON()
	if err != nil || PrunePlanDigest(canonical) != plan.PlanDigest {
		return report, fmt.Errorf("%w: prune plan digest is invalid", ErrPruneIntegrity)
	}
	for _, action := range plan.Actions {
		if action.ArtifactKind != ArtifactWALSegment && action.ArtifactKind != ArtifactRawOutbox {
			return report, fmt.Errorf("%w: artifact kind policy is not implemented for %s", ErrPruneIntegrity, action.ArtifactKind)
		}
	}
	byID := make(map[string]CandidateFact, len(candidates))
	for _, candidate := range candidates {
		id, idErr := candidate.Artifact.StableID()
		if idErr != nil {
			return report, fmt.Errorf("%w: candidate identity: %v", ErrPruneIntegrity, idErr)
		}
		if _, exists := byID[id]; exists {
			return report, fmt.Errorf("%w: duplicate candidate identity", ErrPruneIntegrity)
		}
		byID[id] = candidate
	}
	walActions := make([]PruneAction, 0)
	otherActions := make([]PruneAction, 0)
	for _, action := range plan.Actions {
		if action.ArtifactKind == ArtifactWALSegment {
			walActions = append(walActions, action)
		} else {
			otherActions = append(otherActions, action)
		}
	}
	sort.Slice(walActions, func(i, j int) bool {
		if walActions[i].WALStartSequence != walActions[j].WALStartSequence {
			return walActions[i].WALStartSequence < walActions[j].WALStartSequence
		}
		return walActions[i].ArtifactID < walActions[j].ArtifactID
	})
	for _, action := range walActions {
		if err := contextErr(ctx); err != nil {
			return report, err
		}
		candidate, err := e.validateAction(action, byID)
		if err != nil {
			return report, err
		}
		if err := e.executeWAL(ctx, plan, action, candidate); err != nil {
			return report, err
		}
		report.Completed = append(report.Completed, action.ArtifactID)
	}
	for _, action := range otherActions {
		if err := contextErr(ctx); err != nil {
			return report, err
		}
		completed, err := e.completedOtherAction(action)
		if err != nil {
			return report, err
		}
		if completed {
			report.Completed = append(report.Completed, action.ArtifactID)
			continue
		}
		candidate, err := e.validateAction(action, byID)
		if err != nil {
			return report, err
		}
		if err := e.executeOther(ctx, plan, action, candidate); err != nil {
			return report, err
		}
		report.Completed = append(report.Completed, action.ArtifactID)
	}
	return report, nil
}

func contextErr(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return fmt.Errorf("%w: %v", ErrPruneAvailability, ctx.Err())
	default:
		return nil
	}
}

func (e *PruneExecutor) validateAction(action PruneAction, byID map[string]CandidateFact) (CandidateFact, error) {
	candidate, ok := byID[action.ArtifactID]
	if !ok {
		return CandidateFact{}, fmt.Errorf("%w: plan action has no candidate fact", ErrPruneIntegrity)
	}
	if candidate.RecoveryRequired || !candidate.FreshRemote || !candidate.CoverageVerified || candidate.Proof == nil {
		return CandidateFact{}, fmt.Errorf("%w: candidate is no longer deletion eligible", ErrPruneIntegrity)
	}
	canonicalAction, err := makePruneAction(candidate)
	if err != nil || canonicalAction != action {
		return CandidateFact{}, fmt.Errorf("%w: plan action identity changed", ErrPruneIntegrity)
	}
	if err := candidate.Proof.Validate(); err != nil || !proofMatchesArtifact(candidate.Proof, candidate.Artifact) {
		return CandidateFact{}, fmt.Errorf("%w: candidate proof changed", ErrPruneIntegrity)
	}
	root, err := e.roots.rootFor(action.ArtifactKind)
	if err != nil {
		return CandidateFact{}, fmt.Errorf("%w: %v", ErrPruneIntegrity, err)
	}
	path, err := trustedArtifactPath(root, candidate.Artifact.TrustedPath)
	if err != nil {
		return CandidateFact{}, fmt.Errorf("%w: trusted artifact path: %v", ErrPruneIntegrity, err)
	}
	if err := verifyLocalArtifact(path, candidate.Artifact); err != nil {
		return CandidateFact{}, err
	}
	return candidate, nil
}

func trustedArtifactPath(root, relative string) (string, error) {
	if err := validateRelativePath(relative); err != nil {
		return "", err
	}
	absoluteRoot, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	rootInfo, err := os.Lstat(absoluteRoot)
	if err != nil || rootInfo.Mode()&os.ModeSymlink != 0 || !rootInfo.IsDir() {
		return "", fmt.Errorf("configured root is not a trusted directory")
	}
	path := filepath.Join(absoluteRoot, filepath.FromSlash(relative))
	rel, err := filepath.Rel(absoluteRoot, path)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("artifact path escapes configured root")
	}
	current := absoluteRoot
	parts := strings.Split(filepath.FromSlash(relative), string(filepath.Separator))
	for index, part := range parts {
		if part == "" || part == "." || part == ".." {
			return "", fmt.Errorf("artifact path contains forbidden component")
		}
		current = filepath.Join(current, part)
		info, statErr := os.Lstat(current)
		if statErr != nil {
			return "", statErr
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return "", fmt.Errorf("artifact path contains symlink")
		}
		if index < len(parts)-1 && !info.IsDir() {
			return "", fmt.Errorf("artifact parent is not a directory")
		}
	}
	return path, nil
}

func verifyLocalArtifact(path string, artifact LocalArtifact) error {
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("%w: local artifact disappeared: %v", ErrPruneIntegrity, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return fmt.Errorf("%w: local artifact is not a regular file", ErrPruneIntegrity)
	}
	if info.Size() < 0 || uint64(info.Size()) != artifact.Bytes {
		return fmt.Errorf("%w: local artifact size changed", ErrPruneIntegrity)
	}
	digest, err := hashFile(path)
	if err != nil || digest != artifact.ContentSHA256 {
		return fmt.Errorf("%w: local artifact digest changed", ErrPruneIntegrity)
	}
	if artifact.Kind == ArtifactRawOutbox && filepath.ToSlash(artifact.TrustedPath) != archive.RawWALObjectKey(artifact.ContentSHA256) {
		return fmt.Errorf("%w: raw outbox path is not content-addressed", ErrPruneIntegrity)
	}
	if artifact.Kind == ArtifactWALSegment || artifact.Kind == ArtifactRawOutbox {
		segment, verifyErr := wal.VerifySealedSegment(path)
		if verifyErr != nil || artifact.WALRange == nil || segment.ObjectSHA256 != artifact.ContentSHA256 || uint64(segment.FileBytes) != artifact.Bytes || segment.StartSequence != artifact.WALRange.StartSequence || segment.LastSequence != artifact.WALRange.EndSequence || segment.ChainStart != artifact.WALRange.StartChainRoot || segment.ChainRoot != artifact.WALRange.EndChainRoot {
			return fmt.Errorf("%w: local WAL metadata changed", ErrPruneIntegrity)
		}
	}
	return nil
}

func (e *PruneExecutor) executeWAL(ctx context.Context, plan PrunePlan, action PruneAction, candidate CandidateFact) error {
	root := e.roots.WALRoot
	chain, err := LoadCheckpointChain(root)
	if err != nil && !errors.Is(err, ErrCheckpointAbsent) {
		return err
	}
	var previousDigest [32]byte
	expectedStart := uint64(1)
	var expectedChainRoot [32]byte
	if len(chain) != 0 {
		previous := chain[len(chain)-1]
		previousDigest = previous.Digest
		if previous.Checkpoint.EndSequence == ^uint64(0) {
			return fmt.Errorf("%w: WAL is already terminally pruned", ErrPruneIntegrity)
		}
		expectedStart = previous.Checkpoint.EndSequence + 1
		expectedChainRoot = previous.Checkpoint.RetainedChainRoot
	}
	if plan.WALExpectedStart != expectedStart || candidate.Artifact.WALRange == nil || action.WALStartSequence != expectedStart || candidate.Artifact.WALRange.StartChainRoot != expectedChainRoot {
		return fmt.Errorf("%w: prune plan WAL prefix does not match checkpoint anchor", ErrPruneIntegrity)
	}
	if filepath.ToSlash(candidate.Artifact.TrustedPath) != "sealed/"+SegmentNameFromRange(action.WALStartSequence, action.WALEndSequence) {
		return fmt.Errorf("%w: WAL path is not the canonical sealed segment path", ErrPruneIntegrity)
	}
	checkpoint := PruneCheckpoint{
		CheckpointVersion:        PruneCheckpointVersion,
		EndSequence:              action.WALEndSequence,
		RetainedChainRoot:        candidate.Artifact.WALRange.EndChainRoot,
		LastSegmentSHA256:        candidate.Artifact.ContentSHA256,
		RetentionProofDigest:     mustProofDigest(candidate.Proof),
		RetentionProof:           *candidate.Proof,
		PreviousCheckpointDigest: previousDigest,
	}
	if checkpoint.EndSequence < candidate.Artifact.WALRange.StartSequence {
		return fmt.Errorf("%w: WAL checkpoint range is invalid", ErrPruneIntegrity)
	}
	if err := e.inject(FaultBeforeCheckpointPublish, action.ArtifactID); err != nil {
		return err
	}
	if err := PublishCheckpoint(root, checkpoint); err != nil {
		return err
	}
	if err := e.inject(FaultCheckpointDirectorySync, action.ArtifactID); err != nil {
		return err
	}
	if err := e.inject(FaultAfterCheckpointPublish, action.ArtifactID); err != nil {
		return err
	}
	path, err := trustedArtifactPath(root, candidate.Artifact.TrustedPath)
	if err != nil {
		return fmt.Errorf("%w: resolve WAL path after checkpoint: %v", ErrPruneIntegrity, err)
	}
	trash, err := trashPathFor(path, root)
	if err != nil {
		return fmt.Errorf("%w: resolve WAL trash: %v", ErrPruneIntegrity, err)
	}
	trashDirectory, err := trustedPruneDirectory(root, "trash", true)
	if err != nil {
		return fmt.Errorf("%w: create WAL trash: %v", ErrPruneAvailability, err)
	}
	if _, statErr := os.Lstat(trash); errors.Is(statErr, os.ErrNotExist) {
		if err := e.inject(FaultBeforeTrashRename, action.ArtifactID); err != nil {
			return err
		}
		if err := os.Rename(path, trash); err != nil {
			return fmt.Errorf("%w: move WAL to trash: %v", ErrPruneAvailability, err)
		}
		if err := e.inject(FaultAfterTrashRename, action.ArtifactID); err != nil {
			return err
		}
		if err := e.inject(FaultTrashDirectorySync, action.ArtifactID); err != nil {
			return err
		}
		if err := syncDirectory(trashDirectory); err != nil {
			return fmt.Errorf("%w: sync WAL trash directory: %v", ErrPruneAvailability, err)
		}
	} else if statErr != nil {
		return fmt.Errorf("%w: inspect WAL trash: %v", ErrPruneAvailability, statErr)
	} else {
		if err := verifyTrashWAL(trash, candidate.Artifact); err != nil {
			return err
		}
		if _, sourceErr := os.Lstat(path); sourceErr == nil {
			return fmt.Errorf("%w: WAL exists both at source and trash", ErrPruneIntegrity)
		} else if !errors.Is(sourceErr, os.ErrNotExist) {
			return fmt.Errorf("%w: inspect WAL source: %v", ErrPruneAvailability, sourceErr)
		}
	}
	if err := e.inject(FaultBeforeUnlink, action.ArtifactID); err != nil {
		return err
	}
	if err := os.Remove(trash); err != nil {
		return fmt.Errorf("%w: remove WAL trash: %v", ErrPruneAvailability, err)
	}
	if err := e.inject(FaultAfterUnlink, action.ArtifactID); err != nil {
		return err
	}
	if err := e.inject(FaultTrashDirectorySync, action.ArtifactID); err != nil {
		return err
	}
	if err := syncDirectory(trashDirectory); err != nil {
		return fmt.Errorf("%w: sync WAL trash directory after unlink: %v", ErrPruneAvailability, err)
	}
	return nil
}

func mustProofDigest(proof *RetentionProof) [32]byte {
	digest, _ := proof.Digest()
	return digest
}

func (e *PruneExecutor) completedOtherAction(action PruneAction) (bool, error) {
	root, err := e.roots.rootFor(action.ArtifactKind)
	if err != nil {
		return false, fmt.Errorf("%w: %v", ErrPruneIntegrity, err)
	}
	completion, exists, err := loadPruneCompletion(root, action.ArtifactID)
	if err != nil || !exists {
		return false, err
	}
	if err := completionMatchesAction(completion, action); err != nil {
		return false, fmt.Errorf("%w: %v", ErrPruneIntegrity, err)
	}
	if path, err := trustedArtifactPath(root, action.TrustedPath); err == nil {
		artifact, artifactErr := completionArtifact(completion)
		if artifactErr != nil {
			return false, fmt.Errorf("%w: completed artifact identity: %v", ErrPruneIntegrity, artifactErr)
		}
		if verifyErr := verifyLocalArtifact(path, artifact); verifyErr != nil {
			return false, fmt.Errorf("%w: completed artifact changed: %v", ErrPruneIntegrity, verifyErr)
		}
		// The completion may have been published immediately before a crash or
		// injected fault at unlink. Leave the source for executeOther to retry;
		// an exact byte/proof match makes that retry idempotent.
		return false, nil
	} else if errors.Is(err, os.ErrNotExist) {
		return true, nil
	} else {
		return false, fmt.Errorf("%w: inspect completed artifact: %v", ErrPruneIntegrity, err)
	}
}

func (e *PruneExecutor) executeOther(ctx context.Context, plan PrunePlan, action PruneAction, candidate CandidateFact) error {
	root, err := e.roots.rootFor(action.ArtifactKind)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrPruneIntegrity, err)
	}
	path, err := trustedArtifactPath(root, candidate.Artifact.TrustedPath)
	if err != nil {
		return fmt.Errorf("%w: resolve outbox path: %v", ErrPruneIntegrity, err)
	}
	proofDigest, err := candidate.Proof.Digest()
	if err != nil {
		return fmt.Errorf("%w: outbox proof digest: %v", ErrPruneIntegrity, err)
	}
	completion := PruneCompletion{
		CompletionVersion: PruneCompletionVersion,
		ArtifactID:        action.ArtifactID,
		ArtifactKind:      action.ArtifactKind,
		TrustedPath:       action.TrustedPath,
		Bytes:             action.Bytes,
		ContentSHA256:     candidate.Artifact.ContentSHA256,
		ProofDigest:       proofDigest,
		RetentionProof:    *candidate.Proof,
		PlanDigest:        plan.PlanDigest,
	}
	if err := publishPruneCompletion(root, completion); err != nil {
		return err
	}
	if err := e.inject(FaultBeforeUnlink, action.ArtifactID); err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("%w: remove non-WAL artifact: %v", ErrPruneAvailability, err)
	}
	if err := e.inject(FaultAfterUnlink, action.ArtifactID); err != nil {
		return err
	}
	if err := e.inject(FaultOutboxDirectorySync, action.ArtifactID); err != nil {
		return err
	}
	if err := syncDirectory(filepath.Dir(path)); err != nil {
		return fmt.Errorf("%w: sync non-WAL parent directory: %v", ErrPruneAvailability, err)
	}
	return nil
}

func (e *PruneExecutor) inject(point PruneFaultPoint, artifactID string) error {
	if e.fault == nil {
		return nil
	}
	if err := e.fault.Inject(point, artifactID); err != nil {
		return fmt.Errorf("%w: fault at %s: %v", ErrPruneAvailability, point, err)
	}
	return nil
}

func trashPathFor(source, root string) (string, error) {
	absoluteRoot, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	sealedRoot := filepath.Join(absoluteRoot, "sealed")
	relative, err := filepath.Rel(sealedRoot, source)
	if err != nil || relative == "." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) || strings.Contains(relative, string(filepath.Separator)) {
		return "", fmt.Errorf("WAL source is not a direct sealed segment")
	}
	return filepath.Join(absoluteRoot, "trash", relative), nil
}

func verifyTrashWAL(path string, artifact LocalArtifact) error {
	segment, err := wal.VerifySealedSegment(path)
	if err != nil || artifact.WALRange == nil || segment.ObjectSHA256 != artifact.ContentSHA256 || uint64(segment.FileBytes) != artifact.Bytes || segment.StartSequence != artifact.WALRange.StartSequence || segment.LastSequence != artifact.WALRange.EndSequence || segment.ChainStart != artifact.WALRange.StartChainRoot || segment.ChainRoot != artifact.WALRange.EndChainRoot {
		return fmt.Errorf("%w: WAL trash does not match action", ErrPruneIntegrity)
	}
	return nil
}

// RecoverPrune reconciles checkpointed WAL deletes after a crash. It finishes
// only a checkpoint-backed source/trash transition, rejects uncheckpointed
// trash or holes, and leaves the retained inventory anchored and continuous.
func RecoverPrune(root string, maxObjects uint64, maxBytes ...uint64) error {
	chain, err := LoadCheckpointChain(root)
	if errors.Is(err, ErrCheckpointAbsent) {
		if trashErr := rejectUnexpectedTrash(root); trashErr != nil {
			return trashErr
		}
		if _, statErr := trustedPruneDirectory(root, "sealed", false); errors.Is(statErr, os.ErrNotExist) {
			return nil
		} else if statErr != nil {
			return fmt.Errorf("%w: inspect sealed WAL directory: %v", ErrPruneAvailability, statErr)
		}
		if _, verifyErr := VerifyRetainedWAL(root, nil, maxObjects, maxBytes...); verifyErr != nil {
			return verifyErr
		}
		return nil
	}
	if err != nil {
		return err
	}
	if err := recoverCheckpointedSegments(root, chain); err != nil {
		return err
	}
	if err := rejectUnexpectedTrash(root); err != nil {
		return err
	}
	if _, err := VerifyRetainedWAL(root, &chain[len(chain)-1].Checkpoint, maxObjects, maxBytes...); err != nil {
		return err
	}
	return nil
}

func recoverCheckpointedSegments(root string, chain []CheckpointRecord) error {
	sealedDirectory, err := trustedPruneDirectory(root, "sealed", false)
	if err != nil {
		return fmt.Errorf("%w: inspect sealed WAL directory: %v", ErrPruneAvailability, err)
	}
	trashDirectory, err := trustedPruneDirectory(root, "trash", true)
	if err != nil {
		return fmt.Errorf("%w: inspect WAL trash directory: %v", ErrPruneIntegrity, err)
	}
	var previousEnd uint64
	for index, record := range chain {
		start := uint64(1)
		if index > 0 {
			previousEnd = chain[index-1].Checkpoint.EndSequence
			if previousEnd == ^uint64(0) {
				return fmt.Errorf("%w: checkpoint follows terminal sequence", ErrPruneIntegrity)
			}
			start = previousEnd + 1
		}
		end := record.Checkpoint.EndSequence
		name := SegmentNameFromRange(start, end)
		source := filepath.Join(sealedDirectory, name)
		trash := filepath.Join(trashDirectory, name)
		sourceExists := false
		if info, err := os.Lstat(source); err == nil {
			if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
				return fmt.Errorf("%w: checkpointed source is not a regular file", ErrPruneIntegrity)
			}
			sourceExists = true
		} else if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("%w: inspect checkpointed source: %v", ErrPruneAvailability, err)
		}
		trashExists := false
		if info, err := os.Lstat(trash); err == nil {
			if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
				return fmt.Errorf("%w: checkpointed trash is not a regular file", ErrPruneIntegrity)
			}
			trashExists = true
		} else if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("%w: inspect checkpointed trash: %v", ErrPruneAvailability, err)
		}
		if sourceExists && trashExists {
			return fmt.Errorf("%w: checkpointed WAL exists at source and trash", ErrPruneIntegrity)
		}
		if sourceExists {
			segment, verifyErr := wal.VerifySealedSegment(source)
			if verifyErr != nil || segment.LastSequence != end || segment.ObjectSHA256 != record.Checkpoint.LastSegmentSHA256 || segment.ChainRoot != record.Checkpoint.RetainedChainRoot {
				return fmt.Errorf("%w: checkpointed source does not match checkpoint", ErrPruneIntegrity)
			}
			if err := os.Rename(source, trash); err != nil {
				return fmt.Errorf("%w: recover WAL rename: %v", ErrPruneAvailability, err)
			}
			trashExists = true
		}
		if trashExists {
			segment, verifyErr := wal.VerifySealedSegment(trash)
			if verifyErr != nil || segment.LastSequence != end || segment.ObjectSHA256 != record.Checkpoint.LastSegmentSHA256 || segment.ChainRoot != record.Checkpoint.RetainedChainRoot {
				return fmt.Errorf("%w: checkpointed trash does not match checkpoint", ErrPruneIntegrity)
			}
			if err := os.Remove(trash); err != nil {
				return fmt.Errorf("%w: recover WAL unlink: %v", ErrPruneAvailability, err)
			}
		}
	}
	return nil
}

func rejectUnexpectedTrash(root string) error {
	directory, err := trustedPruneDirectory(root, "trash", false)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("%w: inspect WAL trash directory: %v", ErrPruneIntegrity, err)
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		return fmt.Errorf("%w: read WAL trash: %v", ErrPruneAvailability, err)
	}
	for _, entry := range entries {
		if entry.Type()&os.ModeSymlink != 0 || entry.IsDir() || !entry.Type().IsRegular() {
			return fmt.Errorf("%w: unexpected WAL trash entry %s", ErrPruneIntegrity, entry.Name())
		}
		return fmt.Errorf("%w: uncheckpointed WAL trash entry %s", ErrPruneIntegrity, entry.Name())
	}
	return nil
}

// SegmentNameFromRange is exposed for operator/status code and tests that
// need to bind the canonical WAL filename to a verified sequence range.
func SegmentNameFromRange(start, end uint64) string {
	return "segment-" + zeroPadUint(start) + "-" + zeroPadUint(end) + ".wal"
}

func zeroPadUint(value uint64) string {
	return fmt.Sprintf("%020d", value)
}
