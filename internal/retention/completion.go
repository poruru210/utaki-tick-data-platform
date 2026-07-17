package retention

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"tick-data-platform/internal/archive"
	"tick-data-platform/internal/protocol"
	"tick-data-platform/internal/r2"
)

const (
	PruneCompletionVersion = "prune-completion-v1"
	pruneCompletionDomain  = "tick-data-platform/prune-completion/v1\x00"
)

// PruneCompletion is a durable authorization record for a non-WAL object
// delete. It is published before unlink, so a missing file is accepted only
// when this exact proof- and plan-bound record exists.
type PruneCompletion struct {
	CompletionVersion string
	ArtifactID        string
	ArtifactKind      string
	TrustedPath       string
	Bytes             uint64
	ContentSHA256     [32]byte
	ProofDigest       [32]byte
	RetentionProof    RetentionProof
	PlanDigest        [32]byte
}

func (c PruneCompletion) Value() map[string]any {
	return map[string]any{
		"artifact_id":        c.ArtifactID,
		"artifact_kind":      c.ArtifactKind,
		"bytes":              c.Bytes,
		"completion_version": c.CompletionVersion,
		"content_sha256":     protocol.EncodeHashHex(c.ContentSHA256),
		"plan_digest":        protocol.EncodeHashHex(c.PlanDigest),
		"proof_digest":       protocol.EncodeHashHex(c.ProofDigest),
		"retention_proof":    c.RetentionProof.Value(),
		"trusted_path":       c.TrustedPath,
	}
}

func (c PruneCompletion) Validate() error {
	if c.CompletionVersion != PruneCompletionVersion || c.ArtifactID == "" {
		return fmt.Errorf("prune completion identity is invalid")
	}
	if c.ArtifactKind != ArtifactRawOutbox && c.ArtifactKind != ArtifactReplayOutbox && c.ArtifactKind != ArtifactCache {
		return fmt.Errorf("prune completion kind is invalid")
	}
	if !strings.HasPrefix(c.ArtifactID, "artifact-") || len(c.ArtifactID) != len("artifact-")+64 {
		return fmt.Errorf("prune completion artifact ID is invalid")
	}
	if _, err := protocol.ParseHashHex(strings.TrimPrefix(c.ArtifactID, "artifact-")); err != nil {
		return fmt.Errorf("prune completion artifact ID is invalid")
	}
	if err := validateRelativePath(c.TrustedPath); err != nil {
		return err
	}
	if c.Bytes == 0 || c.ContentSHA256 == ([32]byte{}) || c.ProofDigest == ([32]byte{}) || c.PlanDigest == ([32]byte{}) {
		return fmt.Errorf("prune completion proof identity is incomplete")
	}
	if err := c.RetentionProof.Validate(); err != nil {
		return fmt.Errorf("prune completion retention proof: %w", err)
	}
	if c.RetentionProof.ArtifactKind != c.ArtifactKind || c.RetentionProof.TrustedRelativePath != c.TrustedPath || c.RetentionProof.Bytes != c.Bytes || c.RetentionProof.ContentSHA256 != c.ContentSHA256 {
		return fmt.Errorf("prune completion retention proof is not bound to artifact")
	}
	proofDigest, err := c.RetentionProof.Digest()
	if err != nil || proofDigest != c.ProofDigest {
		return fmt.Errorf("prune completion retention proof digest is not bound")
	}
	if c.ArtifactKind == ArtifactRawOutbox && (c.RetentionProof.WALRange == nil || c.TrustedPath != archive.RawWALObjectKey(c.ContentSHA256)) {
		return fmt.Errorf("raw outbox completion is not bound to a verified WAL")
	}
	return nil
}

func (c PruneCompletion) CanonicalJSON() ([]byte, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return protocol.CanonicalJSON(c.Value())
}

func PruneCompletionDigest(canonical []byte) [32]byte {
	return sha256.Sum256(append([]byte(pruneCompletionDomain), canonical...))
}

var pruneCompletionKeys = map[string]bool{
	"artifact_id": true, "artifact_kind": true, "bytes": true, "completion_version": true,
	"content_sha256": true, "plan_digest": true, "proof_digest": true, "retention_proof": true, "trusted_path": true,
}

func DecodePruneCompletion(data []byte) (PruneCompletion, error) {
	object, err := decodeObject(data, pruneCompletionKeys)
	if err != nil {
		return PruneCompletion{}, err
	}
	var result PruneCompletion
	if result.ArtifactID, err = stringField(object, "artifact_id"); err != nil {
		return PruneCompletion{}, err
	}
	if result.ArtifactKind, err = stringField(object, "artifact_kind"); err != nil {
		return PruneCompletion{}, err
	}
	if result.Bytes, err = uintField(object, "bytes"); err != nil {
		return PruneCompletion{}, err
	}
	if result.CompletionVersion, err = stringField(object, "completion_version"); err != nil {
		return PruneCompletion{}, err
	}
	if result.ContentSHA256, err = hashField(object, "content_sha256", false); err != nil {
		return PruneCompletion{}, err
	}
	if result.PlanDigest, err = hashField(object, "plan_digest", false); err != nil {
		return PruneCompletion{}, err
	}
	if result.ProofDigest, err = hashField(object, "proof_digest", false); err != nil {
		return PruneCompletion{}, err
	}
	proofObject, ok := object["retention_proof"].(map[string]any)
	if !ok {
		return PruneCompletion{}, fmt.Errorf("retention_proof must be an object")
	}
	proofCanonical, err := protocol.CanonicalJSON(proofObject)
	if err != nil {
		return PruneCompletion{}, err
	}
	if result.RetentionProof, err = DecodeRetentionProof(proofCanonical); err != nil {
		return PruneCompletion{}, err
	}
	if result.TrustedPath, err = stringField(object, "trusted_path"); err != nil {
		return PruneCompletion{}, err
	}
	if err := result.Validate(); err != nil {
		return PruneCompletion{}, err
	}
	canonical, err := result.CanonicalJSON()
	if err != nil || !bytes.Equal(canonical, data) {
		return PruneCompletion{}, fmt.Errorf("prune completion bytes are not canonical")
	}
	return result, nil
}

func pruneCompletionDirectory(root string) string {
	return filepath.Join(root, "prune-completions")
}

func pruneCompletionPath(directory, artifactID string) (string, error) {
	if artifactID == "" || filepath.Base(artifactID) != artifactID || strings.ContainsAny(artifactID, `/\\`) {
		return "", fmt.Errorf("prune completion artifact ID is not a filename")
	}
	return filepath.Join(directory, "completion-"+artifactID+".json"), nil
}

func ensurePruneCompletionDirectory(root string) (string, error) {
	absoluteRoot, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	info, err := os.Lstat(absoluteRoot)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return "", fmt.Errorf("prune completion root is not trusted")
	}
	directory := filepath.Join(absoluteRoot, "prune-completions")
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return "", err
	}
	info, err = os.Lstat(directory)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return "", fmt.Errorf("prune completion directory is not trusted")
	}
	return directory, nil
}

func publishPruneCompletion(root string, completion PruneCompletion) error {
	canonical, err := completion.CanonicalJSON()
	if err != nil {
		return fmt.Errorf("%w: completion validation: %v", ErrPruneIntegrity, err)
	}
	directory, err := ensurePruneCompletionDirectory(root)
	if err != nil {
		return fmt.Errorf("%w: completion directory: %v", ErrPruneAvailability, err)
	}
	destination, err := pruneCompletionPath(directory, completion.ArtifactID)
	if err != nil {
		return fmt.Errorf("%w: completion path: %v", ErrPruneIntegrity, err)
	}
	if existing, readErr := os.ReadFile(destination); readErr == nil {
		if bytes.Equal(existing, canonical) {
			return nil
		}
		prior, decodeErr := DecodePruneCompletion(existing)
		if decodeErr == nil && samePruneCompletionExecution(prior, completion) {
			// Preserve the first durable plan digest as historical evidence. A
			// retry may use a rebuilt plan after the durable clock advances.
			return nil
		}
		return fmt.Errorf("%w: completion differs for artifact %s", ErrPruneIntegrity, completion.ArtifactID)
	} else if !errors.Is(readErr, os.ErrNotExist) {
		return fmt.Errorf("%w: inspect completion: %v", ErrPruneAvailability, readErr)
	}
	temporary, err := os.CreateTemp(directory, ".prune-completion-*.tmp")
	if err != nil {
		return fmt.Errorf("%w: create completion temporary: %v", ErrPruneAvailability, err)
	}
	temporaryName := temporary.Name()
	defer func() { _ = os.Remove(temporaryName) }()
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("%w: completion permissions: %v", ErrPruneAvailability, err)
	}
	if _, err := temporary.Write(canonical); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("%w: write completion: %v", ErrPruneAvailability, err)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("%w: sync completion: %v", ErrPruneAvailability, err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("%w: close completion: %v", ErrPruneAvailability, err)
	}
	if err := os.Link(temporaryName, destination); err != nil {
		if errors.Is(err, os.ErrExist) {
			existing, readErr := os.ReadFile(destination)
			if readErr == nil && bytes.Equal(existing, canonical) {
				return nil
			}
			if readErr == nil {
				prior, decodeErr := DecodePruneCompletion(existing)
				if decodeErr == nil && samePruneCompletionExecution(prior, completion) {
					return nil
				}
			}
			return fmt.Errorf("%w: completion publish raced with different bytes", ErrPruneIntegrity)
		}
		return fmt.Errorf("%w: no-clobber completion publish: %v", ErrPruneAvailability, err)
	}
	if err := syncDirectory(directory); err != nil {
		return fmt.Errorf("%w: sync completion directory: %v", ErrPruneAvailability, err)
	}
	if err := syncDirectory(filepath.Dir(directory)); err != nil {
		return fmt.Errorf("%w: sync completion root directory: %v", ErrPruneAvailability, err)
	}
	return nil
}

func samePruneCompletionExecution(left, right PruneCompletion) bool {
	return left.CompletionVersion == right.CompletionVersion && left.ArtifactID == right.ArtifactID && left.ArtifactKind == right.ArtifactKind && left.TrustedPath == right.TrustedPath && left.Bytes == right.Bytes && left.ContentSHA256 == right.ContentSHA256 && left.ProofDigest == right.ProofDigest
}

func loadPruneCompletion(root, artifactID string) (PruneCompletion, bool, error) {
	directory, err := ensurePruneCompletionDirectory(root)
	if err != nil {
		return PruneCompletion{}, false, fmt.Errorf("%w: completion directory: %v", ErrPruneAvailability, err)
	}
	path, err := pruneCompletionPath(directory, artifactID)
	if err != nil {
		return PruneCompletion{}, false, fmt.Errorf("%w: completion path: %v", ErrPruneIntegrity, err)
	}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return PruneCompletion{}, false, nil
	}
	if err != nil {
		return PruneCompletion{}, false, fmt.Errorf("%w: read completion: %v", ErrPruneAvailability, err)
	}
	completion, err := DecodePruneCompletion(data)
	if err != nil || completion.ArtifactID != artifactID {
		return PruneCompletion{}, false, fmt.Errorf("%w: completion identity is invalid", ErrPruneIntegrity)
	}
	return completion, true, nil
}

func completionArtifact(completion PruneCompletion) (LocalArtifact, error) {
	if err := completion.Validate(); err != nil {
		return LocalArtifact{}, err
	}
	artifact := LocalArtifact{
		Kind:          completion.ArtifactKind,
		TrustedPath:   completion.TrustedPath,
		Bytes:         completion.Bytes,
		ContentSHA256: completion.ContentSHA256,
		WALRange:      cloneWALRange(completion.RetentionProof.WALRange),
		Replay:        completion.RetentionProof.Replay,
	}
	artifactID, err := artifact.StableID()
	if err != nil || artifactID != completion.ArtifactID {
		return LocalArtifact{}, fmt.Errorf("prune completion artifact identity is invalid")
	}
	if !proofMatchesArtifact(&completion.RetentionProof, artifact) {
		return LocalArtifact{}, fmt.Errorf("prune completion retention proof identity is invalid")
	}
	return artifact, nil
}

// ValidateRawCompletionScope binds a recovered raw-outbox proof to the
// current trusted immutable layout before it can suppress fresh observation.
// The remote object and covering manifest keys carry the scope and date
// namespace; a completion from another outbox/scope therefore cannot become
// a recovery candidate by local path identity alone.
func ValidateRawCompletionScope(candidate CandidateFact, layout r2.Layout, date string) error {
	if candidate.Proof == nil || candidate.Artifact.Kind != ArtifactRawOutbox || candidate.Artifact.WALRange == nil {
		return fmt.Errorf("raw completion candidate identity is incomplete")
	}
	if err := candidate.Proof.Validate(); err != nil || !proofMatchesArtifact(candidate.Proof, candidate.Artifact) {
		return fmt.Errorf("raw completion candidate proof is invalid")
	}
	wantRemoteKey, err := layout.RemoteKey(archive.RawWALObjectKey(candidate.Artifact.ContentSHA256))
	if err != nil {
		return fmt.Errorf("raw completion remote key: %w", err)
	}
	if candidate.Proof.Remote.FullKey != wantRemoteKey {
		return fmt.Errorf("raw completion remote key is outside the current scope")
	}
	scopeConfigHash, err := layout.Scope.ConfigHash()
	if err != nil {
		return fmt.Errorf("raw completion scope hash: %w", err)
	}
	if candidate.Proof.ScopeConfigHash != scopeConfigHash {
		return fmt.Errorf("raw completion scope hash is outside the current scope")
	}
	manifestPrefix, err := layout.ManifestPrefix(date)
	if err != nil {
		return fmt.Errorf("raw completion manifest prefix: %w", err)
	}
	if candidate.Proof.CoveringManifestKey == manifestPrefix || !strings.HasPrefix(candidate.Proof.CoveringManifestKey, manifestPrefix) {
		return fmt.Errorf("raw completion manifest key is outside the current scope")
	}
	return nil
}

func completionMatchesAction(completion PruneCompletion, action PruneAction) error {
	artifact, err := completionArtifact(completion)
	if err != nil {
		return err
	}
	canonicalAction, err := makePruneAction(CandidateFact{Artifact: artifact, Proof: &completion.RetentionProof})
	if err != nil || canonicalAction != action {
		return fmt.Errorf("prune completion action identity changed")
	}
	// PlanDigest remains a historical audit binding. A retry may rebuild the
	// plan after the durable clock advances, while the proof and action
	// identity above remain the deletion authority.
	return nil
}

// InventoryPruneCompletions restores non-WAL candidates whose durable
// completion record was published before a crash or unlink fault. It verifies
// an existing source against the completion so the executor can safely retry;
// it does not perform any remote write or local source mutation.
func InventoryPruneCompletions(root, kind string, maxObjects, maxBytes uint64) ([]CandidateFact, error) {
	if root == "" || maxObjects == 0 || maxBytes == 0 {
		return nil, fmt.Errorf("prune completion inventory limits are required")
	}
	if kind != ArtifactRawOutbox && kind != ArtifactReplayOutbox && kind != ArtifactCache {
		return nil, fmt.Errorf("unsupported prune completion kind %q", kind)
	}
	absoluteRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve prune completion root: %w", err)
	}
	rootInfo, err := os.Lstat(absoluteRoot)
	if err != nil {
		return nil, fmt.Errorf("stat prune completion root: %w", err)
	}
	if rootInfo.Mode()&os.ModeSymlink != 0 || !rootInfo.IsDir() {
		return nil, fmt.Errorf("prune completion root is not trusted")
	}
	directory := pruneCompletionDirectory(absoluteRoot)
	directoryInfo, err := os.Lstat(directory)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("stat prune completion directory: %w", err)
	}
	if directoryInfo.Mode()&os.ModeSymlink != 0 || !directoryInfo.IsDir() {
		return nil, fmt.Errorf("prune completion directory is not trusted")
	}
	handle, err := os.Open(directory)
	if err != nil {
		return nil, fmt.Errorf("open prune completion directory: %w", err)
	}
	defer handle.Close()
	result := make([]CandidateFact, 0)
	var totalBytes uint64
	for {
		names, readErr := handle.Readdirnames(128)
		for _, name := range names {
			path := filepath.Join(directory, name)
			info, infoErr := os.Lstat(path)
			if infoErr != nil {
				return nil, fmt.Errorf("stat prune completion entry: %w", infoErr)
			}
			if info.Mode()&os.ModeSymlink != 0 || info.IsDir() || !info.Mode().IsRegular() {
				return nil, fmt.Errorf("prune completion directory contains an unsafe entry: %s", name)
			}
			if !strings.HasPrefix(name, "completion-artifact-") || !strings.HasSuffix(name, ".json") {
				return nil, fmt.Errorf("prune completion directory contains an unexpected entry: %s", name)
			}
			if uint64(len(result)) >= maxObjects {
				return nil, fmt.Errorf("prune completion inventory exceeds object limit")
			}
			if info.Size() < 0 || uint64(info.Size()) > maxBytes-totalBytes {
				return nil, fmt.Errorf("prune completion exceeds byte limit: %s", name)
			}
			data, err := os.ReadFile(path)
			if err != nil || uint64(len(data)) != uint64(info.Size()) {
				return nil, fmt.Errorf("read prune completion: %s", name)
			}
			completion, err := DecodePruneCompletion(data)
			if err != nil {
				return nil, fmt.Errorf("decode prune completion %s: %w", name, err)
			}
			wantPath, err := pruneCompletionPath(directory, completion.ArtifactID)
			if err != nil || filepath.Base(wantPath) != name {
				return nil, fmt.Errorf("prune completion filename is not bound to artifact")
			}
			if completion.ArtifactKind != kind {
				return nil, fmt.Errorf("prune completion kind is not expected")
			}
			artifact, err := completionArtifact(completion)
			if err != nil {
				return nil, err
			}
			if path, pathErr := trustedArtifactPath(absoluteRoot, artifact.TrustedPath); pathErr == nil {
				if verifyErr := verifyLocalArtifact(path, artifact); verifyErr != nil {
					return nil, fmt.Errorf("completed prune artifact changed: %w", verifyErr)
				}
			} else if !errors.Is(pathErr, os.ErrNotExist) {
				return nil, fmt.Errorf("inspect completed prune artifact: %w", pathErr)
			}
			proof := completion.RetentionProof
			result = append(result, CandidateFact{Artifact: artifact, Proof: &proof, FreshRemote: true, CoverageVerified: true})
			totalBytes += uint64(len(data))
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return nil, fmt.Errorf("read prune completion directory: %w", readErr)
		}
	}
	return result, nil
}
