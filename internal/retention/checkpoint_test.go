package retention

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func checkpointHash(char byte) [32]byte {
	var result [32]byte
	for index := range result {
		result[index] = char
	}
	return result
}

func checkpointProof(end uint64, content, chainRoot [32]byte) RetentionProof {
	return RetentionProof{
		ProofVersion: RetentionProofVersion, ArtifactKind: ArtifactWALSegment,
		TrustedRelativePath: "sealed/" + SegmentNameFromRange(1, end), Bytes: 1,
		ContentSHA256: content, ScopeConfigHash: checkpointHash('s'), WALRange: &WALRange{StartSequence: 1, EndSequence: end, EndChainRoot: chainRoot},
		Remote:              RemoteObjectObservation{Class: RemoteObservationExact, FullKey: "remote/object", SHA256: content, Bytes: 1},
		CoveringManifestKey: "remote/manifest.json", CoveringManifestDigest: checkpointHash('c'),
		VerificationReportDigest: checkpointHash('d'), ObservedWallTimeUnixMS: 100, GraceNotBeforeUnixMS: 200,
		Limits: ProofLimits{MaxProofObjects: 10, MaxProofBytes: 1000, MaxManifestNodes: 10},
	}
}

func testCheckpoint(end uint64, previous [32]byte) PruneCheckpoint {
	content := checkpointHash(byte(end + 10))
	chainRoot := checkpointHash(byte(end))
	proof := checkpointProof(end, content, chainRoot)
	proofDigest, _ := proof.Digest()
	return PruneCheckpoint{
		CheckpointVersion:        PruneCheckpointVersion,
		EndSequence:              end,
		RetainedChainRoot:        chainRoot,
		LastSegmentSHA256:        content,
		RetentionProofDigest:     proofDigest,
		RetentionProof:           proof,
		PreviousCheckpointDigest: previous,
	}
}

func TestCheckpointPublishIsCanonicalAppendOnlyAndIdempotent(t *testing.T) {
	root := t.TempDir()
	first := testCheckpoint(2, [32]byte{})
	if err := PublishCheckpoint(root, first); err != nil {
		t.Fatal(err)
	}
	if err := PublishCheckpoint(root, first); err != nil {
		t.Fatalf("same checkpoint retry failed: %v", err)
	}
	chain, err := LoadCheckpointChain(root)
	if err != nil || len(chain) != 1 || chain[0].Checkpoint.EndSequence != 2 {
		t.Fatalf("chain = %+v, err=%v", chain, err)
	}
	second := testCheckpoint(4, chain[0].Digest)
	if err := PublishCheckpoint(root, second); err != nil {
		t.Fatal(err)
	}
	latest, err := LoadLatestCheckpoint(root)
	if err != nil || latest.EndSequence != 4 {
		t.Fatalf("latest = %+v, err=%v", latest, err)
	}
	wrong := testCheckpoint(4, chain[0].Digest)
	wrong.LastSegmentSHA256 = checkpointHash('x')
	if err := PublishCheckpoint(root, wrong); !errors.Is(err, ErrPruneIntegrity) {
		t.Fatalf("different same-sequence checkpoint err=%v", err)
	}
}

func TestCheckpointRejectsUncommittedAndUnknownEntries(t *testing.T) {
	root := t.TempDir()
	directory := CheckpointDirectory(root)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, ".checkpoint-crash.tmp"), []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadCheckpointChain(root); !errors.Is(err, ErrCheckpointAbsent) {
		t.Fatalf("temporary-only directory err=%v", err)
	}
	if err := os.WriteFile(filepath.Join(directory, "unexpected.json"), []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadCheckpointChain(root); !errors.Is(err, ErrPruneIntegrity) {
		t.Fatalf("unknown entry err=%v", err)
	}
}

func TestCheckpointAndTrashDirectoriesRejectSymlinks(t *testing.T) {
	root := t.TempDir()
	target := t.TempDir()
	if err := os.Symlink(target, CheckpointDirectory(root)); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	if _, err := LoadCheckpointChain(root); !errors.Is(err, ErrPruneIntegrity) {
		t.Fatalf("checkpoint symlink err=%v", err)
	}
	if err := os.Remove(CheckpointDirectory(root)); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, TrashDirectory(root)); err != nil {
		t.Skipf("trash symlink unavailable: %v", err)
	}
	if err := RecoverPrune(root, 10); !errors.Is(err, ErrPruneIntegrity) {
		t.Fatalf("trash symlink err=%v", err)
	}
}
