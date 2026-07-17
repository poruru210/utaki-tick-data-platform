package retention

import (
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"tick-data-platform/internal/archive"
	"tick-data-platform/internal/testsupport"
	"tick-data-platform/internal/wal"
	"tick-data-platform/producers/fake"
)

type failAtPoint struct {
	point PruneFaultPoint
}

func (f failAtPoint) Inject(point PruneFaultPoint, _ string) error {
	if point == f.point {
		return errors.New("injected fault")
	}
	return nil
}

func TestPruneExecutorPublishesCheckpointBeforeTrashAndRecovers(t *testing.T) {
	root := t.TempDir()
	store, err := testsupport.NewStartedWAL(root, "gateway-test-01")
	if err != nil {
		t.Fatal(err)
	}
	fixture, err := fake.BatchFixture()
	if err != nil {
		t.Fatal(err)
	}
	entry, err := store.Append(fixture.Frame, 1710000000, 42)
	if err != nil {
		t.Fatal(err)
	}
	segment, err := store.Seal()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	artifact := LocalArtifact{
		Kind: ArtifactWALSegment, TrustedPath: filepath.ToSlash(filepath.Join("sealed", filepath.Base(segment.Path))),
		Bytes: uint64(segment.FileBytes), ContentSHA256: segment.ObjectSHA256,
		WALRange: &WALRange{StartSequence: segment.StartSequence, EndSequence: segment.LastSequence, StartChainRoot: segment.ChainStart, EndChainRoot: segment.ChainRoot},
	}
	proof := &RetentionProof{
		ProofVersion: RetentionProofVersion, ArtifactKind: artifact.Kind, TrustedRelativePath: artifact.TrustedPath,
		Bytes: artifact.Bytes, ContentSHA256: artifact.ContentSHA256, ScopeConfigHash: plannerHash('s'), WALRange: artifact.WALRange,
		Remote:              RemoteObjectObservation{Class: RemoteObservationExact, FullKey: "remote/" + filepath.Base(segment.Path), Bytes: artifact.Bytes, SHA256: artifact.ContentSHA256},
		CoveringManifestKey: "remote/manifest.json", CoveringManifestDigest: plannerHash('c'), VerificationReportDigest: plannerHash('d'),
		ObservedWallTimeUnixMS: 100, GraceNotBeforeUnixMS: 200,
		Limits: ProofLimits{MaxProofObjects: 100, MaxProofBytes: 1 << 20, MaxManifestNodes: 100},
	}
	candidate := CandidateFact{Artifact: artifact, Proof: proof, FreshRemote: true, CoverageVerified: true}
	plan, err := BuildPrunePlan(PlannerInput{Candidates: []CandidateFact{candidate}, CurrentWallTimeUnixMS: 300, DurableWallTimeUnixMS: 300, WALExpectedStart: 1, GraceMS: 100, MaxCandidates: 10, ProofLimits: proof.Limits, Disk: DiskHigh})
	if err != nil {
		t.Fatal(err)
	}
	executor, err := NewPruneExecutor(PruneRoots{WALRoot: root}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if report, err := executor.Execute(context.Background(), plan, []CandidateFact{candidate}); err != nil || len(report.Completed) != 1 {
		t.Fatalf("execution report=%+v err=%v", report, err)
	}
	if _, err := os.Stat(segment.Path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("source WAL still exists: %v", err)
	}
	if err := RecoverPrune(root, 100); err != nil {
		t.Fatal(err)
	}
	checkpoint, err := LoadLatestCheckpoint(root)
	if err != nil || checkpoint.EndSequence != 1 || checkpoint.RetainedChainRoot != entry.EntryHash {
		t.Fatalf("checkpoint=%+v err=%v", checkpoint, err)
	}
	anchored, err := testsupport.NewStartedWALWithAnchor(root, "gateway-test-01", &wal.PruneAnchor{EndSequence: 1, ChainRoot: entry.EntryHash})
	if err != nil {
		t.Fatal(err)
	}
	_ = anchored.Stop(context.Background())
}

func TestPruneExecutorFaultLeavesCheckpointedSourceForRecovery(t *testing.T) {
	root := t.TempDir()
	store, err := testsupport.NewStartedWAL(root, "gateway-test-01")
	if err != nil {
		t.Fatal(err)
	}
	fixture, err := fake.BatchFixture()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(fixture.Frame, 1710000000, 42); err != nil {
		t.Fatal(err)
	}
	segment, err := store.Seal()
	if err != nil {
		t.Fatal(err)
	}
	_ = store.Stop(context.Background())
	artifact := LocalArtifact{Kind: ArtifactWALSegment, TrustedPath: filepath.ToSlash(filepath.Join("sealed", filepath.Base(segment.Path))), Bytes: uint64(segment.FileBytes), ContentSHA256: segment.ObjectSHA256, WALRange: &WALRange{StartSequence: segment.StartSequence, EndSequence: segment.LastSequence, StartChainRoot: segment.ChainStart, EndChainRoot: segment.ChainRoot}}
	proof := &RetentionProof{ProofVersion: RetentionProofVersion, ArtifactKind: artifact.Kind, TrustedRelativePath: artifact.TrustedPath, Bytes: artifact.Bytes, ContentSHA256: artifact.ContentSHA256, ScopeConfigHash: plannerHash('s'), WALRange: artifact.WALRange, Remote: RemoteObjectObservation{Class: RemoteObservationExact, FullKey: "remote/object", Bytes: artifact.Bytes, SHA256: artifact.ContentSHA256}, CoveringManifestKey: "remote/manifest", CoveringManifestDigest: plannerHash('c'), VerificationReportDigest: plannerHash('d'), ObservedWallTimeUnixMS: 100, GraceNotBeforeUnixMS: 200, Limits: ProofLimits{MaxProofObjects: 100, MaxProofBytes: 1 << 20, MaxManifestNodes: 100}}
	candidate := CandidateFact{Artifact: artifact, Proof: proof, FreshRemote: true, CoverageVerified: true}
	plan, err := BuildPrunePlan(PlannerInput{Candidates: []CandidateFact{candidate}, CurrentWallTimeUnixMS: 300, DurableWallTimeUnixMS: 300, WALExpectedStart: 1, GraceMS: 100, MaxCandidates: 10, ProofLimits: proof.Limits, Disk: DiskHigh})
	if err != nil {
		t.Fatal(err)
	}
	executor, err := NewPruneExecutor(PruneRoots{WALRoot: root}, failAtPoint{point: FaultBeforeTrashRename})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := executor.Execute(context.Background(), plan, []CandidateFact{candidate}); !errors.Is(err, ErrPruneAvailability) {
		t.Fatalf("fault error = %v", err)
	}
	if _, err := LoadLatestCheckpoint(root); err != nil {
		t.Fatalf("checkpoint not durable after injected fault: %v", err)
	}
	if err := RecoverPrune(root, 100); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(segment.Path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("recovered source still exists: %v", err)
	}
}

func TestPruneExecutorRejectsReplayOutboxActionUntilKindPolicyExists(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "objects", "raw.bin")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	body := []byte("raw-outbox")
	digest := sha256.Sum256(body)
	artifact := LocalArtifact{
		Kind: ArtifactReplayOutbox, TrustedPath: "objects/raw.bin", Bytes: uint64(len(body)), ContentSHA256: digest,
		Replay: &ReplayIdentity{
			DatasetID: "dataset", CampaignID: "campaign", Date: "2024-03-09", ManifestKey: "remote/manifest.json",
			ManifestSHA256: plannerHash('m'), PartSetRoot: plannerHash('p'), CanonicalStreamRowChainRoot: plannerHash('r'),
		},
	}
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatal(err)
	}
	proof := &RetentionProof{
		ProofVersion: RetentionProofVersion, ArtifactKind: artifact.Kind, TrustedRelativePath: artifact.TrustedPath,
		Bytes: artifact.Bytes, ContentSHA256: artifact.ContentSHA256, ScopeConfigHash: plannerHash('s'), Replay: artifact.Replay,
		Remote:              RemoteObjectObservation{Class: RemoteObservationExact, FullKey: "remote/raw.bin", Bytes: artifact.Bytes, SHA256: artifact.ContentSHA256},
		CoveringManifestKey: "remote/manifest.json", CoveringManifestDigest: plannerHash('c'), VerificationReportDigest: plannerHash('v'),
		ObservedWallTimeUnixMS: 100, GraceNotBeforeUnixMS: 200,
		Limits: ProofLimits{MaxProofObjects: 100, MaxProofBytes: 1 << 20, MaxManifestNodes: 100},
	}
	candidate := CandidateFact{Artifact: artifact, Proof: proof, FreshRemote: true, CoverageVerified: true}
	action, err := makePruneAction(candidate)
	if err != nil {
		t.Fatal(err)
	}
	plan := PrunePlan{
		PlanVersion: PrunePlanVersion, Disk: DiskHigh, CurrentWallTimeUnixMS: 300, DurableWallTimeUnixMS: 300,
		WALExpectedStart: 1, GraceMS: 100, Actions: []PruneAction{action},
	}
	canonical, err := plan.CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	plan.PlanDigest = PrunePlanDigest(canonical)
	executor, err := NewPruneExecutor(PruneRoots{ReplayOutboxRoot: root}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if report, err := executor.Execute(context.Background(), plan, []CandidateFact{candidate}); !errors.Is(err, ErrPruneIntegrity) || len(report.Completed) != 0 {
		t.Fatalf("first outbox execution report=%+v err=%v", report, err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("outbox source was changed by rejected execution: %v", err)
	}
	if report, err := executor.Execute(context.Background(), plan, []CandidateFact{candidate}); !errors.Is(err, ErrPruneIntegrity) || len(report.Completed) != 0 {
		t.Fatalf("retry outbox execution report=%+v err=%v", report, err)
	}
}

func TestPruneExecutorRawOutboxCompletionIsInventorySafeAndRecoverable(t *testing.T) {
	root, segment, walCandidate, _ := newWALPruneFixture(t)
	rawRoot := filepath.Join(t.TempDir(), "raw-outbox")
	if err := os.MkdirAll(rawRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	rawPath := filepath.Join(rawRoot, filepath.FromSlash(archive.RawWALObjectKey(segment.ObjectSHA256)))
	if err := os.MkdirAll(filepath.Dir(rawPath), 0o700); err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(segment.Path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(rawPath, body, 0o600); err != nil {
		t.Fatal(err)
	}
	artifact := LocalArtifact{
		Kind: ArtifactRawOutbox, TrustedPath: archive.RawWALObjectKey(segment.ObjectSHA256),
		Bytes: walCandidate.Artifact.Bytes, ContentSHA256: walCandidate.Artifact.ContentSHA256,
		WALRange: cloneWALRange(walCandidate.Artifact.WALRange),
	}
	proof := *walCandidate.Proof
	proof.ArtifactKind = artifact.Kind
	proof.TrustedRelativePath = artifact.TrustedPath
	proof.WALRange = cloneWALRange(artifact.WALRange)
	candidate := CandidateFact{Artifact: artifact, Proof: &proof, FreshRemote: true, CoverageVerified: true}
	plan, err := BuildPrunePlan(PlannerInput{
		Candidates: []CandidateFact{candidate}, CurrentWallTimeUnixMS: 300, DurableWallTimeUnixMS: 300,
		WALExpectedStart: 1, GraceMS: 100, MaxCandidates: 10, ProofLimits: proof.Limits, Disk: DiskHigh,
	})
	if err != nil {
		t.Fatal(err)
	}
	executor, err := NewPruneExecutor(PruneRoots{RawOutboxRoot: rawRoot}, failAtPoint{point: FaultBeforeUnlink})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := executor.Execute(context.Background(), plan, []CandidateFact{candidate}); !errors.Is(err, ErrPruneAvailability) {
		t.Fatalf("pre-unlink fault error=%v", err)
	}
	if _, err := os.Stat(rawPath); err != nil {
		t.Fatalf("raw outbox source disappeared before unlink retry: %v", err)
	}
	retryCandidates, err := InventoryPruneCompletions(rawRoot, ArtifactRawOutbox, 10, 1<<20)
	if err != nil || len(retryCandidates) != 1 {
		t.Fatalf("present completion should yield a verified retry candidate: recovered=%+v err=%v", retryCandidates, err)
	}
	retryPlan, err := BuildPrunePlan(PlannerInput{
		Candidates: retryCandidates, CurrentWallTimeUnixMS: 400, DurableWallTimeUnixMS: 400,
		WALExpectedStart: 1, GraceMS: 100, MaxCandidates: 10, ProofLimits: proof.Limits, Disk: DiskHigh,
	})
	if err != nil {
		t.Fatal(err)
	}
	if retryPlan.PlanDigest == plan.PlanDigest {
		t.Fatal("retry plan unexpectedly retained the original durable-clock digest")
	}
	executor, err = NewPruneExecutor(PruneRoots{RawOutboxRoot: rawRoot}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if report, err := executor.Execute(context.Background(), retryPlan, retryCandidates); err != nil || len(report.Completed) != 1 {
		t.Fatalf("raw outbox execution report=%+v err=%v", report, err)
	}
	if _, err := os.Stat(rawPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("raw outbox source still exists: %v", err)
	}
	if _, err := InventoryFiles(rawRoot, ArtifactRawOutbox, 10, 1<<20); err != nil {
		t.Fatalf("completion metadata poisoned raw inventory: %v", err)
	}
	recovered, err := InventoryPruneCompletions(rawRoot, ArtifactRawOutbox, 10, 1<<20)
	if err != nil || len(recovered) != 1 {
		t.Fatalf("recovered candidates=%+v err=%v", recovered, err)
	}
	if recovered[0].Proof == nil || recovered[0].Proof.ContentSHA256 != artifact.ContentSHA256 {
		t.Fatalf("recovered proof was not restored: %+v", recovered[0])
	}
	if report, err := executor.Execute(context.Background(), retryPlan, recovered); err != nil || len(report.Completed) != 1 {
		t.Fatalf("recovery execution report=%+v err=%v", report, err)
	}
	_ = root
}

func TestPruneExecutorFaultMatrixConvergesAfterRecovery(t *testing.T) {
	points := []PruneFaultPoint{
		FaultBeforeCheckpointPublish,
		FaultAfterCheckpointPublish,
		FaultCheckpointDirectorySync,
		FaultBeforeTrashRename,
		FaultAfterTrashRename,
		FaultTrashDirectorySync,
		FaultBeforeUnlink,
		FaultAfterUnlink,
	}
	for _, point := range points {
		t.Run(string(point), func(t *testing.T) {
			root, segment, candidate, plan := newWALPruneFixture(t)
			executor, err := NewPruneExecutor(PruneRoots{WALRoot: root}, failAtPoint{point: point})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := executor.Execute(context.Background(), plan, []CandidateFact{candidate}); !errors.Is(err, ErrPruneAvailability) {
				t.Fatalf("fault %s error = %v", point, err)
			}
			if err := RecoverPrune(root, 100); err != nil {
				t.Fatalf("recover after %s: %v", point, err)
			}
			_, statErr := os.Stat(segment.Path)
			if point == FaultBeforeCheckpointPublish {
				if statErr != nil {
					t.Fatalf("source after %s = %v", point, statErr)
				}
			} else if !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("source after %s = %v", point, statErr)
			}
		})
	}
}

func newWALPruneFixture(t *testing.T) (string, wal.VerifiedSegment, CandidateFact, PrunePlan) {
	t.Helper()
	root := t.TempDir()
	store, err := testsupport.NewStartedWAL(root, "gateway-test-01")
	if err != nil {
		t.Fatal(err)
	}
	fixture, err := fake.BatchFixture()
	if err != nil {
		_ = store.Stop(context.Background())
		t.Fatal(err)
	}
	entry, err := store.Append(fixture.Frame, 1710000000, 42)
	if err != nil {
		_ = store.Stop(context.Background())
		t.Fatal(err)
	}
	segment, err := store.Seal()
	if err != nil {
		_ = store.Stop(context.Background())
		t.Fatal(err)
	}
	if err := store.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	artifact := LocalArtifact{
		Kind: ArtifactWALSegment, TrustedPath: filepath.ToSlash(filepath.Join("sealed", filepath.Base(segment.Path))),
		Bytes: uint64(segment.FileBytes), ContentSHA256: segment.ObjectSHA256,
		WALRange: &WALRange{StartSequence: segment.StartSequence, EndSequence: segment.LastSequence, StartChainRoot: segment.ChainStart, EndChainRoot: segment.ChainRoot},
	}
	proof := &RetentionProof{
		ProofVersion: RetentionProofVersion, ArtifactKind: artifact.Kind, TrustedRelativePath: artifact.TrustedPath,
		Bytes: artifact.Bytes, ContentSHA256: artifact.ContentSHA256, ScopeConfigHash: plannerHash('s'), WALRange: artifact.WALRange,
		Remote:              RemoteObjectObservation{Class: RemoteObservationExact, FullKey: "remote/object", Bytes: artifact.Bytes, SHA256: artifact.ContentSHA256},
		CoveringManifestKey: "remote/manifest", CoveringManifestDigest: plannerHash('c'), VerificationReportDigest: plannerHash('d'),
		ObservedWallTimeUnixMS: 100, GraceNotBeforeUnixMS: 200,
		Limits: ProofLimits{MaxProofObjects: 100, MaxProofBytes: 1 << 20, MaxManifestNodes: 100},
	}
	candidate := CandidateFact{Artifact: artifact, Proof: proof, FreshRemote: true, CoverageVerified: true}
	plan, err := BuildPrunePlan(PlannerInput{Candidates: []CandidateFact{candidate}, CurrentWallTimeUnixMS: 300, DurableWallTimeUnixMS: 300, WALExpectedStart: 1, GraceMS: 100, MaxCandidates: 10, ProofLimits: proof.Limits, Disk: DiskHigh})
	if err != nil {
		t.Fatal(err)
	}
	_ = entry
	return root, segment, candidate, plan
}
