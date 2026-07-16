package retention

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"tick-data-platform/internal/operations"
)

func plannerHash(char byte) [32]byte {
	var result [32]byte
	for index := range result {
		result[index] = char
	}
	return result
}

func plannerCandidate(start, end uint64, path string, char byte) CandidateFact {
	startRoot := plannerHash('a')
	if start == 1 {
		startRoot = [32]byte{}
	}
	artifact := LocalArtifact{
		Kind: ArtifactWALSegment, TrustedPath: path, Bytes: 100 + start, ContentSHA256: plannerHash(char),
		WALRange: &WALRange{StartSequence: start, EndSequence: end, StartChainRoot: startRoot, EndChainRoot: plannerHash('b')},
	}
	proof := &RetentionProof{
		ProofVersion: RetentionProofVersion, ArtifactKind: ArtifactWALSegment, TrustedRelativePath: path,
		Bytes: artifact.Bytes, ContentSHA256: artifact.ContentSHA256, ScopeConfigHash: plannerHash('s'), WALRange: artifact.WALRange,
		Remote:              RemoteObjectObservation{Class: RemoteObservationExact, FullKey: "remote/" + path, Bytes: artifact.Bytes, SHA256: artifact.ContentSHA256},
		CoveringManifestKey: "remote/manifest.json", CoveringManifestDigest: plannerHash('c'), VerificationReportDigest: plannerHash('d'),
		ObservedWallTimeUnixMS: 100, GraceNotBeforeUnixMS: 200,
		Limits: ProofLimits{MaxProofObjects: 100, MaxProofBytes: 1 << 20, MaxManifestNodes: 100},
	}
	return CandidateFact{Artifact: artifact, Proof: proof, FreshRemote: true, CoverageVerified: true}
}

func plannerInput(candidates ...CandidateFact) PlannerInput {
	return PlannerInput{
		Candidates: candidates, CurrentWallTimeUnixMS: 300, DurableWallTimeUnixMS: 300,
		WALExpectedStart: 1, GraceMS: 100, MaxCandidates: 100, ProofLimits: ProofLimits{MaxProofObjects: 100, MaxProofBytes: 1 << 20, MaxManifestNodes: 100}, Disk: DiskHigh,
	}
}

func TestBuildPrunePlanIsStableAndUsesWALPrefix(t *testing.T) {
	first := plannerCandidate(1, 1, "sealed/segment-000001.wal", '1')
	second := plannerCandidate(2, 2, "sealed/segment-000002.wal", '2')
	left, err := BuildPrunePlan(plannerInput(first, second))
	if err != nil {
		t.Fatal(err)
	}
	right, err := BuildPrunePlan(plannerInput(second, first))
	if err != nil {
		t.Fatal(err)
	}
	leftBytes, err := left.CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	rightBytes, err := right.CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	if string(leftBytes) != string(rightBytes) || left.PlanDigest != right.PlanDigest || len(left.Actions) != 2 {
		t.Fatalf("plan is not stable: left=%s right=%s", leftBytes, rightBytes)
	}
}

func TestBuildPrunePlanBlocksProofAndClockFailures(t *testing.T) {
	proofMissing := plannerCandidate(1, 1, "sealed/one.wal", '1')
	proofMissing.Proof = nil
	remoteDifferent := plannerCandidate(2, 2, "sealed/two.wal", '2')
	remoteDifferent.Proof.Remote.Class = RemoteObservationDifferent
	clock := plannerInput(proofMissing, remoteDifferent)
	clock.CurrentWallTimeUnixMS = 299
	plan, err := BuildPrunePlan(clock)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Actions) != 0 || len(plan.Blocked) != 2 {
		t.Fatalf("blocked plan = %+v", plan)
	}
	if plan.Blocked[0].Reason != "clock_regression" {
		t.Fatalf("clock reason = %+v", plan.Blocked)
	}

	grace := plannerCandidate(1, 1, "sealed/grace.wal", '3')
	grace.Proof.GraceNotBeforeUnixMS = 301
	plan, err = BuildPrunePlan(plannerInput(grace))
	if err != nil || len(plan.Actions) != 0 || plan.Blocked[0].Reason != "grace_not_elapsed" {
		t.Fatalf("grace plan = %+v; err=%v", plan, err)
	}
}

func TestBuildPrunePlanRejectsGapAndDuplicatePath(t *testing.T) {
	first := plannerCandidate(1, 1, "sealed/one.wal", '1')
	gap := plannerCandidate(3, 3, "sealed/three.wal", '3')
	plan, err := BuildPrunePlan(plannerInput(first, gap))
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Actions) != 1 || len(plan.Blocked) != 1 || plan.Blocked[0].Reason != "wal_prefix_gap" {
		t.Fatalf("gap plan = %+v", plan)
	}
	duplicate := first
	if _, err := BuildPrunePlan(plannerInput(first, duplicate)); err == nil {
		t.Fatal("duplicate candidate was accepted")
	}
	collision := plannerCandidate(2, 2, first.Artifact.TrustedPath, '2')
	if _, err := BuildPrunePlan(plannerInput(first, collision)); err == nil {
		t.Fatal("path collision was accepted")
	}
}

func TestBuildPrunePlanBlocksReplayOutboxUntilKindPolicyExists(t *testing.T) {
	candidate := plannerCandidate(1, 1, "objects/raw.bin", '1')
	candidate.Artifact.Kind = ArtifactReplayOutbox
	plan, err := BuildPrunePlan(plannerInput(candidate))
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Actions) != 0 || len(plan.Blocked) != 1 || plan.Blocked[0].Reason != "artifact_policy_unimplemented" {
		t.Fatalf("non-WAL policy plan = %+v", plan)
	}
}

func TestInventoryRejectsSymlink(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "sealed"), 0o700); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(root, "target")
	if err := os.WriteFile(target, []byte("not a WAL"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(root, "sealed", "escape.wal")); err != nil {
		t.Skipf("symlink creation unavailable: %v", err)
	}
	if _, err := InventoryWALSegments(root, operations.DefaultResourceLimits.MaxProofObjects); err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("symlink inventory error = %v", err)
	}
}

func TestInventoryRejectsWALByteBudgetBeforeReading(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "sealed"), 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "sealed", "segment-00000000000000000001-00000000000000000001.wal")
	if err := os.WriteFile(path, []byte("too-large"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := InventoryWALSegments(root, 10, 1); err == nil || !strings.Contains(err.Error(), "byte limit") {
		t.Fatalf("byte budget error = %v", err)
	}
}
