package retention

import "testing"

func TestRetentionProofContractRequiresFreshExactEvidence(t *testing.T) {
	proof := RetentionProof{
		ProofVersion: RetentionProofVersion, ArtifactKind: "wal_segment", TrustedRelativePath: "segments/000001.wal", Bytes: 12,
		ContentSHA256: hashByte("11"), ScopeConfigHash: hashByte("s"), WALRange: &WALRange{StartSequence: 1, EndSequence: 1, EndChainRoot: hashByte("33")},
		Remote:              RemoteObjectObservation{Class: RemoteObservationExact, FullKey: "immutable/raw/segment-11", SHA256: hashByte("11"), Bytes: 12},
		CoveringManifestKey: "immutable/snapshots/raw/day-1.json", CoveringManifestDigest: hashByte("44"), VerificationReportDigest: hashByte("55"),
		ObservedWallTimeUnixMS: 100, GraceNotBeforeUnixMS: 200, Limits: ProofLimits{MaxProofObjects: 10, MaxProofBytes: 1000, MaxManifestNodes: 10},
	}
	canonical, err := proof.CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeRetentionProof(canonical); err != nil {
		t.Fatal(err)
	}
	proof.Remote.Class = RemoteObservationDifferent
	if _, err := proof.CanonicalJSON(); err == nil {
		t.Fatal("non-exact observation was accepted")
	}
}

func TestPruneCheckpointContractRejectsZeroIdentity(t *testing.T) {
	proof := RetentionProof{
		ProofVersion: RetentionProofVersion, ArtifactKind: ArtifactWALSegment, TrustedRelativePath: "sealed/segment-00000000000000000001-00000000000000000001.wal", Bytes: 12,
		ContentSHA256: hashByte("22"), ScopeConfigHash: hashByte("s"), WALRange: &WALRange{StartSequence: 1, EndSequence: 1, EndChainRoot: hashByte("11")},
		Remote:              RemoteObjectObservation{Class: RemoteObservationExact, FullKey: "immutable/raw/segment-22", SHA256: hashByte("22"), Bytes: 12},
		CoveringManifestKey: "immutable/snapshots/raw/day-1.json", CoveringManifestDigest: hashByte("44"), VerificationReportDigest: hashByte("55"),
		ObservedWallTimeUnixMS: 100, GraceNotBeforeUnixMS: 200, Limits: ProofLimits{MaxProofObjects: 10, MaxProofBytes: 1000, MaxManifestNodes: 10},
	}
	proofDigest, err := proof.Digest()
	if err != nil {
		t.Fatal(err)
	}
	checkpoint := PruneCheckpoint{CheckpointVersion: PruneCheckpointVersion, EndSequence: 1, RetainedChainRoot: hashByte("11"), LastSegmentSHA256: hashByte("22"), RetentionProofDigest: proofDigest, RetentionProof: proof}
	canonical, err := checkpoint.CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodePruneCheckpoint(canonical); err != nil {
		t.Fatal(err)
	}
	checkpoint.LastSegmentSHA256 = [32]byte{}
	if _, err := checkpoint.CanonicalJSON(); err == nil {
		t.Fatal("zero segment digest was accepted")
	}
}

func hashByte(prefix string) [32]byte {
	var result [32]byte
	for i := range result {
		result[i] = []byte(prefix)[0]
	}
	return result
}
