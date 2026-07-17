package retention

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"tick-data-platform/internal/archive"
	"tick-data-platform/internal/operations"
	"tick-data-platform/internal/protocol"
	"tick-data-platform/internal/r2"
	"tick-data-platform/internal/testsupport"
	"tick-data-platform/internal/wal"
	"tick-data-platform/producers/fake"
)

type rawObserverBackend struct {
	objects     map[string][]byte
	complete    bool
	ignoreLimit bool
}

func (b *rawObserverBackend) ListLimited(_ context.Context, prefix string, max uint64) (r2.ReplayRemoteObjectList, error) {
	objects := make([]r2.RemoteObject, 0)
	for key, body := range b.objects {
		if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			objects = append(objects, r2.RemoteObject{Key: key, Size: int64(len(body))})
		}
	}
	if uint64(len(objects)) > max {
		return r2.ReplayRemoteObjectList{}, r2.ErrResourceLimit
	}
	return r2.ReplayRemoteObjectList{Objects: objects, Complete: b.complete}, nil
}

func (b *rawObserverBackend) OpenLimited(_ context.Context, key string, max uint64) (io.ReadCloser, int64, error) {
	body, ok := b.objects[key]
	if !ok {
		return nil, 0, r2.ErrObjectNotFound
	}
	if !b.ignoreLimit && uint64(len(body)) > max {
		return nil, int64(len(body)), r2.ErrResourceLimit
	}
	return io.NopCloser(bytes.NewReader(body)), int64(len(body)), nil
}

func TestRawRetentionObserverChargesBytesBeforeClassifyingOversizedRead(t *testing.T) {
	backend := &rawObserverBackend{objects: map[string][]byte{"remote/object": []byte("12345")}, complete: true, ignoreLimit: true}
	limits := ProofLimits{MaxProofObjects: 2, MaxProofBytes: 4, MaxManifestNodes: 2}
	budget, err := NewProofObservationBudget(limits)
	if err != nil {
		t.Fatal(err)
	}
	observer := &RawRetentionObserver{remote: backend}
	if _, class := observer.readRemote(context.Background(), "remote/object", 4, nil, budget); class != RemoteObservationOversized {
		t.Fatalf("oversized read class = %s", class)
	}
	if budget.bytes != limits.MaxProofBytes {
		t.Fatalf("failed read did not exhaust shared byte budget: %d", budget.bytes)
	}
}

func observerScope() archive.ScopeConfig {
	return archive.ScopeConfig{
		DatasetID: "dataset", CampaignID: "campaign", ProviderID: "provider", StableFeedID: "feed", ExactSourceSymbol: "EURUSD",
		BrokerServerFingerprint: "broker", GatewayBuildIdentity: "gateway", ProducerBuildIdentity: "producer", DayDefinitionID: "day", SettlePolicy: "settle", PublisherID: "publisher", PublisherEpoch: 1,
		ProtocolVersion: protocol.ProtocolVersion, ProtocolLimits: archive.ProtocolLimits{MaxFrameBytes: protocol.MaxFrameBytes, MaxRecords: protocol.MaxRecords, MaxStringBytes: protocol.MaxStringBytes},
	}
}

func observerArtifact() LocalArtifact {
	return LocalArtifact{Kind: ArtifactWALSegment, TrustedPath: "sealed/segment-00000000000000000001-00000000000000000001.wal", Bytes: 4, ContentSHA256: [32]byte{1}, WALRange: &WALRange{StartSequence: 1, EndSequence: 1, EndChainRoot: [32]byte{2}}}
}

func observerLimits() ProofLimits {
	return ProofLimits{MaxProofObjects: 10, MaxProofBytes: 1 << 20, MaxManifestNodes: 10}
}

func TestRawRetentionObserverRejectsClaimMismatchBeforeObjectProof(t *testing.T) {
	layout, err := r2.NewLayout("immutable-root", observerScope())
	if err != nil {
		t.Fatal(err)
	}
	claim, err := r2.NewPublisherClaim(layout.Scope)
	if err != nil {
		t.Fatal(err)
	}
	claimKey, err := layout.ClaimKey(claim.PublisherEpoch)
	if err != nil {
		t.Fatal(err)
	}
	backend := &rawObserverBackend{objects: map[string][]byte{claimKey: []byte("wrong-claim")}, complete: true}
	observer, err := NewRawRetentionObserver(backend, layout, "2024-03-09", func() uint64 { return 1000 }, 100)
	if err != nil {
		t.Fatal(err)
	}
	fact, err := observer.Observe(context.Background(), observerArtifact(), observerLimits())
	if err != nil || fact.Class != RemoteObservationDifferent {
		t.Fatalf("claim mismatch fact=%+v err=%v", fact, err)
	}
}

func TestRawRetentionObserverFailsClosedOnIncompleteManifestListing(t *testing.T) {
	layout, err := r2.NewLayout("immutable-root", observerScope())
	if err != nil {
		t.Fatal(err)
	}
	backend := &rawObserverBackend{objects: map[string][]byte{}, complete: false}
	observer, err := NewRawRetentionObserver(backend, layout, "2024-03-09", func() uint64 { return 1000 }, 100)
	if err != nil {
		t.Fatal(err)
	}
	budget, err := NewProofObservationBudget(observerLimits())
	if err != nil {
		t.Fatal(err)
	}
	_, class := observer.readManifestGraph(context.Background(), observerLimits(), budget)
	if class != RemoteObservationAmbiguous {
		t.Fatalf("incomplete listing class=%s", class)
	}
}

func TestRawRetentionObserverVerifiesFullRemoteSnapshotSemantics(t *testing.T) {
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
	if _, err := store.Append(fixture.Frame, 1710000000, 42); err != nil {
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
	object, err := archive.PromoteSealedSegment(t.TempDir(), segment.Path)
	if err != nil {
		t.Fatal(err)
	}
	scope := observerScope()
	manifest, err := archive.BuildRawDayManifest(archive.RawDayManifestInput{
		Scope: scope, Date: "2024-03-09", RawObjects: []archive.RawObject{object},
		TerminalSyncStatus: "complete", CompletenessStatus: "settled_snapshot", LogicalCloseTimeS: 1710028800,
	})
	if err != nil {
		t.Fatal(err)
	}
	layout, err := r2.NewLayout("immutable-root", scope)
	if err != nil {
		t.Fatal(err)
	}
	claim, err := r2.NewPublisherClaim(scope)
	if err != nil {
		t.Fatal(err)
	}
	claimKey, err := layout.ClaimKey(claim.PublisherEpoch)
	if err != nil {
		t.Fatal(err)
	}
	claimBytes, err := claim.CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	manifestKey, err := layout.ManifestKey(manifest)
	if err != nil {
		t.Fatal(err)
	}
	manifestBytes, err := archive.ManifestCanonicalJSON(manifest)
	if err != nil {
		t.Fatal(err)
	}
	rawKey, err := layout.RemoteKey(object.Key)
	if err != nil {
		t.Fatal(err)
	}
	rawBytes, err := os.ReadFile(segment.Path)
	if err != nil {
		t.Fatal(err)
	}
	backend := &rawObserverBackend{objects: map[string][]byte{claimKey: claimBytes, manifestKey: manifestBytes, rawKey: rawBytes}, complete: true}
	observer, err := NewRawRetentionObserver(backend, layout, "2024-03-09", func() uint64 { return 1710000000000 }, 100)
	if err != nil {
		t.Fatal(err)
	}
	artifact := LocalArtifact{
		Kind: ArtifactWALSegment, TrustedPath: filepath.ToSlash(filepath.Join("sealed", filepath.Base(segment.Path))),
		Bytes: uint64(segment.FileBytes), ContentSHA256: segment.ObjectSHA256,
		WALRange: &WALRange{StartSequence: segment.StartSequence, EndSequence: segment.LastSequence, StartChainRoot: segment.ChainStart, EndChainRoot: segment.ChainRoot},
	}
	fact, err := observer.Observe(context.Background(), artifact, observerLimits())
	if err != nil || fact.Class != RemoteObservationExact || fact.Proof == nil || !fact.CoverageVerified {
		t.Fatalf("full remote snapshot fact=%+v err=%v", fact, err)
	}
	candidate := CandidateFact{Artifact: LocalArtifact{Kind: ArtifactRawOutbox, TrustedPath: archive.RawWALObjectKey(artifact.ContentSHA256), Bytes: artifact.Bytes, ContentSHA256: artifact.ContentSHA256, WALRange: cloneWALRange(artifact.WALRange)}, Proof: fact.Proof}
	candidate.Proof.ArtifactKind = ArtifactRawOutbox
	candidate.Proof.TrustedRelativePath = candidate.Artifact.TrustedPath
	if err := ValidateRawCompletionScope(candidate, layout, "2024-03-09"); err != nil {
		t.Fatalf("valid completion scope was rejected: %v", err)
	}
	otherScope := scope
	otherScope.CampaignID = "other-campaign"
	otherLayout, err := r2.NewLayout("immutable-root", otherScope)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateRawCompletionScope(candidate, otherLayout, "2024-03-09"); err == nil {
		t.Fatal("completion from another scope was accepted")
	}
	if fact.Proof.ObservedWallTimeUnixMS != 1710028800000 {
		t.Fatalf("proof did not use the verified manifest logical close time: %d", fact.Proof.ObservedWallTimeUnixMS)
	}
}

func TestRawRetentionObserverBlocksCrossDateSegments(t *testing.T) {
	frame, err := protocol.EncodeMessage(protocol.BatchFrameV1{
		RequestedFromMSC: time.Date(2024, 3, 9, 0, 0, 0, 0, time.UTC).UnixMilli(),
		ReturnedCount:    1,
		SourceSchemaID:   protocol.SourceSchemaMT5,
		Records:          []protocol.RawMqlTickV1{{TimeMSC: time.Date(2024, 3, 10, 0, 0, 0, 0, time.UTC).UnixMilli()}},
	})
	if err != nil {
		t.Fatal(err)
	}
	segment := wal.VerifiedSegment{Entries: []wal.Entry{{Sequence: 1, Frame: frame}}}
	if segmentOnlyContainsDate(segment, "2024-03-09") {
		t.Fatal("cross-date segment was accepted for a single-date retention proof")
	}
}

type unbudgetedObserver struct{}

func (unbudgetedObserver) Observe(context.Context, LocalArtifact, ProofLimits) (RemoteFact, error) {
	return RemoteFact{Class: RemoteObservationAbsent}, nil
}

func TestObserveCandidatesRejectsUnbudgetedMultiCandidateObserver(t *testing.T) {
	limits := operations.DefaultResourceLimits
	first := observerArtifact()
	second := first
	second.TrustedPath = "sealed/segment-00000000000000000002-00000000000000000002.wal"
	if _, err := ObserveCandidates(context.Background(), unbudgetedObserver{}, []LocalArtifact{first, second}, limits); err == nil {
		t.Fatal("unbudgeted multi-candidate observer was accepted")
	}
}
