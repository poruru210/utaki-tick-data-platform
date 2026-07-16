package r2

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"tick-data-platform/internal/archive"
	"tick-data-platform/internal/parquet"
	"tick-data-platform/internal/protocol"
)

type replayPublicationFixture struct {
	scope     archive.ScopeConfig
	layout    Layout
	manifest  archive.RawDayManifest
	input     ReplayPublicationInput
	backend   *fakeBackend
	publisher *ReplayPublisher
	tool      *replayPublisherTool
	artifacts []parquet.PartArtifact
	parts     []protocol.PartManifest
	replay    protocol.ReplayDayManifest
	rawObject archive.RawObject
	receipt   string
}

type replayPublisherTool struct {
	mu          sync.Mutex
	backend     *fakeBackend
	copyCalls   int
	checkCalls  int
	timeoutNext bool
	unknownNext bool
	copyKeys    []string
}

type terminalSecondReadBackend struct {
	base    ReplayRemoteReadBackend
	key     string
	outcome replayObserverOpenStep
	reads   int
}

type parquetListSizeMismatchBackend struct {
	base ReplayRemoteReadBackend
	key  string
}

func (b *parquetListSizeMismatchBackend) ListLimited(ctx context.Context, prefix string, max uint64) (ReplayRemoteObjectList, error) {
	listed, err := b.base.ListLimited(ctx, prefix, max)
	if err != nil {
		return ReplayRemoteObjectList{}, err
	}
	for index := range listed.Objects {
		if listed.Objects[index].Key == b.key {
			listed.Objects[index].Size++
		}
	}
	return listed, nil
}

func (b *parquetListSizeMismatchBackend) OpenLimited(ctx context.Context, key string, max uint64) (io.ReadCloser, int64, error) {
	return b.base.OpenLimited(ctx, key, max)
}

func (b *terminalSecondReadBackend) ListLimited(ctx context.Context, prefix string, max uint64) (ReplayRemoteObjectList, error) {
	return b.base.ListLimited(ctx, prefix, max)
}

func (b *terminalSecondReadBackend) OpenLimited(ctx context.Context, key string, max uint64) (io.ReadCloser, int64, error) {
	if key != b.key {
		return b.base.OpenLimited(ctx, key, max)
	}
	b.reads++
	if b.reads != 2 {
		return b.base.OpenLimited(ctx, key, max)
	}
	if b.outcome.err != nil {
		return nil, 0, b.outcome.err
	}
	size := int64(len(b.outcome.body))
	return &boundedReplayReadCloser{Reader: io.LimitReader(bytes.NewReader(b.outcome.body), int64(max)+1), Closer: io.NopCloser(bytes.NewReader(nil))}, size, nil
}

func (t *replayPublisherTool) PutFileIfAbsent(ctx context.Context, remoteKey, localPath string, expectedSHA256 [32]byte, expectedBytes uint64) (RemoteObjectCommit, error) {
	t.mu.Lock()
	t.copyCalls++
	t.copyKeys = append(t.copyKeys, remoteKey)
	timeout := t.timeoutNext
	unknown := t.unknownNext
	t.timeoutNext = false
	t.unknownNext = false
	t.mu.Unlock()
	if timeout {
		return RemoteObjectCommit{}, context.DeadlineExceeded
	}
	if unknown {
		return RemoteObjectCommit{}, errors.New("unknown copy outcome")
	}
	return t.backend.PutFileIfAbsent(ctx, remoteKey, localPath, expectedSHA256, expectedBytes)
}

func (t *replayPublisherTool) VerifyFile(ctx context.Context, remoteKey, localPath string, expectedSHA256 [32]byte, expectedBytes uint64) (RemoteObjectVerification, error) {
	t.mu.Lock()
	t.checkCalls++
	t.mu.Unlock()
	return t.backend.VerifyFile(ctx, remoteKey, localPath, expectedSHA256, expectedBytes)
}

func (t *replayPublisherTool) counts() (int, int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.copyCalls, t.checkCalls
}

func newReplayPublicationFixture(t *testing.T, empty bool) *replayPublicationFixture {
	t.Helper()
	scope := layoutTestScope()
	layout, err := NewLayout("v1", scope)
	if err != nil {
		t.Fatal(err)
	}
	var rawObject archive.RawObject
	var rawObjects []archive.RawObject
	if !empty {
		rawObject = publicationObject(t)
		rawObjects = []archive.RawObject{rawObject}
	}
	manifest, err := archive.BuildRawDayManifest(archive.RawDayManifestInput{
		Scope: scope, Date: "2024-03-09", RawObjects: rawObjects,
		TerminalSyncStatus: "complete", CompletenessStatus: "settled_snapshot", LogicalCloseTimeS: 1710028800,
	})
	if err != nil {
		t.Fatal(err)
	}
	rawBytes, err := archive.ManifestCanonicalJSON(manifest)
	if err != nil {
		t.Fatal(err)
	}
	backend := newFakeBackend()
	rawKey, err := layout.ManifestKey(manifest)
	if err != nil {
		t.Fatal(err)
	}
	backend.force(rawKey, rawBytes)
	if !empty {
		objectKey, err := layout.RemoteKey(manifest.ChainObjects[0].Key)
		if err != nil {
			t.Fatal(err)
		}
		objectBytes, err := os.ReadFile(rawObject.Path)
		if err != nil {
			t.Fatal(err)
		}
		backend.force(objectKey, objectBytes)
	}
	claim, err := NewPublisherClaim(scope)
	if err != nil {
		t.Fatal(err)
	}
	claimBytes, err := claim.CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	claimKey, err := layout.ClaimKey(scope.PublisherEpoch)
	if err != nil {
		t.Fatal(err)
	}
	backend.force(claimKey, claimBytes)

	spec, err := parquet.NewConversionSpec("replay-v1", "conversion-v1", "converter-test", "windows-amd64-go1.24.13", 1, 1<<20, 1)
	if err != nil {
		t.Fatal(err)
	}
	rawRelative, err := archive.RawDayManifestRelativeKey(scope, manifest)
	if err != nil {
		t.Fatal(err)
	}
	replayScope := protocol.ReplayScope{
		DatasetID: scope.DatasetID, CampaignID: scope.CampaignID, DayDefinitionID: scope.DayDefinitionID, Date: manifest.Date,
		ReplayContractID: spec.ReplayContractID, ConversionID: spec.ConversionID,
		RawDayManifestKey: rawRelative, RawDayManifestSHA256: manifest.ManifestSHA256,
	}
	var artifacts []parquet.PartArtifact
	var parts []protocol.PartManifest
	var rowRoot [32]byte
	if !empty {
		generator, err := parquet.NewGenerator(spec, replayScope, t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		for sequence := uint64(0); sequence < 4; sequence++ {
			if err := generator.WriteRow(replayPublicationDataRow(replayScope, sequence)); err != nil {
				t.Fatal(err)
			}
		}
		result, err := generator.Close()
		if err != nil {
			t.Fatal(err)
		}
		artifacts = result.Parts
		rowRoot = result.RowChainRoot
		conversion, _ := archive.ConversionTupleFromSpec(spec)
		var previous *protocol.PartManifest
		for _, artifact := range artifacts {
			partInput, err := archive.PartManifestInputFromArtifact(replayScope, conversion, artifact)
			if err != nil {
				t.Fatal(err)
			}
			part, err := archive.BuildPartManifest(partInput, previous)
			if err != nil {
				t.Fatal(err)
			}
			parts = append(parts, part)
			previous = &parts[len(parts)-1]
		}
	}
	conversion, _ := archive.ConversionTupleFromSpec(spec)
	replay, err := archive.BuildReplayDayManifest(archive.ReplayDayManifestInput{
		Scope: replayScope, Conversion: conversion, CompletenessStatus: "settled_snapshot",
		Parts: parts, CanonicalStreamRowChainRoot: rowRoot,
	})
	if err != nil {
		t.Fatal(err)
	}
	replayBytes, err := protocol.ReplayDayManifestCanonicalJSON(replay)
	if err != nil {
		t.Fatal(err)
	}
	partBytes := make([][]byte, len(parts))
	for index, part := range parts {
		partBytes[index], err = protocol.PartManifestCanonicalJSON(part)
		if err != nil {
			t.Fatal(err)
		}
	}
	remote, err := NewReplayRemoteReadAdapter(backend)
	if err != nil {
		t.Fatal(err)
	}
	tool := &replayPublisherTool{backend: backend}
	receiptPath := filepath.Join(t.TempDir(), "replay-receipt.json")
	publisher, err := NewReplayPublisher(layout, remote, tool, NewReplayDiagnosticEventStore(), FileReplayReceiptStore{}, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	rawPaths := map[string]string{}
	if !empty {
		rawPaths[rawObject.Key] = rawObject.Path
	}
	return &replayPublicationFixture{
		scope: scope, layout: layout, manifest: manifest, backend: backend, publisher: publisher, tool: tool,
		artifacts: artifacts, parts: parts, replay: replay, rawObject: rawObject, receipt: receiptPath,
		input: ReplayPublicationInput{
			Conversion: spec, Limits: protocol.ReplayPublicationImplementationBounds, RawManifestBytes: rawBytes,
			RawObjectPaths: rawPaths, Parts: artifacts, PartManifestBytes: partBytes,
			ReplayManifestBytes: replayBytes, ReceiptPath: receiptPath,
		},
	}
}

func (f *replayPublicationFixture) publish(t *testing.T) (ReplayVerificationReceipt, error) {
	t.Helper()
	return f.publisher.Publish(context.Background(), f.input)
}

func (f *replayPublicationFixture) sealedBundle(t *testing.T) ReplayPublicationBundle {
	t.Helper()
	bundle, err := SealReplayPublicationBundle(replayBundleInputFromFixture(t, f))
	if err != nil {
		t.Fatal(err)
	}
	return bundle
}

func TestNewReplayPublisherDerivesCampaignEpochLockFromRoot(t *testing.T) {
	fixture := newReplayPublicationFixture(t, true)
	root := filepath.Dir(fixture.publisher.lockPath)
	want, err := PublicationLockPath(root, fixture.layout.Scope)
	if err != nil {
		t.Fatal(err)
	}
	if fixture.publisher.lockPath != want {
		t.Fatalf("lock path = %q, want canonical %q", fixture.publisher.lockPath, want)
	}
	if fixture.publisher.lockPath == filepath.Join(root, "campaign.lock") {
		t.Fatal("publisher retained an arbitrary lock filename")
	}
}

func TestReplayPublisherFirstPublishAndSameContentRetry(t *testing.T) {
	fixture := newReplayPublicationFixture(t, false)
	first, err := fixture.publish(t)
	if err != nil {
		t.Fatal(err)
	}
	second, err := fixture.publish(t)
	if err != nil {
		t.Fatal(err)
	}
	if !first.VerificationComplete || !second.VerificationComplete || first.FinalObservationDigest != second.FinalObservationDigest {
		t.Fatalf("receipts differ: first=%+v second=%+v", first, second)
	}
}

func TestReplayPublisherRequiresExactClaimAndRawBeforeDerivative(t *testing.T) {
	for _, testCase := range []struct {
		name   string
		mutate func(*replayPublicationFixture)
	}{
		{"claim", func(f *replayPublicationFixture) {
			key, _ := f.layout.ClaimKey(f.scope.PublisherEpoch)
			f.backend.remove(key)
		}},
		{"raw", func(f *replayPublicationFixture) {
			key, _ := f.layout.ManifestKey(f.manifest)
			f.backend.force(key, []byte("malformed"))
		}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			fixture := newReplayPublicationFixture(t, false)
			testCase.mutate(fixture)
			if _, err := fixture.publish(t); !errors.Is(err, ErrReplayPublicationIntegrity) {
				t.Fatalf("publish error = %v", err)
			}
			copies, _ := fixture.tool.counts()
			if copies != 0 {
				t.Fatal("derivative copy ran before claim/raw Exact")
			}
		})
	}
}

func TestReplayPublisherAcceptsDataBeforeManifestCandidate(t *testing.T) {
	fixture := newReplayPublicationFixture(t, false)
	bundle := fixture.sealedBundle(t)
	object := bundle.Contract.ParquetObjects[0]
	body, err := os.ReadFile(bundle.LocalSources.Artifacts[ReplayObjectID(object.ObjectID)].Path)
	if err != nil {
		t.Fatal(err)
	}
	fixture.backend.force(object.FullKey, body)
	if _, err := fixture.publish(t); err != nil {
		t.Fatal(err)
	}
	copies, _ := fixture.tool.counts()
	if copies != len(bundle.Contract.ParquetObjects)+len(bundle.Contract.PartManifests) {
		t.Fatalf("copy count = %d", copies)
	}
}

func TestReplayPublisherRejectsUnrelatedOrphanAndReplayBranch(t *testing.T) {
	t.Run("orphan", func(t *testing.T) {
		fixture := newReplayPublicationFixture(t, false)
		bundle := fixture.sealedBundle(t)
		scope, _, _ := replayVerificationInputs(bundle.Contract)
		base, _ := protocol.ReplayDerivativeBaseKey(scope)
		fixture.backend.force(bundle.Contract.Scope.ImmutablePrefix+"/"+base+"/unknown.bin", []byte{1})
		if _, err := fixture.publish(t); !errors.Is(err, ErrReplayPublicationIntegrity) {
			t.Fatalf("orphan error = %v", err)
		}
	})
	t.Run("branch", func(t *testing.T) {
		fixture := newReplayPublicationFixture(t, false)
		bundle := fixture.sealedBundle(t)
		scope, _, _ := replayVerificationInputs(bundle.Contract)
		base, _ := protocol.ReplayDerivativeBaseKey(scope)
		fixture.backend.force(bundle.Contract.Scope.ImmutablePrefix+"/"+base+"/replay-day-2-"+strings.Repeat("a", 64)+".json", []byte("{}"))
		if _, err := fixture.publish(t); !errors.Is(err, ErrReplayPublicationIntegrity) {
			t.Fatalf("branch error = %v", err)
		}
	})
}

func TestReplayPublisherLocalMutationAfterLockStopsBeforeRemoteCopy(t *testing.T) {
	fixture := newReplayPublicationFixture(t, false)
	fixture.publisher.hooks.afterLock = func() error {
		file, err := os.OpenFile(fixture.artifacts[0].Path, os.O_APPEND|os.O_WRONLY, 0)
		if err != nil {
			return err
		}
		_, writeErr := file.Write([]byte{0})
		_ = file.Close()
		return writeErr
	}
	if _, err := fixture.publish(t); !errors.Is(err, ErrReplayPublicationIntegrity) {
		t.Fatalf("mutation error = %v", err)
	}
	copies, _ := fixture.tool.counts()
	if copies != 0 {
		t.Fatal("mutated local source reached remote copy")
	}
}

func TestReplayPublisherTimeoutAndUnknownOutcomeNeverSaveReceipt(t *testing.T) {
	for _, unknown := range []bool{false, true} {
		fixture := newReplayPublicationFixture(t, false)
		if unknown {
			fixture.tool.unknownNext = true
		} else {
			fixture.tool.timeoutNext = true
		}
		if _, err := fixture.publish(t); !errors.Is(err, ErrReplayPublicationRetry) {
			t.Fatalf("retry error = %v", err)
		}
		if _, err := os.Stat(fixture.receipt); !os.IsNotExist(err) {
			t.Fatalf("receipt exists after unknown outcome: %v", err)
		}
	}
}

func TestReplayPublisherTerminalSecondReadStopsWithoutActionOrReceipt(t *testing.T) {
	wrongFixture := newReplayPublicationFixture(t, true)
	for _, testCase := range []struct {
		name    string
		outcome replayObserverOpenStep
		wantErr error
	}{
		{name: "timeout_unavailable", outcome: replayObserverOpenStep{err: context.DeadlineExceeded}, wantErr: ErrReplayPublicationRetry},
		{name: "budget_oversized", outcome: replayObserverOpenStep{err: ErrResourceLimit}, wantErr: ErrReplayPublicationResource},
		{name: "readable_invalid_ambiguous", outcome: replayObserverOpenStep{body: []byte("{")}, wantErr: ErrReplayPublicationIntegrity},
		{name: "valid_wrong_identity_different", outcome: replayObserverOpenStep{body: wrongFixture.input.ReplayManifestBytes}, wantErr: ErrReplayPublicationIntegrity},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			fixture := newReplayPublicationFixture(t, false)
			if _, err := fixture.publish(t); err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(fixture.receipt); err != nil {
				t.Fatal(err)
			}
			copiesBefore, _ := fixture.tool.counts()
			base := fixture.publisher.remote
			fixture.publisher.remote = &terminalSecondReadBackend{base: base, key: fixture.sealedBundle(t).Contract.ReplayManifest.FullKey, outcome: testCase.outcome}

			if _, err := fixture.publish(t); !errors.Is(err, testCase.wantErr) {
				t.Fatalf("terminal second read error = %v, want %v", err, testCase.wantErr)
			}
			copiesAfter, _ := fixture.tool.counts()
			if copiesAfter != copiesBefore {
				t.Fatalf("non-Exact terminal read authorized action: before=%d after=%d", copiesBefore, copiesAfter)
			}
			if _, err := os.Stat(fixture.receipt); !os.IsNotExist(err) {
				t.Fatalf("receipt exists after non-Exact terminal read: %v", err)
			}
		})
	}
}

func TestReplayPublisherParquetListSizeMismatchStopsWithoutActionOrReceipt(t *testing.T) {
	fixture := newReplayPublicationFixture(t, false)
	if _, err := fixture.publish(t); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(fixture.receipt); err != nil {
		t.Fatal(err)
	}
	copiesBefore, _ := fixture.tool.counts()
	target := fixture.sealedBundle(t).Contract.ParquetObjects[0]
	fixture.publisher.remote = &parquetListSizeMismatchBackend{base: fixture.publisher.remote, key: target.FullKey}
	if _, err := fixture.publish(t); !errors.Is(err, ErrReplayPublicationIntegrity) {
		t.Fatalf("Parquet list size mismatch = %v", err)
	}
	copiesAfter, _ := fixture.tool.counts()
	if copiesAfter != copiesBefore {
		t.Fatalf("Parquet list size mismatch authorized action: before=%d after=%d", copiesBefore, copiesAfter)
	}
	if _, err := os.Stat(fixture.receipt); !os.IsNotExist(err) {
		t.Fatalf("receipt exists after Parquet list size mismatch: %v", err)
	}
}

func TestReplayPublisherReobservesAfterEveryActionAndSharesBudget(t *testing.T) {
	fixture := newReplayPublicationFixture(t, false)
	var requests []uint64
	fixture.publisher.hooks.afterObservation = func(_ uint64, observation ReplayRemoteObservation) error {
		requests = append(requests, observation.RequestCount)
		return nil
	}
	if _, err := fixture.publish(t); err != nil {
		t.Fatal(err)
	}
	copies, _ := fixture.tool.counts()
	if len(requests) != copies+1 {
		t.Fatalf("observations=%d copies=%d", len(requests), copies)
	}
	for index := 1; index < len(requests); index++ {
		if requests[index] <= requests[index-1] {
			t.Fatalf("budget reset between rounds: %v", requests)
		}
	}
}

func TestReplayPublisherRejectsInsufficientRoundsBeforeLock(t *testing.T) {
	fixture := newReplayPublicationFixture(t, true)
	fixture.input.Limits.MaxPublicationRounds = 1
	lockAcquired := false
	fixture.publisher.hooks.afterLock = func() error {
		lockAcquired = true
		return nil
	}
	if _, err := fixture.publish(t); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("round error = %v", err)
	}
	if lockAcquired {
		t.Fatal("insufficient round budget acquired the campaign lock")
	}
	if _, err := os.Stat(fixture.receipt); !os.IsNotExist(err) {
		t.Fatalf("receipt exists after round stop: %v", err)
	}
}

func TestReplayPublisherPreLockBudgetFailureDoesNotAcquireLock(t *testing.T) {
	fixture := newReplayPublicationFixture(t, false)
	fixture.input.Limits.MaxObservationRequests = 1
	lockAcquired := false
	fixture.publisher.hooks.afterLock = func() error {
		lockAcquired = true
		return nil
	}
	if _, err := fixture.publish(t); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("pre-lock budget failure = %v, want ErrResourceLimit", err)
	}
	if lockAcquired {
		t.Fatal("campaign lock was acquired after pre-lock budget failure")
	}
}

type lockCheckingReceiptStore struct {
	lockPath string
	saved    bool
}

func (s *lockCheckingReceiptStore) SaveNoClobber(_ context.Context, path string, receipt ReplayVerificationReceipt) error {
	second, err := AcquirePublicationLock(s.lockPath)
	if second != nil {
		_ = second.Close()
	}
	if !errors.Is(err, ErrPublicationLock) {
		return fmt.Errorf("receipt save occurred without held campaign lock: %v", err)
	}
	s.saved = true
	return SaveReplayVerificationReceipt(path, receipt)
}

func TestReplayPublisherHoldsLockThroughReceiptSave(t *testing.T) {
	fixture := newReplayPublicationFixture(t, true)
	store := &lockCheckingReceiptStore{lockPath: fixture.publisher.lockPath}
	fixture.publisher.receiptStore = store
	if _, err := fixture.publish(t); err != nil {
		t.Fatal(err)
	}
	if !store.saved {
		t.Fatal("receipt store was not called")
	}
}

func TestReplayPublisherEventMissingAndDuplicateDoNotAuthorizeActions(t *testing.T) {
	fixture := newReplayPublicationFixture(t, true)
	fixture.publisher.events = nil
	if _, err := fixture.publish(t); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.publish(t); err != nil {
		t.Fatal(err)
	}
	copies, _ := fixture.tool.counts()
	if copies != 1 {
		t.Fatalf("missing events changed remote-derived actions: copies=%d", copies)
	}
}

type failingReplayEventStore struct{ err error }

func (s failingReplayEventStore) Append(context.Context, ReplayPublicationBundle, ReplayPublicationEvent) error {
	return s.err
}

func (s failingReplayEventStore) Load(context.Context, ReplayPublicationBundle) ([]ReplayPublicationEvent, error) {
	return nil, errors.New("diagnostic event state must not be loaded as authority")
}

func TestReplayPublisherEventConflictAndTimeoutAreNonAuthority(t *testing.T) {
	for _, eventErr := range []error{errors.New("diagnostic event conflict"), context.DeadlineExceeded} {
		fixture := newReplayPublicationFixture(t, true)
		fixture.publisher.events = failingReplayEventStore{err: eventErr}
		receipt, err := fixture.publish(t)
		if err != nil || !receipt.VerificationComplete {
			t.Fatalf("diagnostic event failure affected publication: receipt=%+v err=%v", receipt, err)
		}
	}
}

func TestReplayPublisherEmptyDayPublishesOnlyReplayManifest(t *testing.T) {
	fixture := newReplayPublicationFixture(t, true)
	receipt, err := fixture.publish(t)
	if err != nil {
		t.Fatal(err)
	}
	if !receipt.VerificationComplete {
		t.Fatal("empty day receipt is incomplete")
	}
	copies, _ := fixture.tool.counts()
	if copies != 1 {
		t.Fatalf("empty day copy count = %d", copies)
	}
}

func replayPublicationDataRow(scope protocol.ReplayScope, sequence uint64) protocol.ReplayRow {
	rawObjectHash := sha256.Sum256([]byte("replay-publisher-fixture-raw-object"))
	record := protocol.RawMqlTickV1{
		Time: 1_710_000_000 + int64(sequence), TimeMSC: 1_710_000_000_000 + int64(sequence),
		BidBits: sequence + 10, AskBits: sequence + 20, CaptureSequence: sequence,
	}
	fingerprint := protocol.SourcePayloadFingerprint(record)
	return protocol.ReplayRow{Kind: protocol.ReplayRowData, Data: &protocol.ReplayDataRow{
		Scope: scope, StreamSequence: sequence, ContinuitySegmentID: "replay-publisher-segment",
		RawObjectKey: protocol.RawWALObjectKey(rawObjectHash), RawObjectSHA256: rawObjectHash,
		GatewayIngestSequence: sequence + 1, ProducerInstanceID: "replay-publisher-instance",
		ProducerSessionID: "replay-publisher-session", BatchSequence: 1, RecordOrdinal: uint32(sequence), CaptureSequence: sequence,
		Record: record, SourcePayloadFingerprint: fingerprint,
		ObservationHash: protocol.ObservationHash("replay-publisher-instance", "replay-publisher-session", 1, uint32(sequence), sequence, fingerprint),
	}}
}

func hashString(value []byte) [32]byte {
	return sha256.Sum256(value)
}
