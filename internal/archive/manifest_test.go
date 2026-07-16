package archive_test

import (
	"encoding/json"
	"errors"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"tick-data-platform/internal/archive"
	"tick-data-platform/internal/protocol"
	"tick-data-platform/internal/wal"
)

func TestBuildRawDayManifestSelectsRangesAndZeroBatchSentinel(t *testing.T) {
	day := time.Date(2024, 3, 9, 0, 0, 0, 0, time.UTC)
	selected := day.Add(100 * time.Millisecond).UnixMilli()
	nextDay := day.Add(24*time.Hour + 100*time.Millisecond).UnixMilli()
	frame, err := encodeTestBatch(protocol.BatchFrameV1{
		RequestedFromMSC: day.UnixMilli(),
		ReturnedCount:    3,
		SourceSchemaID:   protocol.SourceSchemaMT5,
		Records: []protocol.RawMqlTickV1{
			{TimeMSC: selected, CaptureSequence: 10},
			{TimeMSC: nextDay, CaptureSequence: 11},
			{TimeMSC: selected + 200, CaptureSequence: 12},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	zeroFrame, err := encodeTestBatch(protocol.BatchFrameV1{
		RequestedFromMSC: day.UnixMilli(),
		ReturnedCount:    -1,
		CopyTicksError:   4,
		SourceSchemaID:   protocol.SourceSchemaMT5,
	})
	if err != nil {
		t.Fatal(err)
	}
	object := promoteTestObject(t, frame, zeroFrame)
	scope := testScope()
	input := archive.RawDayManifestInput{
		Scope:              scope,
		Date:               "2024-03-09",
		RawObjects:         []archive.RawObject{object},
		TerminalSyncStatus: "complete",
		CompletenessStatus: "settled_snapshot",
		LogicalCloseTimeS:  day.Add(25 * time.Hour).Unix(),
	}
	manifest, err := archive.BuildRawDayManifest(input)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Revision != 1 || manifest.AcceptedRecordCount != 2 || manifest.ErrorCount != 1 {
		t.Fatalf("unexpected manifest counts: %+v", manifest)
	}
	if len(manifest.Objects) != 3 {
		t.Fatalf("manifest object ranges = %d, want 3", len(manifest.Objects))
	}
	if manifest.Objects[0].StartIngestSequence != 1 || manifest.Objects[0].FirstRecordOrdinal != 0 || manifest.Objects[1].FirstRecordOrdinal != 2 {
		t.Fatalf("same-entry disjoint ranges were not preserved: %+v", manifest.Objects)
	}
	if manifest.Objects[2].StartIngestSequence != 2 || manifest.Objects[2].FirstRecordOrdinal != 0 || manifest.Objects[2].LastRecordOrdinal != 0 {
		t.Fatalf("zero-record sentinel range is wrong: %+v", manifest.Objects[2])
	}
	if manifest.ChainSliceStartSequence != 1 || manifest.ChainSliceEndSequence != 2 {
		t.Fatalf("chain slice = %d..%d, want 1..2", manifest.ChainSliceStartSequence, manifest.ChainSliceEndSequence)
	}
	canonical, err := archive.ManifestCanonicalJSON(manifest)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := archive.VerifyRawDayManifest(canonical)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.ManifestSHA256 != manifest.ManifestSHA256 {
		t.Fatal("manifest digest did not round-trip")
	}
	second, err := archive.BuildRawDayManifest(input)
	if err != nil {
		t.Fatal(err)
	}
	if second.ManifestSHA256 != manifest.ManifestSHA256 {
		t.Fatal("same verified WAL input did not produce a deterministic manifest")
	}
	coverage, err := archive.VerifyRawDaySegmentCoverage(manifest, object.Segment, object.Key, object.SHA256, uint64(object.Bytes), scope)
	if err != nil || len(coverage.SelectedRanges) != 3 || coverage.AcceptedRecordCount != 2 || coverage.ErrorCount != 1 {
		t.Fatalf("segment semantic coverage = %+v, err=%v", coverage, err)
	}
	tampered := manifest
	tampered.Objects = append([]archive.RawObjectRange(nil), manifest.Objects[:2]...)
	tampered.RawSetRoot, err = archive.RawSetRoot(tampered.Objects)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := archive.VerifyRawDaySegmentCoverage(tampered, object.Segment, object.Key, object.SHA256, uint64(object.Bytes), scope); err == nil {
		t.Fatal("segment semantic coverage accepted a missing selected range")
	}
	if report, err := archive.VerifyRawDaySnapshotSegments(manifest, []archive.RawObject{object}, scope); err != nil || report.AcceptedRecordCount != manifest.AcceptedRecordCount || report.ErrorCount != manifest.ErrorCount {
		t.Fatalf("full remote semantic report = %+v, err=%v", report, err)
	}
	tamperedSummary := manifest
	tamperedSummary.AcceptedRecordCount++
	if _, err := archive.VerifyRawDaySnapshotSegments(tamperedSummary, []archive.RawObject{object}, scope); err == nil {
		t.Fatal("full remote semantic verification accepted a forged aggregate count")
	}
}

func TestBuildRawDayManifestRevisionChainAndScopeDescriptor(t *testing.T) {
	object := promoteTestObject(t, testFrame(t, time.Date(2024, 3, 9, 0, 0, 1, 0, time.UTC).UnixMilli(), 1), nil)
	scope := testScope()
	base := archive.RawDayManifestInput{
		Scope:              scope,
		Date:               "2024-03-09",
		RawObjects:         []archive.RawObject{object},
		TerminalSyncStatus: "complete",
		CompletenessStatus: "provisional",
		LogicalCloseTimeS:  1710003600,
	}
	first, err := archive.BuildRawDayManifest(base)
	if err != nil {
		t.Fatal(err)
	}
	second, err := archive.BuildRawDayManifest(archive.RawDayManifestInput{
		Scope:              scope,
		Date:               base.Date,
		RawObjects:         base.RawObjects,
		Revision:           2,
		Previous:           &first,
		TerminalSyncStatus: base.TerminalSyncStatus,
		CompletenessStatus: "settled_snapshot",
		LogicalCloseTimeS:  base.LogicalCloseTimeS,
	})
	if err != nil {
		t.Fatal(err)
	}
	if second.PreviousManifestSHA256 == nil || *second.PreviousManifestSHA256 != first.ManifestSHA256 {
		t.Fatal("revision chain did not bind the previous manifest digest")
	}
	if _, err := archive.BuildRawDayManifest(archive.RawDayManifestInput{
		Scope:              scope,
		Date:               base.Date,
		RawObjects:         base.RawObjects,
		Revision:           4,
		Previous:           &first,
		TerminalSyncStatus: base.TerminalSyncStatus,
		CompletenessStatus: base.CompletenessStatus,
		LogicalCloseTimeS:  base.LogicalCloseTimeS,
	}); err == nil {
		t.Fatal("revision chain accepted a non-successor revision")
	}

	descriptorRoot := t.TempDir()
	if _, err := archive.EnsureCampaignScopeDescriptor(descriptorRoot, scope); err != nil {
		t.Fatal(err)
	}
	if _, err := archive.EnsureCampaignScopeDescriptor(descriptorRoot, scope); err != nil {
		t.Fatal(err)
	}
	different := scope
	different.ExactSourceSymbol = "EURUSD"
	if _, err := archive.EnsureCampaignScopeDescriptor(descriptorRoot, different); !errors.Is(err, archive.ErrIntegrity) {
		t.Fatalf("different scope content error = %v, want ErrIntegrity", err)
	}
}

func TestBuildRawDayManifestRejectsDiscontinuousCampaignSegments(t *testing.T) {
	first := promoteTestObject(t, testFrame(t, time.Date(2024, 3, 9, 0, 0, 1, 0, time.UTC).UnixMilli(), 1), nil)
	second := promoteTestObject(t, testFrame(t, time.Date(2024, 3, 9, 0, 0, 2, 0, time.UTC).UnixMilli(), 2), nil)
	input := archive.RawDayManifestInput{
		Scope:              testScope(),
		Date:               "2024-03-09",
		RawObjects:         []archive.RawObject{first, second},
		TerminalSyncStatus: "complete",
		CompletenessStatus: "provisional",
		LogicalCloseTimeS:  1710003600,
	}
	if _, err := archive.BuildRawDayManifest(input); !errors.Is(err, archive.ErrIntegrity) {
		t.Fatalf("discontinuous campaign error = %v, want ErrIntegrity", err)
	}
}

func TestRawDayManifestChainObjectsContainCrossDayMiddleObject(t *testing.T) {
	dayA := time.Date(2024, 3, 9, 0, 0, 1, 0, time.UTC).UnixMilli()
	dayB := time.Date(2024, 3, 10, 0, 0, 1, 0, time.UTC).UnixMilli()
	objects := promoteCampaignObjects(t, []int64{dayA, dayB, dayA})
	input := archive.RawDayManifestInput{
		Scope:              testScope(),
		Date:               "2024-03-09",
		RawObjects:         objects,
		TerminalSyncStatus: "complete",
		CompletenessStatus: "settled_snapshot",
		LogicalCloseTimeS:  1710003600,
	}
	manifest, err := archive.BuildRawDayManifest(input)
	if err != nil {
		t.Fatal(err)
	}
	if len(manifest.ChainObjects) != 3 || len(manifest.Objects) != 2 {
		t.Fatalf("chain objects=%d selected objects=%d, want 3 and 2", len(manifest.ChainObjects), len(manifest.Objects))
	}
	if manifest.ChainObjects[1].Key == manifest.Objects[0].Key || manifest.ChainObjects[1].Key == manifest.Objects[1].Key {
		t.Fatal("middle cross-day chain object was incorrectly included in selected ranges")
	}
	paths := make(map[string]string, len(objects))
	for _, object := range objects {
		paths[object.Key] = object.Path
	}
	if err := archive.VerifyRawDaySnapshot(manifest, paths, testScope()); err != nil {
		t.Fatalf("VerifyRawDaySnapshot = %v", err)
	}

	first, err := archive.BuildRawDayManifest(inputForObjects(input, objects[:1], nil))
	if err != nil {
		t.Fatal(err)
	}
	extended, err := archive.BuildRawDayManifest(inputForObjects(input, objects, &first))
	if err != nil {
		t.Fatalf("revision chain extension = %v", err)
	}
	if len(extended.ChainObjects) != 3 || extended.ChainObjects[0] != first.ChainObjects[0] || len(extended.Objects) < len(first.Objects) {
		t.Fatal("revision chain did not preserve the previous chain and object prefixes")
	}
	badPrevious := first
	badPrevious.ChainObjects = append([]archive.RawChainObject(nil), first.ChainObjects...)
	badPrevious.ChainObjects[0].EndIngestSequence++
	if _, err := archive.BuildRawDayManifest(inputForObjects(input, objects, &badPrevious)); !errors.Is(err, archive.ErrIntegrity) {
		t.Fatalf("revision chain prefix violation error = %v, want ErrIntegrity", err)
	}

	missingMiddle := append([]archive.RawObject(nil), objects[:1]...)
	missingMiddle = append(missingMiddle, objects[2])
	if _, err := archive.BuildRawDayManifest(inputForObjects(input, missingMiddle, nil)); !errors.Is(err, archive.ErrIntegrity) {
		t.Fatalf("missing middle object error = %v, want ErrIntegrity", err)
	}
}

func TestVerifyRawDaySnapshotRejectsMissingTamperedFalseBoundaryAndCrossArray(t *testing.T) {
	day := time.Date(2024, 3, 9, 0, 0, 1, 0, time.UTC).UnixMilli()
	objects := promoteCampaignObjects(t, []int64{day, day + 1000, day})
	input := archive.RawDayManifestInput{
		Scope:              testScope(),
		Date:               "2024-03-09",
		RawObjects:         objects,
		TerminalSyncStatus: "complete",
		CompletenessStatus: "settled_snapshot",
		LogicalCloseTimeS:  1710003600,
	}
	manifest, err := archive.BuildRawDayManifest(input)
	if err != nil {
		t.Fatal(err)
	}
	paths := make(map[string]string, len(objects))
	for _, object := range objects {
		paths[object.Key] = object.Path
	}
	missing := make(map[string]string, len(paths))
	for key, path := range paths {
		if key != objects[1].Key {
			missing[key] = path
		}
	}
	if err := archive.VerifyRawDaySnapshot(manifest, missing, testScope()); !errors.Is(err, archive.ErrIntegrity) {
		t.Fatalf("missing chain object error = %v, want ErrIntegrity", err)
	}
	falseBoundary := make(map[string]string, len(paths))
	for key, path := range paths {
		falseBoundary[key] = path
	}
	falseBoundary[objects[1].Key] = objects[0].Path
	if err := archive.VerifyRawDaySnapshot(manifest, falseBoundary, testScope()); !errors.Is(err, archive.ErrIntegrity) {
		t.Fatalf("false segment boundary error = %v, want ErrIntegrity", err)
	}
	tampered := filepath.Join(t.TempDir(), "tampered.rtw")
	data, err := os.ReadFile(objects[1].Path)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)/2] ^= 0xff
	if err := os.WriteFile(tampered, data, 0o600); err != nil {
		t.Fatal(err)
	}
	tamperedPaths := make(map[string]string, len(paths))
	for key, path := range paths {
		tamperedPaths[key] = path
	}
	tamperedPaths[objects[1].Key] = tampered
	if err := archive.VerifyRawDaySnapshot(manifest, tamperedPaths, testScope()); !errors.Is(err, archive.ErrIntegrity) {
		t.Fatalf("tampered object error = %v, want ErrIntegrity", err)
	}
	crossArray := manifest
	crossArray.ChainObjects = append([]archive.RawChainObject(nil), manifest.ChainObjects...)
	crossArray.ChainObjects[1].Bytes++
	if err := archive.ValidateRawDayManifest(crossArray); err == nil {
		t.Fatal("cross-array metadata mismatch was accepted")
	}
}

func TestVerifyRawDaySnapshotUsesScopedRecordLimit(t *testing.T) {
	day := time.Date(2024, 3, 9, 0, 0, 1, 0, time.UTC).UnixMilli()
	frame, err := encodeTestBatch(protocol.BatchFrameV1{
		RequestedFromMSC: day,
		ReturnedCount:    2,
		SourceSchemaID:   protocol.SourceSchemaMT5,
		Records: []protocol.RawMqlTickV1{
			{TimeMSC: day, CaptureSequence: 1},
			{TimeMSC: day + 1, CaptureSequence: 2},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	scope := testScope()
	object := promoteTestObject(t, frame)
	manifest, err := archive.BuildRawDayManifest(archive.RawDayManifestInput{
		Scope:              scope,
		Date:               "2024-03-09",
		RawObjects:         []archive.RawObject{object},
		TerminalSyncStatus: "complete",
		CompletenessStatus: "settled_snapshot",
		LogicalCloseTimeS:  1710003600,
	})
	if err != nil {
		t.Fatal(err)
	}
	strictScope := scope
	strictScope.ProtocolLimits.MaxRecords = 1
	manifest.ConfigHash, err = strictScope.ConfigHash()
	if err != nil {
		t.Fatal(err)
	}
	manifest.ManifestSHA256, err = archive.ManifestDigest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	paths := map[string]string{object.Key: object.Path}
	if err := archive.VerifyRawDaySnapshot(manifest, paths, strictScope); !errors.Is(err, archive.ErrIntegrity) {
		t.Fatalf("scoped record limit violation error = %v, want ErrIntegrity", err)
	}
}

func TestRawDayManifestRejectsForgedAndTraversalObjectKeys(t *testing.T) {
	object := promoteTestObject(t, testFrame(t, time.Date(2024, 3, 9, 0, 0, 1, 0, time.UTC).UnixMilli(), 1))
	for _, key := range []string{"objects/raw/wal-" + strings.Repeat("0", 64) + ".rtw", "../objects/raw/wal-x.rtw", "objects\\raw\\wal-x.rtw", "C:\\objects\\raw\\wal-x.rtw"} {
		forged := object
		forged.Key = key
		input := archive.RawDayManifestInput{
			Scope:              testScope(),
			Date:               "2024-03-09",
			RawObjects:         []archive.RawObject{forged},
			TerminalSyncStatus: "complete",
			CompletenessStatus: "provisional",
			LogicalCloseTimeS:  1710003600,
		}
		if _, err := archive.BuildRawDayManifest(input); !errors.Is(err, archive.ErrIntegrity) {
			t.Fatalf("forged key %q error = %v, want ErrIntegrity", key, err)
		}
	}
}

func TestVerifyGoldenRawDayManifest(t *testing.T) {
	for _, name := range []string{"raw-day-manifest-v1.json", "raw-day-manifest-chain-slice-v1.json"} {
		path := filepath.Join("..", "..", "testdata", "tickdata", "golden", name)
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		manifestFixture, err := decodeFixtureCanonicalJSON(data)
		if err != nil {
			t.Fatal(err)
		}
		manifest, err := archive.VerifyRawDayManifest([]byte(manifestFixture))
		if err != nil {
			t.Fatal(err)
		}
		if manifest.Revision != 1 || manifest.ManifestSHA256 == ([32]byte{}) {
			t.Fatalf("golden raw-day manifest was not verified: %+v", manifest)
		}
	}
}

func testScope() archive.ScopeConfig {
	return archive.ScopeConfig{
		DatasetID:               "dataset-demo",
		CampaignID:              "campaign-demo",
		ProviderID:              "provider-demo",
		StableFeedID:            "feed-demo",
		ExactSourceSymbol:       "EURUSD.raw",
		BrokerServerFingerprint: "server-fingerprint",
		GatewayBuildIdentity:    "gateway-build-1",
		ProducerBuildIdentity:   "producer-build-1",
		DayDefinitionID:         "utc-day-v1",
		SettlePolicy:            "manual-v1",
		PublisherID:             "publisher-1",
		PublisherEpoch:          1,
	}
}

func inputForObjects(base archive.RawDayManifestInput, objects []archive.RawObject, previous *archive.RawDayManifest) archive.RawDayManifestInput {
	base.RawObjects = objects
	base.Previous = previous
	if previous != nil {
		base.CompletenessStatus = "settled_snapshot"
	}
	return base
}

func promoteCampaignObjects(t *testing.T, times []int64) []archive.RawObject {
	t.Helper()
	root := t.TempDir()
	outbox := t.TempDir()
	store, err := wal.Open(root, "gateway-test-01")
	if err != nil {
		t.Fatal(err)
	}
	objects := make([]archive.RawObject, 0, len(times))
	for i, timeMSC := range times {
		frame := testFrame(t, timeMSC, uint64(i+1))
		if _, err := store.Append(frame, 1710000000+int64(i), uint64(100+i)); err != nil {
			_ = store.Close()
			t.Fatal(err)
		}
		sealed, err := store.Seal()
		if err != nil {
			_ = store.Close()
			t.Fatal(err)
		}
		object, err := archive.PromoteSealedSegment(outbox, sealed.Path)
		if err != nil {
			_ = store.Close()
			t.Fatal(err)
		}
		objects = append(objects, object)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	return objects
}

func testFrame(t *testing.T, timeMSC int64, captureSequence uint64) []byte {
	t.Helper()
	frame, err := encodeTestBatch(protocol.BatchFrameV1{
		RequestedFromMSC: timeMSC,
		ReturnedCount:    1,
		SourceSchemaID:   protocol.SourceSchemaMT5,
		Records: []protocol.RawMqlTickV1{{
			TimeMSC:         timeMSC,
			CaptureSequence: captureSequence,
			BidBits:         math.Float64bits(1.1),
			AskBits:         math.Float64bits(1.2),
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return frame
}

func encodeTestBatch(batch protocol.BatchFrameV1) ([]byte, error) {
	if batch.SourceSchemaID == "" {
		batch.SourceSchemaID = protocol.SourceSchemaMT5
	}
	if batch.ReturnedCount == 0 && len(batch.Records) > 0 {
		batch.ReturnedCount = int32(len(batch.Records))
	}
	return protocol.EncodeMessage(batch)
}

func decodeFixtureCanonicalJSON(data []byte) (string, error) {
	var fixture struct {
		CanonicalJSON string `json:"canonical_json"`
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		return "", err
	}
	return fixture.CanonicalJSON, nil
}

func promoteTestObject(t *testing.T, frames ...[]byte) archive.RawObject {
	t.Helper()
	root := t.TempDir()
	store, err := wal.Open(root, "gateway-test-01")
	if err != nil {
		t.Fatal(err)
	}
	for i, frame := range frames {
		if frame == nil {
			continue
		}
		if _, err := store.Append(frame, 1710000000+int64(i), uint64(100+i)); err != nil {
			_ = store.Close()
			t.Fatal(err)
		}
	}
	sealed, err := store.Seal()
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	object, err := archive.PromoteSealedSegment(t.TempDir(), sealed.Path)
	if err != nil {
		t.Fatal(err)
	}
	return object
}
