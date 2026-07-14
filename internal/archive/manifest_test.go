package archive_test

import (
	"encoding/json"
	"errors"
	"math"
	"os"
	"path/filepath"
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

func TestVerifyGoldenRawDayManifest(t *testing.T) {
	path := filepath.Join("..", "..", "testdata", "tickdata", "golden", "raw-day-manifest-v1.json")
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
