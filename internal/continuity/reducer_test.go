package continuity_test

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"tick-data-platform/internal/archive"
	"tick-data-platform/internal/continuity"
	"tick-data-platform/internal/protocol"
	"tick-data-platform/internal/wal"
)

type rawFixture struct {
	scope    archive.ScopeConfig
	manifest archive.RawDayManifest
	bytes    []byte
	paths    map[string]string
	limits   archive.ReplayResourceLimits
}

const testProducerInstanceID = "producer-1"

func TestReduceUsesVerifiedM2RawDayAndPreservesExactOverlap(t *testing.T) {
	a := tick(1000, 1, 10)
	b := tick(1010, 2, 20)
	c := tick(1020, 3, 30)
	fixture := buildFixture(t, []protocol.BatchFrameV1{
		batch("session-1", 1, a, b),
		batch("session-1", 2, b, c),
	})
	result, rows := reduceFixture(t, fixture)
	if result.RowCount != 4 || result.MarkerCount != 1 {
		t.Fatalf("summary = %+v, want one marker and three data rows", result)
	}
	if dataCount(rows) != 3 || markerCount(rows, protocol.MarkerSegmentStart) != 1 {
		t.Fatalf("rows=%d markers=%v", dataCount(rows), markerCodes(rows))
	}
	for _, row := range rows {
		if row.Kind == protocol.ReplayRowData && row.Data.ProducerInstanceID != testProducerInstanceID {
			t.Fatalf("producer instance = %q, want %q", row.Data.ProducerInstanceID, testProducerInstanceID)
		}
	}
	if result.RowChainRoot == ([32]byte{}) {
		t.Fatal("non-empty replay has zero row-chain root")
	}
}

func TestReduceAmbiguousRepeatedPatternPreservesOccurrences(t *testing.T) {
	a := tick(1000, 1, 10)
	fixture := buildFixture(t, []protocol.BatchFrameV1{
		batch("session-1", 1, a, a),
		batch("session-1", 2, a),
	})
	_, rows := reduceFixture(t, fixture)
	if dataCount(rows) != 3 {
		t.Fatalf("data rows=%d, want all repeated occurrences", dataCount(rows))
	}
	if markerCount(rows, protocol.MarkerAmbiguousOverlap) != 1 {
		t.Fatalf("markers=%v, want ambiguous overlap", markerCodes(rows))
	}
}

func TestReduceAcceptsLongestUniqueOverlapWithMultiplicity(t *testing.T) {
	a := tick(1000, 1, 10)
	b := tick(1010, 2, 20)
	fixture := buildFixture(t, []protocol.BatchFrameV1{
		batch("session-1", 1, a, a),
		batch("session-1", 2, a, a, b),
	})
	_, rows := reduceFixture(t, fixture)
	if dataCount(rows) != 3 {
		t.Fatalf("data rows=%d, want two retained A occurrences and one B", dataCount(rows))
	}
	if markerCount(rows, protocol.MarkerAmbiguousOverlap) != 0 {
		t.Fatalf("markers=%v, shorter overlap must not make the longest overlap ambiguous", markerCodes(rows))
	}
}

func TestReduceSessionRestartAndChangedPayloadAreAmbiguous(t *testing.T) {
	first := tick(1000, 1, 10)
	changed := tick(1000, 1, 99)
	fixture := buildFixture(t, []protocol.BatchFrameV1{
		batch("session-1", 1, first),
		batch("session-2", 1, changed),
	})
	_, rows := reduceFixture(t, fixture)
	if dataCount(rows) != 2 || markerCount(rows, protocol.MarkerAmbiguousOverlap) != 1 {
		t.Fatalf("rows=%d markers=%v, want both payloads and one ambiguity", dataCount(rows), markerCodes(rows))
	}
	if markerCount(rows, protocol.MarkerSourceHistoryChanged) != 0 {
		t.Fatal("capture_sequence/time_msc guessed a source history change")
	}
}

func TestReduceReportsProvenSourceHistoryChange(t *testing.T) {
	a := tick(1000, 1, 10)
	b := tick(1000, 2, 20)
	c := tick(1000, 3, 30)
	changed := tick(1000, 4, 99)
	d := tick(1000, 5, 40)
	fixture := buildFixture(t, []protocol.BatchFrameV1{
		batch("session-1", 1, a, b, c),
		batch("session-1", 2, a, changed, c, d),
	})
	_, rows := reduceFixture(t, fixture)
	if dataCount(rows) != 7 {
		t.Fatalf("data rows=%d, want all original and changed occurrences", dataCount(rows))
	}
	if markerCount(rows, protocol.MarkerSourceHistoryChanged) != 1 {
		t.Fatalf("markers=%v, want one proven source history change", markerCodes(rows))
	}
	if markerCount(rows, protocol.MarkerAmbiguousOverlap) != 0 {
		t.Fatalf("markers=%v, exact surrounding evidence was unique", markerCodes(rows))
	}
}

func TestReducePrefersLongestBoundaryHistoryAlignment(t *testing.T) {
	a := tick(1000, 1, 10)
	changed := tick(1000, 2, 99)
	fixture := buildFixture(t, []protocol.BatchFrameV1{
		batch("session-1", 1, a, a, a, a),
		batch("session-1", 2, a, changed, a, a),
	})
	_, rows := reduceFixture(t, fixture)
	if dataCount(rows) != 8 {
		t.Fatalf("data rows=%d, want all occurrences after the longest boundary proof", dataCount(rows))
	}
	if markerCount(rows, protocol.MarkerSourceHistoryChanged) != 1 {
		t.Fatalf("markers=%v, want one longest-boundary history change", markerCodes(rows))
	}
	if markerCount(rows, protocol.MarkerAmbiguousOverlap) != 0 {
		t.Fatalf("markers=%v, shorter nested alignment must not create ambiguity", markerCodes(rows))
	}
}

func TestReduceRejectsRepeatedWildcardCompatibleHistoryPositions(t *testing.T) {
	a := tick(1000, 1, 10)
	changed := tick(1000, 2, 99)
	fixture := buildFixture(t, []protocol.BatchFrameV1{
		batch("session-1", 1, a, a, a, a),
		batch("session-1", 2, a, changed, a),
	})
	_, rows := reduceFixture(t, fixture)
	if dataCount(rows) != 7 {
		t.Fatalf("data rows=%d, want every occurrence after ambiguous proof", dataCount(rows))
	}
	if markerCount(rows, protocol.MarkerSourceHistoryChanged) != 0 {
		t.Fatalf("markers=%v, repeated wildcard-compatible positions must not be history-changed", markerCodes(rows))
	}
	if markerCount(rows, protocol.MarkerAmbiguousOverlap) != 1 {
		t.Fatalf("markers=%v, want ambiguous overlap", markerCodes(rows))
	}
}

func TestReduceDoesNotUseInteriorHistoryWindowWithoutBoundaryProof(t *testing.T) {
	a := tick(1000, 1, 10)
	b := tick(1000, 2, 20)
	c := tick(1000, 3, 30)
	oldTailEnd := tick(1000, 4, 40)
	changed := tick(1000, 5, 99)
	newTailEnd := tick(1000, 6, 60)
	fixture := buildFixture(t, []protocol.BatchFrameV1{
		batch("session-1", 1, a, b, c, oldTailEnd),
		batch("session-1", 2, a, changed, c, newTailEnd),
	})
	_, rows := reduceFixture(t, fixture)
	if dataCount(rows) != 8 {
		t.Fatalf("data rows=%d, want every occurrence from the non-overlapping boundary", dataCount(rows))
	}
	if markerCount(rows, protocol.MarkerSourceHistoryChanged) != 0 {
		t.Fatalf("markers=%v, interior-only substitution must not be history-changed", markerCodes(rows))
	}
	if markerCount(rows, protocol.MarkerAmbiguousOverlap) != 1 {
		t.Fatalf("markers=%v, want ambiguous overlap at the unproven boundary", markerCodes(rows))
	}
}

func TestReduceRejectsWrongProducerInstanceBeforeSink(t *testing.T) {
	fixture := buildFixture(t, []protocol.BatchFrameV1{batch("session-1", 1, tick(1000, 1, 10))})
	reader, err := archive.OpenVerifiedReplaySource(archive.ReplaySourceInput{
		Scope: fixture.scope, ProducerInstanceID: "wrong-producer", ManifestRelativeKey: manifestKey(t, fixture), ManifestBytes: fixture.bytes,
		ObjectPaths: fixture.paths, ReplayContractID: "replay-v1", ConversionID: "conversion-v1",
		ResourceLimits: fixture.limits,
	})
	if err != nil {
		t.Fatal(err)
	}
	sinkCalls := 0
	if _, err := continuity.Reduce(reader, func(protocol.ReplayRow) error {
		sinkCalls++
		return nil
	}); err == nil {
		t.Fatal("wrong producer instance was accepted")
	}
	if sinkCalls != 0 {
		t.Fatalf("sink calls=%d, wrong producer must fail before row emission", sinkCalls)
	}
}

func TestReduceValidatesDataRowBeforeSink(t *testing.T) {
	a := tick(1000, 1, 10)
	b := tick(1010, 2, 20)
	fixture := buildFixture(t, []protocol.BatchFrameV1{batch("session-1", 1, a)})
	verified, err := archive.OpenVerifiedReplaySource(archive.ReplaySourceInput{
		Scope: fixture.scope, ProducerInstanceID: testProducerInstanceID, ManifestRelativeKey: manifestKey(t, fixture), ManifestBytes: fixture.bytes,
		ObjectPaths: fixture.paths, ReplayContractID: "replay-v1", ConversionID: "conversion-v1",
		ResourceLimits: fixture.limits,
	})
	if err != nil {
		t.Fatal(err)
	}
	firstFrame, err := protocol.EncodeMessage(batch("session-1", 1, a))
	if err != nil {
		t.Fatal(err)
	}
	secondFrame, err := protocol.EncodeMessage(batch("session-1", 2, a, b))
	if err != nil {
		t.Fatal(err)
	}
	object := fixture.manifest.Objects[0]
	fake := &fakeVerifiedReader{
		scope: verified.ReplayScope(), identity: verified.ProducerIdentity(), maxRecords: fixture.scope.ProtocolLimits.MaxRecords,
		batches: []continuity.VerifiedBatch{
			{ObjectKey: object.Key, ObjectSHA256: object.SHA256, GatewayIngestSequence: 1, Frame: firstFrame, SelectedRecordOrdinals: []uint32{0}},
			{ObjectKey: "objects/raw/not-the-hash.rtw", ObjectSHA256: object.SHA256, GatewayIngestSequence: 2, Frame: secondFrame, SelectedRecordOrdinals: []uint32{0, 1}},
		},
	}
	sinkCalls := 0
	if _, err := continuity.Reduce(fake, func(protocol.ReplayRow) error {
		sinkCalls++
		return nil
	}); err == nil {
		t.Fatal("invalid data row was accepted")
	}
	if sinkCalls != 2 {
		t.Fatalf("sink calls=%d, expected only the valid initial marker and data row", sinkCalls)
	}
}

func TestReduceSourceErrorGapAndTimestampRegressionMarkers(t *testing.T) {
	errorBatch := batch("session-1", 2)
	errorBatch.BatchSequence = 3
	errorBatch.CopyTicksError = 7
	fixture := buildFixture(t, []protocol.BatchFrameV1{
		batch("session-1", 1, tick(1000, 1, 10)),
		batch("session-1", 2, tick(900, 2, 20)),
		errorBatch,
		batch("session-1", 5, tick(1100, 5, 50)),
	})
	_, rows := reduceFixture(t, fixture)
	for _, code := range []string{protocol.MarkerSourceError, protocol.MarkerGap, protocol.MarkerTimestampRegression} {
		if markerCount(rows, code) != 1 {
			t.Errorf("marker %s missing from %v", code, markerCodes(rows))
		}
	}
	if dataCount(rows) != 3 {
		t.Fatalf("data rows=%d, want all non-error occurrences", dataCount(rows))
	}
}

func TestReduceRerunIsDeterministicAndSummaryDoesNotRetainRows(t *testing.T) {
	fixture := buildFixture(t, []protocol.BatchFrameV1{
		batch("session-1", 1, tick(1000, 1, 10)),
		batch("session-1", 2, tick(1010, 2, 20)),
	})
	left, leftRows := reduceFixture(t, fixture)
	right, rightRows := reduceFixture(t, fixture)
	if left.RowChainRoot != right.RowChainRoot || !bytes.Equal(flattenCanonical(t, leftRows), flattenCanonical(t, rightRows)) {
		t.Fatal("rerun changed canonical rows or row-chain root")
	}
	if left.RowCount != 4 || left.TailSize > fixture.scope.ProtocolLimits.MaxRecords {
		t.Fatalf("bounded summary = %+v", left)
	}
}

func TestReduceEmptyVerifiedDayHasNoSyntheticRows(t *testing.T) {
	fixture := buildFixture(t, nil)
	result, rows := reduceFixture(t, fixture)
	if result.RowCount != 0 || result.RowChainRoot != ([32]byte{}) || len(rows) != 0 {
		t.Fatalf("empty result=%+v rows=%d", result, len(rows))
	}
}

func TestOpenVerifiedReplaySourceRejectsTrustBoundaryFailures(t *testing.T) {
	fixture := buildFixture(t, []protocol.BatchFrameV1{batch("session-1", 1, tick(1000, 1, 10))})
	key := manifestKey(t, fixture)
	tests := []struct {
		name   string
		mutate func(*rawFixture)
	}{
		{name: "raw key mismatch", mutate: func(f *rawFixture) { f.bytes = append([]byte(nil), f.bytes...); f.scope.CampaignID = "other" }},
		{name: "scope config mismatch", mutate: func(f *rawFixture) { f.scope.ProtocolLimits.MaxRecords = 1 }},
		{name: "missing sealed object", mutate: func(f *rawFixture) { f.paths = map[string]string{} }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mutated := fixture
			mutated.paths = clonePaths(fixture.paths)
			test.mutate(&mutated)
			_, err := archive.OpenVerifiedReplaySource(archive.ReplaySourceInput{
				Scope: mutated.scope, ProducerInstanceID: testProducerInstanceID, ManifestRelativeKey: key, ManifestBytes: mutated.bytes, ObjectPaths: mutated.paths,
				ReplayContractID: "replay-v1", ConversionID: "conversion-v1",
				ResourceLimits: mutated.limits,
			})
			if err == nil {
				t.Fatal("invalid M2 raw input was accepted")
			}
		})
	}

	tampered := fixture
	tampered.paths = clonePaths(fixture.paths)
	for _, path := range tampered.paths {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		data[len(data)/2] ^= 0xff
		mutatedPath := filepath.Join(t.TempDir(), "mutated.rtw")
		if err := os.WriteFile(mutatedPath, data, 0o600); err != nil {
			t.Fatal(err)
		}
		for objectKey := range tampered.paths {
			tampered.paths[objectKey] = mutatedPath
		}
	}
	if _, err := archive.OpenVerifiedReplaySource(archive.ReplaySourceInput{
		Scope: tampered.scope, ProducerInstanceID: testProducerInstanceID, ManifestRelativeKey: key, ManifestBytes: tampered.bytes, ObjectPaths: tampered.paths,
		ReplayContractID: "replay-v1", ConversionID: "conversion-v1",
		ResourceLimits: tampered.limits,
	}); !errors.Is(err, archive.ErrIntegrity) {
		t.Fatalf("sealed WAL mutation error=%v, want ErrIntegrity", err)
	}
	if _, err := archive.OpenVerifiedReplaySource(archive.ReplaySourceInput{
		Scope: fixture.scope, ProducerInstanceID: testProducerInstanceID, ManifestRelativeKey: key + ".wrong", ManifestBytes: fixture.bytes, ObjectPaths: fixture.paths,
		ReplayContractID: "replay-v1", ConversionID: "conversion-v1",
		ResourceLimits: fixture.limits,
	}); !errors.Is(err, archive.ErrIntegrity) {
		t.Fatalf("raw manifest key mismatch error=%v, want ErrIntegrity", err)
	}
	zeroLimit := fixture
	zeroLimit.scope.ProtocolLimits.MaxRecords = 0
	if _, err := archive.OpenVerifiedReplaySource(archive.ReplaySourceInput{
		Scope: zeroLimit.scope, ProducerInstanceID: testProducerInstanceID, ManifestRelativeKey: key, ManifestBytes: zeroLimit.bytes, ObjectPaths: zeroLimit.paths,
		ReplayContractID: "replay-v1", ConversionID: "conversion-v1",
		ResourceLimits: zeroLimit.limits,
	}); !errors.Is(err, archive.ErrIntegrity) {
		t.Fatalf("zero max_records error=%v, want ErrIntegrity", err)
	}
	gap := fixture
	gap.manifest.ChainObjects = append([]archive.RawChainObject(nil), fixture.manifest.ChainObjects...)
	gap.manifest.ChainObjects[0].StartIngestSequence++
	gap.manifest.ManifestSHA256 = [32]byte{}
	gapBytes, err := archive.ManifestCanonicalJSON(gap.manifest)
	if err != nil {
		t.Fatal(err)
	}
	gap.bytes = gapBytes
	if _, err := archive.OpenVerifiedReplaySource(archive.ReplaySourceInput{
		Scope: gap.scope, ProducerInstanceID: testProducerInstanceID, ManifestRelativeKey: manifestKey(t, gap), ManifestBytes: gap.bytes, ObjectPaths: gap.paths,
		ReplayContractID: "replay-v1", ConversionID: "conversion-v1",
		ResourceLimits: gap.limits,
	}); !errors.Is(err, archive.ErrIntegrity) {
		t.Fatalf("chain gap error=%v, want ErrIntegrity", err)
	}
}

func TestOpenVerifiedReplaySourceRejectsReplayResourceLimitsBeforeSink(t *testing.T) {
	fixture := buildFixture(t, []protocol.BatchFrameV1{batch("session-1", 1, tick(1000, 1, 10))})
	if fixture.limits.MaxObjectBytes < 2 || fixture.limits.MaxChainBytes < 2 {
		t.Fatal("fixture object unexpectedly too small for resource-limit tests")
	}
	tests := []struct {
		name   string
		mutate func(*archive.ReplayResourceLimits)
	}{
		{name: "zero chain objects", mutate: func(limits *archive.ReplayResourceLimits) { limits.MaxChainObjects = 0 }},
		{name: "zero object bytes", mutate: func(limits *archive.ReplayResourceLimits) { limits.MaxObjectBytes = 0 }},
		{name: "zero chain bytes", mutate: func(limits *archive.ReplayResourceLimits) { limits.MaxChainBytes = 0 }},
		{name: "object count", mutate: func(limits *archive.ReplayResourceLimits) { limits.MaxChainObjects = 0 }},
		{name: "object bytes", mutate: func(limits *archive.ReplayResourceLimits) { limits.MaxObjectBytes-- }},
		{name: "chain bytes", mutate: func(limits *archive.ReplayResourceLimits) { limits.MaxChainBytes-- }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			limits := fixture.limits
			test.mutate(&limits)
			reader, err := archive.OpenVerifiedReplaySource(archive.ReplaySourceInput{
				Scope: fixture.scope, ProducerInstanceID: testProducerInstanceID, ManifestRelativeKey: manifestKey(t, fixture),
				ManifestBytes: fixture.bytes, ObjectPaths: fixture.paths, ReplayContractID: "replay-v1", ConversionID: "conversion-v1",
				ResourceLimits: limits,
			})
			if err == nil {
				sinkCalls := 0
				_, err = continuity.Reduce(reader, func(protocol.ReplayRow) error {
					sinkCalls++
					return nil
				})
				if sinkCalls != 0 {
					t.Fatalf("sink calls=%d, over-limit verification must fail before sink", sinkCalls)
				}
			}
			if err == nil || !errors.Is(err, archive.ErrIntegrity) {
				t.Fatalf("resource limit error=%v, want ErrIntegrity", err)
			}
		})
	}
	mutated := fixture
	mutated.paths = clonePaths(fixture.paths)
	for key, path := range mutated.paths {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		data = append(data, 0)
		largerPath := filepath.Join(t.TempDir(), "larger.rtw")
		if err := os.WriteFile(largerPath, data, 0o600); err != nil {
			t.Fatal(err)
		}
		mutated.paths[key] = largerPath
	}
	reader, err := archive.OpenVerifiedReplaySource(archive.ReplaySourceInput{
		Scope: mutated.scope, ProducerInstanceID: testProducerInstanceID, ManifestRelativeKey: manifestKey(t, mutated),
		ManifestBytes: mutated.bytes, ObjectPaths: mutated.paths, ReplayContractID: "replay-v1", ConversionID: "conversion-v1",
		ResourceLimits: mutated.limits,
	})
	if err == nil {
		sinkCalls := 0
		_, err = continuity.Reduce(reader, func(protocol.ReplayRow) error {
			sinkCalls++
			return nil
		})
		if sinkCalls != 0 {
			t.Fatalf("sink calls=%d, oversized reverified object must fail before sink", sinkCalls)
		}
	}
	if err == nil || !errors.Is(err, archive.ErrIntegrity) {
		t.Fatalf("oversized reverified object error=%v, want ErrIntegrity", err)
	}
}

func TestOpenVerifiedReplaySourceRejectsArbitraryManifestRoot(t *testing.T) {
	fixture := buildFixture(t, []protocol.BatchFrameV1{batch("session-1", 1, tick(1000, 1, 10))})
	key := manifestKey(t, fixture)
	if _, err := archive.OpenVerifiedReplaySource(archive.ReplaySourceInput{
		Scope: fixture.scope, ProducerInstanceID: testProducerInstanceID, ManifestRelativeKey: "immutable-root/" + key,
		ManifestBytes: fixture.bytes, ObjectPaths: fixture.paths, ReplayContractID: "replay-v1", ConversionID: "conversion-v1",
		ResourceLimits: fixture.limits,
	}); !errors.Is(err, archive.ErrIntegrity) {
		t.Fatalf("arbitrary root error=%v, want ErrIntegrity", err)
	}
}

func TestOpenVerifiedReplaySourceExcludesChainOnlyCrossDayEntry(t *testing.T) {
	dayA := time.Date(2024, 3, 9, 0, 0, 1, 0, time.UTC).UnixMilli()
	dayB := time.Date(2024, 3, 10, 0, 0, 1, 0, time.UTC).UnixMilli()
	fixture := buildFixture(t, []protocol.BatchFrameV1{
		batch("session-1", 1, tick(dayA, 1, 10)),
		batch("session-1", 2, tick(dayB, 2, 20)),
		batch("session-1", 3, tick(dayA+1000, 3, 30)),
	})
	fixture.manifest.Date = "2024-03-09"
	// Rebuild the manifest with the actual target day; the fixture helper's
	// default date is the same, so only the source times determine selection.
	fixture = rebuildFixtureManifest(t, fixture, "2024-03-09")
	reader, err := archive.OpenVerifiedReplaySource(archive.ReplaySourceInput{
		Scope: fixture.scope, ProducerInstanceID: testProducerInstanceID, ManifestRelativeKey: manifestKey(t, fixture), ManifestBytes: fixture.bytes,
		ObjectPaths: fixture.paths, ReplayContractID: "replay-v1", ConversionID: "conversion-v1",
		ResourceLimits: fixture.limits,
	})
	if err != nil {
		t.Fatal(err)
	}
	var entries []continuity.VerifiedBatch
	for {
		entry, ok, err := reader.Next()
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		entries = append(entries, entry)
	}
	if len(entries) != 2 || entries[0].GatewayIngestSequence != 1 || entries[1].GatewayIngestSequence != 3 {
		t.Fatalf("selected WAL entries=%v, want sequences 1 and 3 only", entries)
	}
}

func TestOpenVerifiedReplaySourceRejectsNonEquivalentCompactRange(t *testing.T) {
	first := []protocol.RawMqlTickV1{tick(1000, 1, 10), tick(1010, 2, 20), tick(1020, 3, 30)}
	second := []protocol.RawMqlTickV1{tick(1030, 4, 40), tick(1040, 5, 50), tick(1050, 6, 60)}
	third := []protocol.RawMqlTickV1{tick(1060, 7, 70), tick(1070, 8, 80), tick(1080, 9, 90)}
	fixture := buildFixture(t, []protocol.BatchFrameV1{
		batch("session-1", 1, first...), batch("session-1", 2, second...), batch("session-1", 3, third...),
	})
	object := fixture.manifest.Objects[0]
	object.StartIngestSequence = 1
	object.EndIngestSequence = 3
	object.FirstRecordOrdinal = 1
	object.LastRecordOrdinal = 1
	fixture.manifest.Objects = []archive.RawObjectRange{object}
	fixture.manifest.AcceptedRecordCount = 7
	fixture.manifest.ObservedThroughSourceMSC = third[1].TimeMSC
	fixture.manifest.ObservedThroughCaptureSeq = third[1].CaptureSequence
	var err error
	fixture.manifest.RawSetRoot, err = archive.RawSetRoot(fixture.manifest.Objects)
	if err != nil {
		t.Fatal(err)
	}
	fixture.manifest.ManifestSHA256 = [32]byte{}
	fixture.bytes, err = archive.ManifestCanonicalJSON(fixture.manifest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := archive.OpenVerifiedReplaySource(archive.ReplaySourceInput{
		Scope: fixture.scope, ProducerInstanceID: testProducerInstanceID,
		ManifestRelativeKey: manifestKey(t, fixture), ManifestBytes: fixture.bytes, ObjectPaths: fixture.paths,
		ReplayContractID: "replay-v1", ConversionID: "conversion-v1",
		ResourceLimits: fixture.limits,
	}); !errors.Is(err, archive.ErrIntegrity) {
		t.Fatalf("non-equivalent compact selection error=%v, want ErrIntegrity", err)
	}
}

func TestOpenVerifiedReplaySourceAcceptsEquivalentCompactRange(t *testing.T) {
	first := []protocol.RawMqlTickV1{tick(1000, 1, 10), tick(1010, 2, 20), tick(1020, 3, 30)}
	second := []protocol.RawMqlTickV1{tick(1030, 4, 40), tick(1040, 5, 50), tick(1050, 6, 60)}
	third := []protocol.RawMqlTickV1{tick(1060, 7, 70), tick(1070, 8, 80), tick(1080, 9, 90)}
	fixture := buildFixture(t, []protocol.BatchFrameV1{
		batch("session-1", 1, first...), batch("session-1", 2, second...), batch("session-1", 3, third...),
	})
	object := fixture.manifest.Objects[0]
	object.StartIngestSequence = 1
	object.EndIngestSequence = 3
	object.FirstRecordOrdinal = 0
	object.LastRecordOrdinal = 2
	fixture.manifest.Objects = []archive.RawObjectRange{object}
	var err error
	fixture.manifest.RawSetRoot, err = archive.RawSetRoot(fixture.manifest.Objects)
	if err != nil {
		t.Fatal(err)
	}
	fixture.manifest.ManifestSHA256 = [32]byte{}
	fixture.bytes, err = archive.ManifestCanonicalJSON(fixture.manifest)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := archive.OpenVerifiedReplaySource(archive.ReplaySourceInput{
		Scope: fixture.scope, ProducerInstanceID: testProducerInstanceID,
		ManifestRelativeKey: manifestKey(t, fixture), ManifestBytes: fixture.bytes, ObjectPaths: fixture.paths,
		ReplayContractID: "replay-v1", ConversionID: "conversion-v1", ResourceLimits: fixture.limits,
	})
	if err != nil {
		t.Fatal(err)
	}
	var coordinates []string
	for {
		entry, ok, err := reader.Next()
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		for _, ordinal := range entry.SelectedRecordOrdinals {
			coordinates = append(coordinates, fmt.Sprintf("%d/%d", entry.GatewayIngestSequence, ordinal))
		}
	}
	want := []string{"1/0", "1/1", "1/2", "2/0", "2/1", "2/2", "3/0", "3/1", "3/2"}
	if !reflect.DeepEqual(coordinates, want) {
		t.Fatalf("selected coordinates=%v, want %v", coordinates, want)
	}
}

func TestOpenVerifiedReplaySourceExpandsZeroSentinelAndSourceError(t *testing.T) {
	errorBatch := batch("session-1", 2, tick(1010, 2, 20))
	errorBatch.CopyTicksError = 7
	fixture := buildFixture(t, []protocol.BatchFrameV1{
		batch("session-1", 1), errorBatch, batch("session-1", 3, tick(1020, 3, 30)),
	})
	object := fixture.manifest.Objects[0]
	object.StartIngestSequence = 1
	object.EndIngestSequence = 3
	object.FirstRecordOrdinal = 0
	object.LastRecordOrdinal = 0
	fixture.manifest.Objects = []archive.RawObjectRange{object}
	var err error
	fixture.manifest.RawSetRoot, err = archive.RawSetRoot(fixture.manifest.Objects)
	if err != nil {
		t.Fatal(err)
	}
	fixture.manifest.ManifestSHA256 = [32]byte{}
	fixture.bytes, err = archive.ManifestCanonicalJSON(fixture.manifest)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := archive.OpenVerifiedReplaySource(archive.ReplaySourceInput{
		Scope: fixture.scope, ProducerInstanceID: testProducerInstanceID,
		ManifestRelativeKey: manifestKey(t, fixture), ManifestBytes: fixture.bytes, ObjectPaths: fixture.paths,
		ReplayContractID: "replay-v1", ConversionID: "conversion-v1", ResourceLimits: fixture.limits,
	})
	if err != nil {
		t.Fatal(err)
	}
	var sequences []uint64
	for {
		entry, ok, err := reader.Next()
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		sequences = append(sequences, entry.GatewayIngestSequence)
	}
	if len(sequences) != 3 || sequences[0] != 1 || sequences[1] != 2 || sequences[2] != 3 {
		t.Fatalf("selected sequences=%v, want [1 2 3] including sentinel and source error", sequences)
	}
}

func TestOpenVerifiedReplaySourceRejectsCrossDayCompactRange(t *testing.T) {
	dayA := time.Date(2024, 3, 9, 0, 0, 1, 0, time.UTC).UnixMilli()
	dayB := time.Date(2024, 3, 10, 0, 0, 1, 0, time.UTC).UnixMilli()
	fixture := buildFixture(t, []protocol.BatchFrameV1{
		batch("session-1", 1, tick(dayA, 1, 10)), batch("session-1", 2, tick(dayB, 2, 20)), batch("session-1", 3, tick(dayA+1000, 3, 30)),
	})
	object := fixture.manifest.Objects[0]
	object.StartIngestSequence = 1
	object.EndIngestSequence = 3
	object.FirstRecordOrdinal = 0
	object.LastRecordOrdinal = 0
	fixture.manifest.Objects = []archive.RawObjectRange{object}
	var err error
	fixture.manifest.RawSetRoot, err = archive.RawSetRoot(fixture.manifest.Objects)
	if err != nil {
		t.Fatal(err)
	}
	fixture.manifest.ManifestSHA256 = [32]byte{}
	fixture.bytes, err = archive.ManifestCanonicalJSON(fixture.manifest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := archive.OpenVerifiedReplaySource(archive.ReplaySourceInput{
		Scope: fixture.scope, ProducerInstanceID: testProducerInstanceID,
		ManifestRelativeKey: manifestKey(t, fixture), ManifestBytes: fixture.bytes, ObjectPaths: fixture.paths,
		ReplayContractID: "replay-v1", ConversionID: "conversion-v1", ResourceLimits: fixture.limits,
	}); !errors.Is(err, archive.ErrIntegrity) {
		t.Fatalf("cross-day compact range error=%v, want ErrIntegrity", err)
	}
}

func buildFixture(t *testing.T, batches []protocol.BatchFrameV1) rawFixture {
	t.Helper()
	scope := testScope()
	root := t.TempDir()
	store, err := wal.Open(root, "gateway-test-01")
	if err != nil {
		t.Fatal(err)
	}
	for index, batch := range batches {
		frame, err := protocol.EncodeMessage(batch)
		if err != nil {
			_ = store.Close()
			t.Fatal(err)
		}
		if _, err := store.Append(frame, 1710000000+int64(index), uint64(100+index)); err != nil {
			_ = store.Close()
			t.Fatal(err)
		}
	}
	var objects []archive.RawObject
	if len(batches) > 0 {
		sealed, err := store.Seal()
		if err != nil {
			_ = store.Close()
			t.Fatal(err)
		}
		object, err := archive.PromoteSealedSegment(t.TempDir(), sealed.Path)
		if err != nil {
			_ = store.Close()
			t.Fatal(err)
		}
		objects = []archive.RawObject{object}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	manifest, err := archive.BuildRawDayManifest(archive.RawDayManifestInput{
		Scope: scope, Date: "2024-03-09", RawObjects: objects,
		TerminalSyncStatus: "complete", CompletenessStatus: "settled_snapshot", LogicalCloseTimeS: 1710003600,
	})
	if err != nil {
		t.Fatal(err)
	}
	manifestBytes, err := archive.ManifestCanonicalJSON(manifest)
	if err != nil {
		t.Fatal(err)
	}
	paths := make(map[string]string, len(objects))
	for _, object := range objects {
		paths[object.Key] = object.Path
	}
	return rawFixture{scope: scope, manifest: manifest, bytes: manifestBytes, paths: paths, limits: replayLimitsForManifest(manifest)}
}

func rebuildFixtureManifest(t *testing.T, fixture rawFixture, date string) rawFixture {
	t.Helper()
	var objects []archive.RawObject
	for key, path := range fixture.paths {
		segment, err := wal.VerifySealedSegment(path)
		if err != nil {
			t.Fatal(err)
		}
		objects = append(objects, archive.RawObject{Key: key, Path: path, SHA256: segment.ObjectSHA256, Bytes: segment.FileBytes, Segment: segment})
	}
	manifest, err := archive.BuildRawDayManifest(archive.RawDayManifestInput{
		Scope: fixture.scope, Date: date, RawObjects: objects,
		TerminalSyncStatus: "complete", CompletenessStatus: "settled_snapshot", LogicalCloseTimeS: 1710003600,
	})
	if err != nil {
		t.Fatal(err)
	}
	data, err := archive.ManifestCanonicalJSON(manifest)
	if err != nil {
		t.Fatal(err)
	}
	fixture.manifest, fixture.bytes = manifest, data
	fixture.limits = replayLimitsForManifest(manifest)
	return fixture
}

func reduceFixture(t *testing.T, fixture rawFixture) (continuity.Result, []protocol.ReplayRow) {
	t.Helper()
	reader, err := archive.OpenVerifiedReplaySource(archive.ReplaySourceInput{
		Scope: fixture.scope, ProducerInstanceID: testProducerInstanceID, ManifestRelativeKey: manifestKey(t, fixture), ManifestBytes: fixture.bytes,
		ObjectPaths: fixture.paths, ReplayContractID: "replay-v1", ConversionID: "conversion-v1",
		ResourceLimits: fixture.limits,
	})
	if err != nil {
		t.Fatal(err)
	}
	var rows []protocol.ReplayRow
	result, err := continuity.Reduce(reader, func(row protocol.ReplayRow) error {
		rows = append(rows, row)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return result, rows
}

func manifestKey(t *testing.T, fixture rawFixture) string {
	t.Helper()
	key, err := archive.RawDayManifestRelativeKey(fixture.scope, fixture.manifest)
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func replayLimitsForManifest(manifest archive.RawDayManifest) archive.ReplayResourceLimits {
	limits := archive.ReplayResourceLimits{MaxChainObjects: uint64(len(manifest.ChainObjects)), MaxObjectBytes: 1, MaxChainBytes: 1}
	var chainBytes uint64
	for _, object := range manifest.ChainObjects {
		if object.Bytes > limits.MaxObjectBytes {
			limits.MaxObjectBytes = object.Bytes
		}
		if ^uint64(0)-chainBytes < object.Bytes {
			return archive.ReplayResourceLimits{MaxChainObjects: ^uint64(0), MaxObjectBytes: ^uint64(0), MaxChainBytes: ^uint64(0)}
		}
		chainBytes += object.Bytes
	}
	if limits.MaxChainObjects == 0 {
		limits.MaxChainObjects = 1
	}
	if chainBytes > 0 {
		limits.MaxChainBytes = chainBytes
	}
	return limits
}

func testScope() archive.ScopeConfig {
	return archive.ScopeConfig{
		DatasetID: "dataset-demo", CampaignID: "campaign-demo", ProviderID: "provider-demo", StableFeedID: "feed-demo",
		ExactSourceSymbol: "EURUSD.raw", BrokerServerFingerprint: "server-fingerprint", GatewayBuildIdentity: "gateway-build-1",
		ProducerBuildIdentity: "producer-build-1", DayDefinitionID: "utc-day-v1", SettlePolicy: "manual-v1",
		PublisherID: "publisher-1", PublisherEpoch: 1, ProtocolLimits: archive.ProtocolLimits{MaxFrameBytes: protocol.MaxFrameBytes, MaxRecords: 4, MaxStringBytes: protocol.MaxStringBytes},
	}
}

func batch(session string, sequence uint64, records ...protocol.RawMqlTickV1) protocol.BatchFrameV1 {
	requestedFrom := int64(1709942400000)
	if len(records) > 0 {
		requestedFrom = records[0].TimeMSC
	}
	scope := testScope()
	lease := protocol.DeriveSessionLeaseID(testProducerInstanceID, session, scope.CampaignID, scope.ProviderID, scope.StableFeedID, scope.BrokerServerFingerprint, scope.ExactSourceSymbol)
	return protocol.BatchFrameV1{SessionLeaseID: lease, ProducerSessionID: session, BatchSequence: sequence,
		RequestedFromMSC: requestedFrom, RequestedCount: uint32(len(records)), FetchWallStartS: 1, FetchWallEndS: 1,
		FetchMonotonicStartUS: sequence, FetchMonotonicEndUS: sequence + 1, ReturnedCount: int32(len(records)),
		SourceSchemaID: protocol.SourceSchemaMT5, Records: records}
}

func tick(timeMSC int64, capture, bid uint64) protocol.RawMqlTickV1 {
	if timeMSC < 1_000_000_000_000 {
		timeMSC += 1709942400000
	}
	return protocol.RawMqlTickV1{Time: timeMSC / 1000, BidBits: bid, AskBits: bid + 1, LastBits: bid + 2,
		Volume: 1, TimeMSC: timeMSC, Flags: 1, VolumeRealBits: 1, CaptureSequence: capture}
}

func dataCount(rows []protocol.ReplayRow) int {
	count := 0
	for _, row := range rows {
		if row.Kind == protocol.ReplayRowData {
			count++
		}
	}
	return count
}

func markerCount(rows []protocol.ReplayRow, code string) int {
	count := 0
	for _, row := range rows {
		if row.Kind == protocol.ReplayRowMarker && row.Marker.MarkerCode == code {
			count++
		}
	}
	return count
}

func markerCodes(rows []protocol.ReplayRow) []string {
	result := make([]string, 0)
	for _, row := range rows {
		if row.Kind == protocol.ReplayRowMarker {
			result = append(result, row.Marker.MarkerCode)
		}
	}
	return result
}

func flattenCanonical(t *testing.T, rows []protocol.ReplayRow) []byte {
	t.Helper()
	var result []byte
	for _, row := range rows {
		canonical, err := row.CanonicalBytes()
		if err != nil {
			t.Fatal(err)
		}
		result = append(result, canonical...)
	}
	return result
}

func clonePaths(paths map[string]string) map[string]string {
	result := make(map[string]string, len(paths))
	for key, path := range paths {
		result[key] = path
	}
	return result
}

type fakeVerifiedReader struct {
	scope      protocol.ReplayScope
	identity   continuity.ProducerIdentity
	maxRecords uint32
	batches    []continuity.VerifiedBatch
	at         int
}

func (r *fakeVerifiedReader) ReplayScope() protocol.ReplayScope { return r.scope }

func (r *fakeVerifiedReader) ProducerIdentity() continuity.ProducerIdentity { return r.identity }

func (r *fakeVerifiedReader) MaxRecords() uint32 { return r.maxRecords }

func (r *fakeVerifiedReader) Next() (continuity.VerifiedBatch, bool, error) {
	if r.at >= len(r.batches) {
		return continuity.VerifiedBatch{}, false, nil
	}
	batch := r.batches[r.at]
	r.at++
	return batch, true, nil
}
