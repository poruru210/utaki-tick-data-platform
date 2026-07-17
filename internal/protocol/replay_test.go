package protocol

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func replayTestScope() ReplayScope {
	manifestBytes := []byte("raw-manifest-v1")
	manifestHash := sha256Bytes(manifestBytes)
	return ReplayScope{
		DatasetID: "demo", DayDefinitionID: "utc-day-v1", Date: "2024-03-09",
		ReplayContractID: "replay-v1", ConversionID: "conversion-v1",
		RawDayManifestKey:    "snapshots/raw/day-definition=utc-day-v1/date=2024-03-09/raw-day-1-demo.json",
		RawDayManifestSHA256: manifestHash,
	}
}

func sha256Bytes(value []byte) [32]byte {
	return sha256.Sum256(value)
}

func hashFill(value byte) [32]byte {
	var result [32]byte
	for index := range result {
		result[index] = value
	}
	return result
}

func TestReplayCanonicalRowsAndRootsAreStrict(t *testing.T) {
	scope := replayTestScope()
	record := RawMqlTickV1{Time: 1, BidBits: 2, AskBits: 3, LastBits: 4, Volume: 5, TimeMSC: 6, Flags: 7, VolumeRealBits: 8, CaptureSequence: 9}
	source := SourcePayloadFingerprint(record)
	row := ReplayRow{Kind: ReplayRowData, Data: &ReplayDataRow{
		Scope: scope, StreamSequence: 0, ContinuitySegmentID: "segment-1", RawObjectKey: "objects/raw/wal-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.rtw", RawObjectSHA256: hashFill(0xaa),
		GatewayIngestSequence: 1, ProducerInstanceID: "producer-1", ProducerSessionID: "session-1", BatchSequence: 2, RecordOrdinal: 0, CaptureSequence: record.CaptureSequence, Record: record, SourcePayloadFingerprint: source,
		ObservationHash: ObservationHash("producer-1", "session-1", 2, 0, record.CaptureSequence, source),
	}}
	rowBytes, err := row.CanonicalBytes()
	if err != nil {
		t.Fatal(err)
	}
	if len(rowBytes) == 0 || rowBytes[0] != 't' {
		t.Fatalf("unexpected canonical row bytes")
	}
	root, err := RowChainRoot([]ReplayRow{row})
	if err != nil {
		t.Fatal(err)
	}
	if root == ([32]byte{}) {
		t.Fatal("non-empty row chain has an empty root")
	}
	marker := ReplayRow{Kind: ReplayRowMarker, Marker: &ReplayMarkerRow{
		Scope: scope, StreamSequence: 0, ContinuitySegmentID: "segment-1", MarkerCode: MarkerSegmentStart, Reason: ReasonInitial, Detail: "start",
		PredecessorRowChainHash: [32]byte{}, ContinuitySegmentStartHash: [32]byte{},
	}}
	if _, err := marker.CanonicalBytes(); err != nil {
		t.Fatal(err)
	}
	if _, err := (ReplayRow{Kind: 99, Data: nil}).CanonicalBytes(); err == nil {
		t.Fatal("unknown row kind was accepted")
	}
}

func TestPartAndReplayManifestDigestsAndM0Compatibility(t *testing.T) {
	part := PartManifest{
		ManifestVersion: PartManifestVersion, DatasetID: "demo", DayDefinitionID: "utc-day-v1", Date: "2024-03-09",
		ReplayContractID: "replay-v1", FormatID: ReplayFormatID, ConversionID: "conversion-v1", ConverterBuildID: "converter-1",
		DependencyLockHash: hashFill(0x44), WriterConfigurationHash: hashFill(0x55), TargetPlatformContract: "parquet-v1",
		RawDayManifestKey: replayTestScope().RawDayManifestKey, RawDayManifestSHA256: replayTestScope().RawDayManifestSHA256,
		PartSequence: 0, PartSHA256: hashFill(0x11), PartBytes: 100,
		RowCount: 2, CanonicalRowBytes: 200, FirstStreamSequence: 0, LastStreamSequence: 1,
		PreviousRowChainHash: [32]byte{}, FirstRowChainHash: hashFill(0x22), LastRowChainHash: hashFill(0x33),
	}
	part.PartKey = mustReplayPartKey(t, replayTestScope(), part.FirstStreamSequence, part.LastStreamSequence, part.PartSHA256)
	partJSON, err := PartManifestCanonicalJSON(part)
	if err != nil {
		t.Fatal(err)
	}
	decodedPart, err := VerifyPartManifest(partJSON)
	if err != nil {
		t.Fatal(err)
	}
	partDigest, err := PartManifestDigest(part)
	if err != nil || decodedPart.ManifestSHA256 != partDigest {
		t.Fatalf("part digest mismatch: %v", err)
	}
	var partObject map[string]any
	if err := json.Unmarshal(partJSON, &partObject); err != nil {
		t.Fatal(err)
	}
	delete(partObject, "raw_day_manifest_key")
	missingPartField, err := json.Marshal(partObject)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := VerifyPartManifest(missingPartField); err == nil {
		t.Fatal("part manifest without raw binding was accepted")
	}
	partUnknown := append(bytes.TrimSuffix(partJSON, []byte("}")), []byte(`,"unknown":1}`)...)
	if _, err := VerifyPartManifest(partUnknown); err == nil {
		t.Fatal("unknown part manifest key was accepted")
	}
	replay := ReplayDayManifest{
		ManifestVersion: ReplayDayManifestVersion, ManifestID: "replay-demo-r1", DatasetID: "demo", DayDefinitionID: "utc-day-v1", Date: "2024-03-09", Revision: 1,
		RawDayManifestKey: replayTestScope().RawDayManifestKey, RawDayManifestSHA256: replayTestScope().RawDayManifestSHA256,
		ReplayContractID: "replay-v1", FormatID: ReplayFormatID, ConversionID: "conversion-v1", ConverterBuildID: "converter-1",
		DependencyLockHash: hashFill(0x44), WriterConfigurationHash: hashFill(0x55), TargetPlatformContract: "parquet-v1", CompletenessStatus: "settled_snapshot",
	}
	partKey, err := PartManifestKey(part)
	if err != nil {
		t.Fatal(err)
	}
	replay.PartManifestKeys = []string{partKey}
	replay.PartSetRoot = mustPartSetRoot(t, []PartManifest{part})
	replay.CanonicalStreamRowChainRoot = hashFill(0x33)
	replayJSON, err := ReplayDayManifestCanonicalJSON(replay)
	if err != nil {
		t.Fatal(err)
	}
	decodedReplay, err := VerifyReplayDayManifest(replayJSON)
	if err != nil {
		t.Fatal(err)
	}
	replayDigest, err := ReplayDayManifestDigest(replay)
	if err != nil || decodedReplay.ManifestSHA256 != replayDigest {
		t.Fatalf("replay digest mismatch: %v", err)
	}
	if zero, err := PartSetRoot(nil); err != nil || zero != ([32]byte{}) {
		t.Fatalf("empty part set root = %x, err=%v", zero, err)
	}
	if err := VerifyReplayDayManifestBinding(replay, "wrong-key", []byte("raw")); err == nil {
		t.Fatal("raw manifest key mismatch was accepted")
	}
	if err := VerifyReplayDayManifestBinding(replay, replay.RawDayManifestKey, []byte("raw")); err == nil {
		t.Fatal("raw manifest hash mismatch was accepted")
	}
	rawCanonical := []byte(`{"manifest_version":"raw-day-manifest-v1"}`)
	domainHash := RawDayManifestDigest(rawCanonical)
	domainBound := replay
	domainBound.RawDayManifestSHA256 = domainHash
	if err := VerifyReplayDayManifestBinding(domainBound, domainBound.RawDayManifestKey, rawCanonical); err != nil {
		t.Fatalf("domain-bound raw manifest was rejected: %v", err)
	}
	domainBound.RawDayManifestSHA256 = sha256.Sum256(rawCanonical)
	if err := VerifyReplayDayManifestBinding(domainBound, domainBound.RawDayManifestKey, rawCanonical); err == nil {
		t.Fatal("plain SHA-256 raw manifest binding was accepted")
	}
	m0Data, err := os.ReadFile("../../testdata/tickdata/golden/replay-day-manifest-v1.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		CanonicalJSON string `json:"canonical_json"`
	}
	if err := json.Unmarshal(m0Data, &fixture); err != nil {
		t.Fatal(err)
	}
	m0, err := VerifyReplayDayManifest([]byte(fixture.CanonicalJSON))
	if err != nil || !m0.M0EmptyPartsCompatibility {
		t.Fatalf("M0 compatibility form was rejected: %v", err)
	}
	if err := VerifyReplayDayManifestBinding(m0, "", nil); err == nil {
		t.Fatal("M0 compatibility form was accepted as M3 binding")
	}
	unknown := append([]byte{}, replayJSON...)
	unknown = append(bytes.TrimSuffix(unknown, []byte("}")), []byte(`,"unknown":1}`)...)
	if _, err := VerifyReplayDayManifest(unknown); err == nil {
		t.Fatal("unknown replay manifest key was accepted")
	}
	unknownVersion := bytes.Replace(replayJSON, []byte("replay-day-manifest-v1"), []byte("replay-day-manifest-v9"), 1)
	if _, err := VerifyReplayDayManifest(unknownVersion); err == nil {
		t.Fatal("unknown replay manifest version was accepted")
	}
	_ = hex.EncodeToString(replayDigest[:])
}

func TestPartManifestRejectsZeroBytesAndIdentityAnchors(t *testing.T) {
	base := validPartManifestForZeroTest()
	cases := []struct {
		name   string
		mutate func(*PartManifest)
	}{
		{name: "part bytes", mutate: func(part *PartManifest) { part.PartBytes = 0 }},
		{name: "part sha256", mutate: func(part *PartManifest) { part.PartSHA256 = [32]byte{} }},
		{name: "first row-chain hash", mutate: func(part *PartManifest) { part.FirstRowChainHash = [32]byte{} }},
		{name: "last row-chain hash", mutate: func(part *PartManifest) { part.LastRowChainHash = [32]byte{} }},
		{name: "raw manifest sha256", mutate: func(part *PartManifest) { part.RawDayManifestSHA256 = [32]byte{} }},
		{name: "dependency lock hash", mutate: func(part *PartManifest) { part.DependencyLockHash = [32]byte{} }},
		{name: "writer configuration hash", mutate: func(part *PartManifest) { part.WriterConfigurationHash = [32]byte{} }},
		{name: "part zero predecessor row-chain", mutate: func(part *PartManifest) { part.PreviousRowChainHash = hashFill(0x99) }},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			candidate := base
			test.mutate(&candidate)
			if _, err := PartManifestCanonicalJSON(candidate); err == nil {
				t.Fatal("zero or invalid part manifest value was accepted")
			}
		})
	}
	predecessorDigest := hashFill(0x98)
	successor := base
	successor.PartSequence = 1
	successor.PreviousManifestSHA256 = &predecessorDigest
	if _, err := PartManifestCanonicalJSON(successor); err == nil {
		t.Fatal("successor with an all-zero previous row-chain hash was accepted")
	}
}

func TestDerivativeKeysAreExactDateLocalAndRejectGenericOrRangeMismatches(t *testing.T) {
	scope := replayTestScope()
	if ExactIdentityPathKey("EURUSD") == ExactIdentityPathKey("eurusd") {
		t.Fatal("identity path key folded case")
	}
	exactUTF8 := sha256.Sum256([]byte("é"))
	if ExactIdentityPathKey("é") != hex.EncodeToString(exactUTF8[:]) {
		t.Fatal("identity path key did not hash exact UTF-8 bytes")
	}
	part := validPartManifestForZeroTest()
	objectKey, err := ReplayPartObjectKey(scope, part.FirstStreamSequence, part.LastStreamSequence, part.PartSHA256)
	if err != nil {
		t.Fatal(err)
	}
	if part.PartKey != objectKey || !strings.Contains(objectKey, "/date=2024-03-09/parquet/0-1-") {
		t.Fatalf("unexpected date-local part key %q", objectKey)
	}
	manifestKey, err := PartManifestKey(part)
	if err != nil {
		t.Fatal(err)
	}
	if strings.HasPrefix(manifestKey, "manifests/replay/") || strings.Contains(manifestKey, "hour=") {
		t.Fatalf("old or hourly manifest key was accepted: %q", manifestKey)
	}
	part.PartKey = "objects/replay/part-" + hex.EncodeToString(part.PartSHA256[:]) + ".parquet"
	if _, err := PartManifestCanonicalJSON(part); err == nil {
		t.Fatal("generic replay object key was accepted")
	}
	part = validPartManifestForZeroTest()
	part.LastStreamSequence = 2
	if _, err := PartManifestCanonicalJSON(part); err == nil {
		t.Fatal("part key/range mismatch was accepted")
	}
}

func validPartManifestForZeroTest() PartManifest {
	scope := replayTestScope()
	return PartManifest{
		ManifestVersion: PartManifestVersion, DatasetID: scope.DatasetID, DayDefinitionID: scope.DayDefinitionID, Date: scope.Date, ReplayContractID: scope.ReplayContractID,
		FormatID: ReplayFormatID, ConversionID: scope.ConversionID, ConverterBuildID: "converter-1",
		DependencyLockHash: hashFill(0x44), WriterConfigurationHash: hashFill(0x55), TargetPlatformContract: "parquet-v1",
		RawDayManifestKey: scope.RawDayManifestKey, RawDayManifestSHA256: scope.RawDayManifestSHA256,
		PartSequence: 0, PartKey: mustReplayPartKey(nil, replayTestScope(), 0, 1, hashFill(0x11)), PartSHA256: hashFill(0x11), PartBytes: 100,
		RowCount: 2, CanonicalRowBytes: 200, FirstStreamSequence: 0, LastStreamSequence: 1,
		PreviousRowChainHash: [32]byte{}, FirstRowChainHash: hashFill(0x22), LastRowChainHash: hashFill(0x33),
	}
}

func mustPartSetRoot(t *testing.T, parts []PartManifest) [32]byte {
	t.Helper()
	root, err := PartSetRoot(parts)
	if err != nil {
		t.Fatal(err)
	}
	return root
}

func mustReplayPartKey(t *testing.T, scope ReplayScope, first, last uint64, digest [32]byte) string {
	key, err := ReplayPartObjectKey(scope, first, last, digest)
	if err != nil {
		if t != nil {
			t.Fatal(err)
		}
		panic(err)
	}
	return key
}
