package protocol

import (
	"encoding/hex"
	"encoding/json"
	"os"
	"testing"
)

func TestReplayGoldenConformanceMatchesPythonFixture(t *testing.T) {
	data, err := os.ReadFile("../../testdata/tickdata/golden/replay-v1-conformance.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture map[string]any
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	getObject := func(value any) map[string]any { return value.(map[string]any) }
	getString := func(object map[string]any, key string) string { return object[key].(string) }
	getUint := func(object map[string]any, key string) uint64 { return uint64(object[key].(float64)) }
	getHash := func(value string) [32]byte { return fixtureHash(t, value) }
	scopeValue := getObject(fixture["scope"])
	scope := ReplayScope{
		DatasetID: getString(scopeValue, "dataset_id"), DayDefinitionID: getString(scopeValue, "day_definition_id"), Date: getString(scopeValue, "date"),
		ReplayContractID: getString(scopeValue, "replay_contract_id"), ConversionID: getString(scopeValue, "conversion_id"), RawDayManifestKey: getString(scopeValue, "raw_day_manifest_key"), RawDayManifestSHA256: getHash(getString(scopeValue, "raw_day_manifest_sha256")),
	}
	markerValue := getObject(fixture["marker_row"])
	marker := ReplayRow{Kind: ReplayRowMarker, Marker: &ReplayMarkerRow{
		Scope: scope, StreamSequence: getUint(markerValue, "stream_sequence"), ContinuitySegmentID: getString(markerValue, "continuity_segment_id"), RawObjectKey: getString(markerValue, "raw_object_key"), RawObjectSHA256: getHash(getString(markerValue, "raw_object_sha256")),
		MarkerCode: getString(markerValue, "marker_code"), Reason: getString(markerValue, "reason"), Detail: getString(markerValue, "detail"), ReferenceGatewayIngestSequence: getUint(markerValue, "reference_gateway_ingest_sequence"), ReferenceRecordOrdinal: uint32(getUint(markerValue, "reference_record_ordinal")),
		PredecessorRowChainHash: getHash(getString(markerValue, "predecessor_row_chain_hash")), ContinuitySegmentStartHash: getHash(getString(markerValue, "continuity_segment_start_hash")),
	}}
	markerBytes, err := marker.CanonicalBytes()
	if err != nil || hex.EncodeToString(markerBytes) != getString(fixture, "marker_canonical_bytes") {
		t.Fatalf("Go marker canonical bytes differ: %v", err)
	}
	dataValue := getObject(fixture["data_row"])
	recordValue := getObject(dataValue["record"])
	record := RawMqlTickV1{Time: int64(recordValue["time"].(float64)), BidBits: getUint(recordValue, "bid_bits"), AskBits: getUint(recordValue, "ask_bits"), LastBits: getUint(recordValue, "last_bits"), Volume: getUint(recordValue, "volume"), TimeMSC: int64(recordValue["time_msc"].(float64)), Flags: uint32(getUint(recordValue, "flags")), VolumeRealBits: getUint(recordValue, "volume_real_bits"), CaptureSequence: getUint(dataValue, "capture_sequence")}
	dataRow := ReplayRow{Kind: ReplayRowData, Data: &ReplayDataRow{
		Scope: scope, StreamSequence: getUint(dataValue, "stream_sequence"), ContinuitySegmentID: getString(dataValue, "continuity_segment_id"), RawObjectKey: getString(dataValue, "raw_object_key"), RawObjectSHA256: getHash(getString(dataValue, "raw_object_sha256")),
		GatewayIngestSequence: getUint(dataValue, "gateway_ingest_sequence"), ProducerInstanceID: getString(dataValue, "producer_instance_id"), ProducerSessionID: getString(dataValue, "producer_session_id"), BatchSequence: getUint(dataValue, "batch_sequence"), RecordOrdinal: uint32(getUint(dataValue, "record_ordinal")), CaptureSequence: getUint(dataValue, "capture_sequence"), Record: record, SourcePayloadFingerprint: getHash(getString(dataValue, "source_payload_fingerprint")), ObservationHash: getHash(getString(dataValue, "observation_hash")),
		FetchWallStartS: int64(dataValue["fetch_wall_start_s"].(float64)), FetchWallEndS: int64(dataValue["fetch_wall_end_s"].(float64)), FetchMonotonicStartUS: getUint(dataValue, "fetch_monotonic_start_us"), FetchMonotonicEndUS: getUint(dataValue, "fetch_monotonic_end_us"), CopyTicksError: int32(dataValue["copy_ticks_error"].(float64)), SourceStatusFlags: uint32(getUint(dataValue, "source_status_flags")),
	}}
	dataBytes, err := dataRow.CanonicalBytes()
	if err != nil || hex.EncodeToString(dataBytes) != getString(fixture, "data_canonical_bytes") {
		t.Fatalf("Go data canonical bytes differ: %v", err)
	}
	markerHash := RowChainStep(0, [32]byte{}, markerBytes)
	root := RowChainStep(1, markerHash, dataBytes)
	if hex.EncodeToString(root[:]) != getString(fixture, "row_chain_root") {
		t.Fatal("Go row-chain root differs")
	}
	partValue := getObject(fixture["part_manifest"])
	part := PartManifest{
		ManifestVersion: getString(partValue, "manifest_version"), DatasetID: getString(partValue, "dataset_id"), DayDefinitionID: getString(partValue, "day_definition_id"), Date: getString(partValue, "date"),
		ReplayContractID: getString(partValue, "replay_contract_id"), FormatID: getString(partValue, "format_id"), ConversionID: getString(partValue, "conversion_id"), ConverterBuildID: getString(partValue, "converter_build_id"),
		DependencyLockHash: getHash(getString(partValue, "dependency_lock_hash")), WriterConfigurationHash: getHash(getString(partValue, "writer_configuration_hash")), TargetPlatformContract: getString(partValue, "target_platform_contract"),
		RawDayManifestKey: getString(partValue, "raw_day_manifest_key"), RawDayManifestSHA256: getHash(getString(partValue, "raw_day_manifest_sha256")),
		PartSequence: uint32(getUint(partValue, "part_sequence")), PartKey: getString(partValue, "part_key"), PartSHA256: getHash(getString(partValue, "part_sha256")), PartBytes: getUint(partValue, "part_bytes"), RowCount: getUint(partValue, "row_count"), CanonicalRowBytes: getUint(partValue, "canonical_row_bytes"), FirstStreamSequence: getUint(partValue, "first_stream_sequence"), LastStreamSequence: getUint(partValue, "last_stream_sequence"), PreviousRowChainHash: getHash(getString(partValue, "previous_row_chain_hash")), FirstRowChainHash: getHash(getString(partValue, "first_row_chain_hash")), LastRowChainHash: getHash(getString(partValue, "last_row_chain_hash")),
	}
	partJSON, err := PartManifestCanonicalJSON(part)
	if err != nil || string(partJSON) != getString(fixture, "part_manifest_canonical_json") {
		t.Fatalf("Go part manifest canonical JSON differs: %v", err)
	}
	partDigest, err := PartManifestDigest(part)
	if err != nil || hex.EncodeToString(partDigest[:]) != getString(fixture, "part_manifest_digest") {
		t.Fatalf("Go part manifest digest differs: %v", err)
	}
	partKey, err := PartManifestKey(part)
	if err != nil || partKey != getString(fixture, "part_manifest_key") {
		t.Fatalf("Go part manifest key differs: %v", err)
	}
	partRoot, err := PartSetRoot([]PartManifest{part})
	if err != nil || hex.EncodeToString(partRoot[:]) != getString(fixture, "part_set_root") {
		t.Fatalf("Go part set root differs: %v", err)
	}
	replayValue := getObject(fixture["replay_manifest"])
	partKeys := make([]string, len(replayValue["part_manifest_keys"].([]any)))
	for index, value := range replayValue["part_manifest_keys"].([]any) {
		partKeys[index] = value.(string)
	}
	manifest := ReplayDayManifest{ManifestVersion: getString(replayValue, "manifest_version"), ManifestID: getString(replayValue, "manifest_id"), DatasetID: getString(replayValue, "dataset_id"), DayDefinitionID: getString(replayValue, "day_definition_id"), Date: getString(replayValue, "date"), Revision: getUint(replayValue, "revision"), RawDayManifestKey: getString(replayValue, "raw_day_manifest_key"), RawDayManifestSHA256: getHash(getString(replayValue, "raw_day_manifest_sha256")), ReplayContractID: getString(replayValue, "replay_contract_id"), FormatID: getString(replayValue, "format_id"), ConversionID: getString(replayValue, "conversion_id"), ConverterBuildID: getString(replayValue, "converter_build_id"), DependencyLockHash: getHash(getString(replayValue, "dependency_lock_hash")), WriterConfigurationHash: getHash(getString(replayValue, "writer_configuration_hash")), TargetPlatformContract: getString(replayValue, "target_platform_contract"), CompletenessStatus: getString(replayValue, "completeness_status"), PartManifestKeys: partKeys, PartSetRoot: partRoot, CanonicalStreamRowChainRoot: root}
	replayJSON, err := ReplayDayManifestCanonicalJSON(manifest)
	if err != nil || string(replayJSON) != getString(fixture, "replay_manifest_canonical_json") {
		t.Fatalf("Go replay manifest canonical JSON differs: %v", err)
	}
	replayDigest, err := ReplayDayManifestDigest(manifest)
	if err != nil || hex.EncodeToString(replayDigest[:]) != getString(fixture, "replay_manifest_digest") {
		t.Fatalf("Go replay manifest digest differs: %v", err)
	}
	replayKey, err := ReplayDayManifestKey(manifest)
	if err != nil || replayKey != getString(fixture, "replay_manifest_key") {
		t.Fatalf("Go replay manifest key differs: %v", err)
	}
}

func fixtureHash(t *testing.T, value string) [32]byte {
	t.Helper()
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != 32 {
		t.Fatalf("invalid fixture hash %q", value)
	}
	var result [32]byte
	copy(result[:], decoded)
	return result
}
