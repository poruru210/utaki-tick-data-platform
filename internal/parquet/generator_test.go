package parquet

import (
	"bytes"
	"math"
	"os"
	"testing"

	"tick-data-platform/internal/protocol"
)

func TestGeneratorWritesAndReopensDeterministicParts(t *testing.T) {
	scope := testParquetScope()
	spec, err := NewConversionSpec("replay-v1", "conversion-v1", "converter-test", "windows-amd64-go1.24.13", 2, 1<<20, 2)
	if err != nil {
		t.Fatal(err)
	}
	rows := []protocol.ReplayRow{
		testDataRow(scope, 0, 0x7ff8000000000001, 0x8000000000000000),
		testMarkerRow(scope, 1),
		testDataRow(scope, 2, 0x7ff8000000000002, 0),
	}
	firstDir := t.TempDir()
	first, err := NewGenerator(spec, scope, firstDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if err := first.WriteRow(row); err != nil {
			t.Fatal(err)
		}
	}
	firstResult, err := first.Close()
	if err != nil {
		t.Fatal(err)
	}
	if len(firstResult.Parts) != 2 || firstResult.RowCount != 3 || firstResult.RowChainRoot == ([32]byte{}) {
		t.Fatalf("unexpected generation result: %+v", firstResult)
	}
	if firstResult.Parts[0].LastStreamSequence != 1 || firstResult.Parts[1].FirstStreamSequence != 2 {
		t.Fatalf("part boundary is not row-count deterministic: %+v", firstResult.Parts)
	}
	for _, part := range firstResult.Parts {
		if err := VerifyPartFile(part.Path, part, scope); err != nil {
			t.Fatal(err)
		}
	}

	secondDir := t.TempDir()
	second, err := NewGenerator(spec, scope, secondDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if err := second.WriteRow(row); err != nil {
			t.Fatal(err)
		}
	}
	secondResult, err := second.Close()
	if err != nil {
		t.Fatal(err)
	}
	if secondResult.RowChainRoot != firstResult.RowChainRoot || len(secondResult.Parts) != len(firstResult.Parts) {
		t.Fatal("logical rerun result changed")
	}
	for index := range firstResult.Parts {
		firstBytes, err := os.ReadFile(firstResult.Parts[index].Path)
		if err != nil {
			t.Fatal(err)
		}
		secondBytes, err := os.ReadFile(secondResult.Parts[index].Path)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(firstBytes, secondBytes) {
			t.Fatalf("identical conversion tuple produced different Parquet bytes in part %d", index)
		}
	}
	retry, err := NewGenerator(spec, scope, firstDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if err := retry.WriteRow(row); err != nil {
			t.Fatal(err)
		}
	}
	retryResult, err := retry.Close()
	if err != nil {
		t.Fatal(err)
	}
	if retryResult.Parts[0].PartSHA256 != firstResult.Parts[0].PartSHA256 {
		t.Fatal("same-scope retry did not reuse sealed Parquet bytes")
	}
}

func TestGeneratorRejectsOversizedCanonicalRowAndWrongScope(t *testing.T) {
	scope := testParquetScope()
	spec, err := NewConversionSpec("replay-v1", "conversion-v1", "converter-test", "windows-amd64-go1.24.13", 2, 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	generator, err := NewGenerator(spec, scope, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := generator.WriteRow(testDataRow(scope, 0, math.Float64bits(math.NaN()), math.Float64bits(math.Copysign(0, -1)))); err == nil {
		t.Fatal("oversized canonical row was accepted")
	}
	wrongScope := scope
	wrongScope.DayDefinitionID = "exchange-day-v1"
	if err := generator.WriteRow(testDataRow(wrongScope, 0, 1, 2)); err == nil {
		t.Fatal("wrong scope was accepted")
	}
}

func TestEmptyGeneratorUsesZeroRoots(t *testing.T) {
	scope := testParquetScope()
	spec, err := NewConversionSpec("replay-v1", "conversion-v1", "converter-test", "windows-amd64-go1.24.13", 2, 1<<20, 2)
	if err != nil {
		t.Fatal(err)
	}
	generator, err := NewGenerator(spec, scope, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	result, err := generator.Close()
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Parts) != 0 || result.RowChainRoot != ([32]byte{}) || result.RowCount != 0 {
		t.Fatalf("empty generation is not zero-root: %+v", result)
	}
}

func TestVerifyPartFileRejectsReopenMutation(t *testing.T) {
	scope := testParquetScope()
	spec, err := NewConversionSpec("replay-v1", "conversion-v1", "converter-test", "windows-amd64-go1.24.13", 4, 1<<20, 4)
	if err != nil {
		t.Fatal(err)
	}
	generator, err := NewGenerator(spec, scope, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := generator.WriteRow(testDataRow(scope, 0, 1, 2)); err != nil {
		t.Fatal(err)
	}
	result, err := generator.Close()
	if err != nil {
		t.Fatal(err)
	}
	partBytes, err := os.ReadFile(result.Parts[0].Path)
	if err != nil {
		t.Fatal(err)
	}
	partBytes[len(partBytes)/2] ^= 0x01
	if err := os.WriteFile(result.Parts[0].Path, partBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := VerifyPartFile(result.Parts[0].Path, result.Parts[0], scope); err == nil {
		t.Fatal("mutated Parquet bytes were accepted")
	}
}

func testParquetScope() protocol.ReplayScope {
	var rawHash [32]byte
	rawHash[0] = 0x44
	return protocol.ReplayScope{
		DatasetID: "dataset", DayDefinitionID: "utc-day-v1", Date: "2026-07-15",
		ReplayContractID: "replay-v1", ConversionID: "conversion-v1", RawDayManifestKey: "snapshots/raw/day.json", RawDayManifestSHA256: rawHash,
	}
}

func testDataRow(scope protocol.ReplayScope, sequence uint64, bidBits, volumeRealBits uint64) protocol.ReplayRow {
	var rawHash [32]byte
	rawHash[0] = 0x55
	record := protocol.RawMqlTickV1{Time: 1710000000 + int64(sequence), BidBits: bidBits, AskBits: math.Float64bits(math.Inf(1)), LastBits: math.Float64bits(math.Inf(-1)), Volume: 9, TimeMSC: 1000 + int64(sequence), Flags: 7, VolumeRealBits: volumeRealBits, CaptureSequence: sequence}
	fingerprint := protocol.SourcePayloadFingerprint(record)
	return protocol.ReplayRow{Kind: protocol.ReplayRowData, Data: &protocol.ReplayDataRow{
		Scope: scope, StreamSequence: sequence, ContinuitySegmentID: "segment-1", RawObjectKey: protocol.RawWALObjectKey(rawHash), RawObjectSHA256: rawHash,
		GatewayIngestSequence: 12 + sequence, ProducerInstanceID: "instance", ProducerSessionID: "session", BatchSequence: 3, RecordOrdinal: uint32(sequence), CaptureSequence: sequence,
		Record: record, SourcePayloadFingerprint: fingerprint, ObservationHash: protocol.ObservationHash("instance", "session", 3, uint32(sequence), sequence, fingerprint),
		FetchWallStartS: 1, FetchWallEndS: 2, FetchMonotonicStartUS: 3, FetchMonotonicEndUS: 4, CopyTicksError: 0, SourceStatusFlags: 5,
	}}
}

func testMarkerRow(scope protocol.ReplayScope, sequence uint64) protocol.ReplayRow {
	return protocol.ReplayRow{Kind: protocol.ReplayRowMarker, Marker: &protocol.ReplayMarkerRow{
		Scope: scope, StreamSequence: sequence, ContinuitySegmentID: "segment-1", MarkerCode: protocol.MarkerGap, Reason: protocol.ReasonWALSequenceGap, Detail: "gap",
		ReferenceGatewayIngestSequence: 12, ReferenceRecordOrdinal: 1,
	}}
}
