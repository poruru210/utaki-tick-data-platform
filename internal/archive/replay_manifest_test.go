package archive

import (
	"bytes"
	"testing"

	"tick-data-platform/internal/parquet"
	"tick-data-platform/internal/protocol"
)

func TestReplayManifestBuildAndStrictVerification(t *testing.T) {
	scope := replayManifestTestScope(1)
	conversion := replayManifestTestConversion(scope)
	input, err := PartManifestInputFromArtifact(scope, conversion, testPartArtifact(scope, 0, 0, 1, testHash(0x11), [32]byte{}, testHash(0x22), testHash(0x33)))
	if err != nil {
		t.Fatal(err)
	}
	part, err := BuildPartManifest(input, nil)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := BuildReplayDayManifest(ReplayDayManifestInput{
		Scope: scope, Conversion: conversion, CompletenessStatus: "settled_snapshot", Parts: []protocol.PartManifest{part}, CanonicalStreamRowChainRoot: testHash(0x33),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := BuildReplayDayManifest(ReplayDayManifestInput{
		Scope: scope, Conversion: conversion, CompletenessStatus: "settled_snapshot", Parts: []protocol.PartManifest{part}, CanonicalStreamRowChainRoot: testHash(0x44),
	}); err == nil {
		t.Fatal("BuildReplayDayManifest accepted an arbitrary nonzero row-chain root")
	}
	data, err := protocol.ReplayDayManifestCanonicalJSON(manifest)
	if err != nil {
		t.Fatal(err)
	}
	key, err := protocol.ReplayDayManifestKey(manifest)
	if err != nil {
		t.Fatal(err)
	}
	verified, err := VerifyReplayDayManifestObject(data, key, scope, conversion, []protocol.PartManifest{part}, nil, testHash(0x33))
	if err != nil {
		t.Fatal(err)
	}
	if verified.ManifestSHA256 != manifest.ManifestSHA256 {
		t.Fatal("replay manifest digest changed during verification")
	}
	if _, err := VerifyReplayDayManifestObject(data, key, scope, conversion, []protocol.PartManifest{part}, nil, testHash(0x45)); err == nil {
		t.Fatal("wrong row-chain root was accepted")
	}
	mutated := bytes.Replace(data, []byte("settled_snapshot"), []byte("provisional"), 1)
	if _, err := VerifyReplayDayManifestObject(mutated, key, scope, conversion, []protocol.PartManifest{part}, nil, testHash(0x33)); err == nil {
		t.Fatal("noncanonical or digest-mutated replay manifest was accepted")
	}
	wrongPartKey, err := protocol.PartManifestKey(part)
	if err != nil {
		t.Fatal(err)
	}
	wrongPartKey += "-wrong"
	if _, err := VerifyPartManifestObject(mustPartJSON(t, part), wrongPartKey, [32]byte{}); err == nil {
		t.Fatal("wrong part manifest key was accepted")
	}
}

func TestReplayManifestEmptyDayAndRevisionRules(t *testing.T) {
	scope := replayManifestTestScope(1)
	conversion := replayManifestTestConversion(scope)
	empty, err := BuildReplayDayManifest(ReplayDayManifestInput{Scope: scope, Conversion: conversion, CompletenessStatus: "settled_snapshot"})
	if err != nil {
		t.Fatal(err)
	}
	if len(empty.PartManifestKeys) != 0 || empty.PartSetRoot != ([32]byte{}) || empty.CanonicalStreamRowChainRoot != ([32]byte{}) {
		t.Fatal("empty replay manifest is not zero-root")
	}
	changedRaw := replayManifestTestScope(2)
	successor, err := BuildReplayDayManifest(ReplayDayManifestInput{Scope: changedRaw, Conversion: conversion, CompletenessStatus: "settled_snapshot", Previous: &empty})
	if err != nil {
		t.Fatal(err)
	}
	if successor.Revision != 2 || successor.PreviousManifestSHA256 == nil {
		t.Fatal("raw revision successor was not chained")
	}
	if _, err := BuildReplayDayManifest(ReplayDayManifestInput{Scope: scope, Conversion: conversion, CompletenessStatus: "settled_snapshot", Previous: &empty}); err == nil {
		t.Fatal("same raw revision was accepted as a successor")
	}
	changedConversion := conversion
	changedConversion.ConversionID = "conversion-v2"
	changedScope := scope
	changedScope.ConversionID = "conversion-v2"
	newIdentity, err := BuildReplayDayManifest(ReplayDayManifestInput{Scope: changedScope, Conversion: changedConversion, CompletenessStatus: "settled_snapshot"})
	if err != nil {
		t.Fatal(err)
	}
	if newIdentity.Revision != 1 || newIdentity.ManifestID == empty.ManifestID {
		t.Fatal("conversion change did not start a separate identity")
	}
}

func TestReplayManifestRejectsPartGapAndOverlap(t *testing.T) {
	scope := replayManifestTestScope(1)
	conversion := replayManifestTestConversion(scope)
	firstInput, err := PartManifestInputFromArtifact(scope, conversion, testPartArtifact(scope, 0, 0, 0, testHash(1), [32]byte{}, testHash(2), testHash(2)))
	if err != nil {
		t.Fatal(err)
	}
	first, err := BuildPartManifest(firstInput, nil)
	if err != nil {
		t.Fatal(err)
	}
	secondInput, err := PartManifestInputFromArtifact(scope, conversion, testPartArtifact(scope, 1, 2, 2, testHash(3), testHash(2), testHash(4), testHash(4)))
	if err != nil {
		t.Fatal(err)
	}
	second, err := BuildPartManifest(secondInput, &first)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := BuildReplayDayManifest(ReplayDayManifestInput{Scope: scope, Conversion: conversion, CompletenessStatus: "settled_snapshot", Parts: []protocol.PartManifest{first, second}, CanonicalStreamRowChainRoot: testHash(4)}); err == nil {
		t.Fatal("part stream gap was accepted")
	}
}

func TestPartArtifactKeyBindsScopeAndExactRange(t *testing.T) {
	scope := replayManifestTestScope(1)
	conversion := replayManifestTestConversion(scope)
	foreignScope := scope
	foreignScope.ReplayContractID = "replay-other"
	foreignKey, err := protocol.ReplayPartObjectKey(foreignScope, 0, 0, testHash(0x71))
	if err != nil {
		t.Fatal(err)
	}
	foreignArtifact := testPartArtifact(scope, 0, 0, 0, testHash(0x71), [32]byte{}, testHash(0x72), testHash(0x73))
	foreignArtifact.PartKey = foreignKey
	if _, err := PartManifestInputFromArtifact(scope, conversion, foreignArtifact); err == nil {
		t.Fatal("cross-scope part object key was accepted")
	}
	rangeArtifact := testPartArtifact(scope, 0, 0, 0, testHash(0x74), [32]byte{}, testHash(0x75), testHash(0x76))
	rangeArtifact.LastStreamSequence = 1
	rangeArtifact.RowCount = 2
	if _, err := PartManifestInputFromArtifact(scope, conversion, rangeArtifact); err == nil {
		t.Fatal("part key/range mismatch was accepted")
	}
}

func replayManifestTestScope(rawRevision byte) protocol.ReplayScope {
	return protocol.ReplayScope{
		DatasetID: "dataset", DayDefinitionID: "utc-day-v1", Date: "2026-07-15",
		ReplayContractID: "replay-v1", ConversionID: "conversion-v1", RawDayManifestKey: "snapshots/raw/day-" + string(rune('0'+rawRevision)) + ".json", RawDayManifestSHA256: testHash(rawRevision + 0x20),
	}
}

func testPartArtifact(scope protocol.ReplayScope, sequence uint32, first, last uint64, objectHash, previousRowHash, firstRowHash, lastRowHash [32]byte) parquet.PartArtifact {
	partKey, err := protocol.ReplayPartObjectKey(scope, first, last, objectHash)
	if err != nil {
		panic(err)
	}
	return parquet.PartArtifact{
		PartSequence: sequence, PartKey: partKey, PartSHA256: objectHash,
		PartBytes: 10, RowCount: last - first + 1, CanonicalRowBytes: 10,
		FirstStreamSequence: first, LastStreamSequence: last, PreviousRowChainHash: previousRowHash,
		FirstRowChainHash: firstRowHash, LastRowChainHash: lastRowHash,
	}
}

func replayManifestTestConversion(scope protocol.ReplayScope) ConversionTuple {
	return ConversionTuple{ReplayContractID: scope.ReplayContractID, FormatID: protocol.ReplayFormatID, ConversionID: scope.ConversionID, ConverterBuildID: "converter-test", DependencyLockHash: testHash(0x40), WriterConfigurationHash: testHash(0x41), TargetPlatformContract: "windows-amd64-go1.24.13"}
}

func testHash(value byte) [32]byte {
	var result [32]byte
	result[0] = value
	return result
}

func mustPartJSON(t *testing.T, part protocol.PartManifest) []byte {
	t.Helper()
	data, err := protocol.PartManifestCanonicalJSON(part)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestReplayManifestRejectsMixedPartBindingsAndRowChainAnchors(t *testing.T) {
	baseScope := replayManifestTestScope(1)
	baseConversion := replayManifestTestConversion(baseScope)
	baseInput, err := PartManifestInputFromArtifact(baseScope, baseConversion, testPartArtifact(baseScope, 0, 0, 0, testHash(0x51), [32]byte{}, testHash(0x52), testHash(0x53)))
	if err != nil {
		t.Fatal(err)
	}
	basePart, err := BuildPartManifest(baseInput, nil)
	if err != nil {
		t.Fatal(err)
	}
	rejectPart := func(name string, scope protocol.ReplayScope, conversion ConversionTuple) {
		t.Helper()
		input, buildErr := PartManifestInputFromArtifact(scope, conversion, testPartArtifact(scope, 0, 0, 0, testHash(byte(len(name)+0x60)), [32]byte{}, testHash(0x62), testHash(0x63)))
		if buildErr != nil {
			t.Fatalf("%s input: %v", name, buildErr)
		}
		part, buildErr := BuildPartManifest(input, nil)
		if buildErr != nil {
			t.Fatalf("%s part: %v", name, buildErr)
		}
		if _, buildErr = BuildReplayDayManifest(ReplayDayManifestInput{Scope: baseScope, Conversion: baseConversion, CompletenessStatus: "settled_snapshot", Parts: []protocol.PartManifest{part}, CanonicalStreamRowChainRoot: part.LastRowChainHash}); buildErr == nil {
			t.Errorf("%s binding was accepted", name)
		}
	}
	dayScope := baseScope
	dayScope.Date = "2026-07-16"
	rejectPart("cross-day", dayScope, baseConversion)
	dayDefinitionScope := baseScope
	dayDefinitionScope.DayDefinitionID = "exchange-day-v1"
	rejectPart("cross-day-definition", dayDefinitionScope, baseConversion)
	conversionScope := baseScope
	conversionScope.ConversionID = "conversion-v2"
	conversion := baseConversion
	conversion.ConversionID = conversionScope.ConversionID
	rejectPart("cross-conversion", conversionScope, conversion)
	rawScope := baseScope
	rawScope.RawDayManifestKey = "snapshots/raw/day-2.json"
	rawScope.RawDayManifestSHA256 = testHash(0x72)
	rejectPart("changed-raw-binding", rawScope, baseConversion)

	badSuccessor := testPartArtifact(baseScope, 1, 1, 1, testHash(0x73), testHash(0x99), testHash(0x74), testHash(0x75))
	badInput, err := PartManifestInputFromArtifact(baseScope, baseConversion, badSuccessor)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := BuildPartManifest(badInput, &basePart); err == nil {
		t.Fatal("wrong previous row-chain anchor was accepted")
	}
}

func TestGeneratedReopenedMultiPartParquetBuildsBoundReplayManifest(t *testing.T) {
	scope := replayManifestTestScope(1)
	spec, err := parquet.NewConversionSpec("replay-v1", "conversion-v1", "converter-test", "windows-amd64-go1.24.13", 1, 1<<20, 1)
	if err != nil {
		t.Fatal(err)
	}
	generator, err := parquet.NewGenerator(spec, scope, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for sequence := uint64(0); sequence < 3; sequence++ {
		if err := generator.WriteRow(testArchiveDataRow(scope, sequence)); err != nil {
			t.Fatal(err)
		}
	}
	result, err := generator.Close()
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Parts) != 3 || result.RowChainRoot == ([32]byte{}) {
		t.Fatalf("expected three verified Parquet parts: %+v", result)
	}
	conversion := ConversionTuple{
		ReplayContractID: scope.ReplayContractID, FormatID: spec.FormatID, ConversionID: spec.ConversionID,
		ConverterBuildID: spec.ConverterBuildID, DependencyLockHash: spec.DependencyLockHash,
		WriterConfigurationHash: spec.WriterConfigurationHash, TargetPlatformContract: spec.TargetPlatformContract,
	}
	parts := make([]protocol.PartManifest, 0, len(result.Parts))
	var previous *protocol.PartManifest
	for _, artifact := range result.Parts {
		input, inputErr := PartManifestInputFromArtifact(scope, conversion, artifact)
		if inputErr != nil {
			t.Fatal(inputErr)
		}
		part, partErr := BuildPartManifest(input, previous)
		if partErr != nil {
			t.Fatal(partErr)
		}
		parts = append(parts, part)
		previous = &parts[len(parts)-1]
	}
	manifest, err := BuildReplayDayManifest(ReplayDayManifestInput{
		Scope: scope, Conversion: conversion, CompletenessStatus: "settled_snapshot", Parts: parts,
		CanonicalStreamRowChainRoot: result.RowChainRoot,
	})
	if err != nil {
		t.Fatal(err)
	}
	if manifest.CanonicalStreamRowChainRoot != parts[len(parts)-1].LastRowChainHash {
		t.Fatal("replay root does not close over generated final part")
	}
	data, err := protocol.ReplayDayManifestCanonicalJSON(manifest)
	if err != nil {
		t.Fatal(err)
	}
	key, err := protocol.ReplayDayManifestKey(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := VerifyReplayDayManifestObject(data, key, scope, conversion, parts, nil, result.RowChainRoot); err != nil {
		t.Fatal(err)
	}
}

func testArchiveDataRow(scope protocol.ReplayScope, sequence uint64) protocol.ReplayRow {
	objectHash := testHash(byte(0x80 + sequence))
	record := protocol.RawMqlTickV1{
		Time: 100 + int64(sequence), BidBits: sequence + 1, AskBits: sequence + 2, LastBits: sequence + 3,
		Volume: 4, TimeMSC: 1000 + int64(sequence), Flags: 1, VolumeRealBits: sequence + 5, CaptureSequence: sequence,
	}
	fingerprint := protocol.SourcePayloadFingerprint(record)
	return protocol.ReplayRow{Kind: protocol.ReplayRowData, Data: &protocol.ReplayDataRow{
		Scope: scope, StreamSequence: sequence, ContinuitySegmentID: "segment-test", RawObjectKey: protocol.RawWALObjectKey(objectHash), RawObjectSHA256: objectHash,
		GatewayIngestSequence: 10 + sequence, ProducerInstanceID: "producer-test", ProducerSessionID: "session-test", BatchSequence: 1, RecordOrdinal: uint32(sequence), CaptureSequence: sequence,
		Record: record, SourcePayloadFingerprint: fingerprint, ObservationHash: protocol.ObservationHash("producer-test", "session-test", 1, uint32(sequence), sequence, fingerprint),
	}}
}
