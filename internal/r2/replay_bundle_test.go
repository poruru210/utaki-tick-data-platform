package r2

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"tick-data-platform/internal/archive"
	"tick-data-platform/internal/parquet"
	"tick-data-platform/internal/protocol"
)

func replayBundleInputFromFixture(t *testing.T, fixture *replayPublicationFixture) ReplayPublicationBundleInput {
	t.Helper()
	partManifests := make([][]byte, len(fixture.parts))
	for index, part := range fixture.parts {
		canonical, err := protocol.PartManifestCanonicalJSON(part)
		if err != nil {
			t.Fatal(err)
		}
		partManifests[index] = canonical
	}
	return ReplayPublicationBundleInput{
		Layout: fixture.layout, Conversion: fixture.input.Conversion, Limits: protocol.ReplayPublicationImplementationBounds,
		RawManifest: fixture.input.RawManifestBytes, RawObjectPaths: cloneStringMap(fixture.input.RawObjectPaths),
		Parts: append([]parquet.PartArtifact(nil), fixture.artifacts...), PartManifests: partManifests,
		ReplayManifest: fixture.input.ReplayManifestBytes, ReceiptPath: filepath.Join(t.TempDir(), "receipt.json"),
	}
}

func TestSealReplayPublicationBundleIsDeterministicAndPathIndependent(t *testing.T) {
	fixture := newReplayPublicationFixture(t, false)
	input := replayBundleInputFromFixture(t, fixture)
	first, err := SealReplayPublicationBundle(input)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Contract.ParquetObjects) != len(fixture.artifacts) || len(first.Contract.PartManifests) != len(fixture.parts) {
		t.Fatalf("sealed bundle inventory = %+v", first.Contract)
	}
	for _, forbidden := range []string{input.ReceiptPath} {
		if forbidden != "" && bytes.Contains(first.CanonicalBytes, []byte(forbidden)) {
			t.Fatalf("canonical bundle contains local path %q", forbidden)
		}
	}
	for _, artifact := range fixture.artifacts {
		if bytes.Contains(first.CanonicalBytes, []byte(artifact.Path)) {
			t.Fatalf("canonical bundle contains Parquet path %q", artifact.Path)
		}
	}

	secondInput := input
	secondInput.RawObjectPaths = cloneStringMap(input.RawObjectPaths)
	for key, source := range input.RawObjectPaths {
		secondInput.RawObjectPaths[key] = copyReplayBundleFile(t, source)
	}
	secondInput.Parts = append([]parquet.PartArtifact(nil), input.Parts...)
	for index := range secondInput.Parts {
		secondInput.Parts[index].Path = copyReplayBundleFile(t, input.Parts[index].Path)
	}
	secondInput.ReceiptPath = filepath.Join(t.TempDir(), "other-receipt.json")
	second, err := SealReplayPublicationBundle(secondInput)
	if err != nil {
		t.Fatal(err)
	}
	if first.Digest != second.Digest || !bytes.Equal(first.CanonicalBytes, second.CanonicalBytes) {
		t.Fatal("local path changes altered canonical bundle identity")
	}
	for index := range first.Contract.ParquetObjects {
		if first.Contract.ParquetObjects[index].ObjectID != second.Contract.ParquetObjects[index].ObjectID {
			t.Fatal("local path changes altered stable Parquet object IDs")
		}
	}
	if first.LocalSources.ReceiptPath == second.LocalSources.ReceiptPath {
		t.Fatal("test did not vary isolated receipt paths")
	}
}

func TestSealReplayPublicationBundleDerivesAllRemoteKeysFromLayout(t *testing.T) {
	fixture := newReplayPublicationFixture(t, false)
	sealed, err := SealReplayPublicationBundle(replayBundleInputFromFixture(t, fixture))
	if err != nil {
		t.Fatal(err)
	}
	for _, object := range sealed.Contract.RawObjects {
		want, err := fixture.layout.RemoteKey(object.RelativeKey)
		if err != nil || object.FullKey != want {
			t.Fatalf("raw full key = %q, want %q, err=%v", object.FullKey, want, err)
		}
	}
	if !strings.HasPrefix(sealed.Contract.ReplayManifest.FullKey, fixture.layout.ImmutableCampaignPrefix()+"/") {
		t.Fatal("replay manifest key is not under trusted Layout prefix")
	}
}

func TestSealReplayPublicationBundleRejectsMutatedLocalInputs(t *testing.T) {
	fixture := newReplayPublicationFixture(t, false)
	base := replayBundleInputFromFixture(t, fixture)
	tests := []struct {
		name   string
		mutate func(*ReplayPublicationBundleInput)
	}{
		{"raw_manifest", func(input *ReplayPublicationBundleInput) {
			input.RawManifest = append(append([]byte(nil), input.RawManifest...), ' ')
		}},
		{"part_manifest", func(input *ReplayPublicationBundleInput) {
			input.PartManifests[0] = append(append([]byte(nil), input.PartManifests[0]...), ' ')
		}},
		{"replay_manifest", func(input *ReplayPublicationBundleInput) {
			input.ReplayManifest = append(append([]byte(nil), input.ReplayManifest...), ' ')
		}},
		{"parquet", func(input *ReplayPublicationBundleInput) {
			path := copyReplayBundleFile(t, input.Parts[0].Path)
			file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := file.Write([]byte{0}); err != nil {
				t.Fatal(err)
			}
			if err := file.Close(); err != nil {
				t.Fatal(err)
			}
			input.Parts[0].Path = path
		}},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			input := base
			input.RawManifest = append([]byte(nil), base.RawManifest...)
			input.ReplayManifest = append([]byte(nil), base.ReplayManifest...)
			input.PartManifests = make([][]byte, len(base.PartManifests))
			for index := range base.PartManifests {
				input.PartManifests[index] = append([]byte(nil), base.PartManifests[index]...)
			}
			input.Parts = append([]parquet.PartArtifact(nil), base.Parts...)
			testCase.mutate(&input)
			if _, err := SealReplayPublicationBundle(input); err == nil {
				t.Fatal("mutated local input was accepted")
			}
		})
	}
}

func TestSealReplayPublicationBundleRejectsPreLockBudgetFailure(t *testing.T) {
	fixture := newReplayPublicationFixture(t, false)
	input := replayBundleInputFromFixture(t, fixture)
	input.Limits.MaxObservationRequests = 1
	if _, err := SealReplayPublicationBundle(input); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("budget failure = %v, want ErrResourceLimit", err)
	}
}

func TestSealReplayPublicationBundleAcceptsRevisionThreeWithImmediatePredecessor(t *testing.T) {
	fixture := newReplayPublicationFixture(t, false)
	bundle := fixture.sealedBundle(t)
	scope, conversion, err := replayVerificationInputs(bundle.Contract)
	if err != nil {
		t.Fatal(err)
	}
	genesisDigest, err := protocol.ReplayDayManifestDigest(fixture.replay)
	if err != nil {
		t.Fatal(err)
	}
	previous := fixture.replay
	previous.Revision = 2
	previous.RawDayManifestKey = "raw/predecessor.json"
	previous.RawDayManifestSHA256 = [32]byte{0xaa}
	previous.PreviousManifestSHA256 = &genesisDigest
	previousDigest, err := protocol.ReplayDayManifestDigest(previous)
	if err != nil {
		t.Fatal(err)
	}
	previous.ManifestSHA256 = previousDigest
	previousBytes, err := protocol.ReplayDayManifestCanonicalJSON(previous)
	if err != nil {
		t.Fatal(err)
	}
	current, err := archive.BuildReplayDayManifest(archive.ReplayDayManifestInput{
		Scope: scope, Conversion: conversion, CompletenessStatus: "settled_snapshot", Revision: 3,
		Previous: &previous, Parts: fixture.parts, CanonicalStreamRowChainRoot: fixture.replay.CanonicalStreamRowChainRoot,
	})
	if err != nil {
		t.Fatal(err)
	}
	currentBytes, err := protocol.ReplayDayManifestCanonicalJSON(current)
	if err != nil {
		t.Fatal(err)
	}
	input := replayBundleInputFromFixture(t, fixture)
	input.PreviousReplayManifest = previousBytes
	input.ReplayManifest = currentBytes
	sealed, err := SealReplayPublicationBundle(input)
	if err != nil {
		t.Fatal(err)
	}
	if sealed.Contract.ReplayManifest.Revision != 3 {
		t.Fatalf("sealed replay revision = %d, want 3", sealed.Contract.ReplayManifest.Revision)
	}
}

func copyReplayBundleFile(t *testing.T, source string) string {
	t.Helper()
	data, err := os.ReadFile(source)
	if err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(t.TempDir(), filepath.Base(source))
	if err := os.WriteFile(target, data, 0o700); err != nil {
		t.Fatal(err)
	}
	return target
}
