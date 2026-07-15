package delivery

import (
	"bytes"
	"context"
	"errors"
	"os"
	"testing"

	"tick-data-platform/internal/archive"
	"tick-data-platform/internal/parquet"
	"tick-data-platform/internal/protocol"
)

type replayDeliveryFixture struct {
	deliveryFixture
	scope       protocol.ReplayScope
	conversion  archive.ConversionTuple
	part        protocol.PartManifest
	manifest    protocol.ReplayDayManifest
	manifestKey string
	parquetKey  string
}

func newReplayDeliveryFixture(t *testing.T) replayDeliveryFixture {
	t.Helper()
	base := newDeliveryFixture(t)
	rawRelative, err := archive.RawDayManifestRelativeKey(base.scope, base.manifestA)
	if err != nil {
		t.Fatal(err)
	}
	scope := protocol.ReplayScope{DatasetID: base.scope.DatasetID, CampaignID: base.scope.CampaignID, DayDefinitionID: base.scope.DayDefinitionID, Date: base.manifestA.Date, ReplayContractID: "replay-reader-v1", ConversionID: "conversion-reader-v1", RawDayManifestKey: rawRelative, RawDayManifestSHA256: base.manifestA.ManifestSHA256}
	spec, err := parquet.NewConversionSpec(scope.ReplayContractID, scope.ConversionID, "reader-build-v1", "windows-amd64-go1.24.13", 10, 1<<20, 10)
	if err != nil {
		t.Fatal(err)
	}
	conversion, err := archive.ConversionTupleFromSpec(spec)
	if err != nil {
		t.Fatal(err)
	}
	segmentID, err := protocol.SegmentID(scope, 0, 0, protocol.MarkerSegmentStart, [32]byte{})
	if err != nil {
		t.Fatal(err)
	}
	row := protocol.ReplayRow{Kind: protocol.ReplayRowMarker, Marker: &protocol.ReplayMarkerRow{Scope: scope, StreamSequence: 0, ContinuitySegmentID: segmentID, RawObjectKey: base.objects[0].Key, RawObjectSHA256: base.objects[0].SHA256, MarkerCode: protocol.MarkerSegmentStart, Reason: protocol.ReasonInitial, Detail: "reader fixture"}}
	generator, err := parquet.NewGenerator(spec, scope, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := generator.WriteRow(row); err != nil {
		t.Fatal(err)
	}
	generated, err := generator.Close()
	if err != nil {
		t.Fatal(err)
	}
	input, err := archive.PartManifestInputFromArtifact(scope, conversion, generated.Parts[0])
	if err != nil {
		t.Fatal(err)
	}
	part, err := archive.BuildPartManifest(input, nil)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := archive.BuildReplayDayManifest(archive.ReplayDayManifestInput{Scope: scope, Conversion: conversion, CompletenessStatus: "settled_snapshot", Parts: []protocol.PartManifest{part}, CanonicalStreamRowChainRoot: generated.RowChainRoot})
	if err != nil {
		t.Fatal(err)
	}
	partBody, _ := protocol.PartManifestCanonicalJSON(part)
	partKey, _ := base.layout.ReplayPartManifestKey(part)
	base.backend.objects[partKey] = partBody
	parquetBody, err := os.ReadFile(generated.Parts[0].Path)
	if err != nil {
		t.Fatal(err)
	}
	parquetKey, _ := base.layout.ReplayPartObjectKey(part)
	base.backend.objects[parquetKey] = parquetBody
	manifestBody, _ := protocol.ReplayDayManifestCanonicalJSON(manifest)
	manifestKey, _ := base.layout.ReplayDayManifestKey(manifest)
	base.backend.objects[manifestKey] = manifestBody
	return replayDeliveryFixture{deliveryFixture: base, scope: scope, conversion: conversion, part: part, manifest: manifest, manifestKey: manifestKey, parquetKey: parquetKey}
}

func (f replayDeliveryFixture) selector() ReplaySnapshotSelector {
	return ReplaySnapshotSelector{ReplayDayScope: ReplayDayScope{DatasetID: f.scope.DatasetID, CampaignID: f.scope.CampaignID, Date: f.scope.Date, ReplayContractID: f.scope.ReplayContractID, ConversionID: f.scope.ConversionID}}
}

func TestReplayDeliveryListResolveFetchAndVerify(t *testing.T) {
	fixture := newReplayDeliveryFixture(t)
	ctx := context.Background()
	items, err := fixture.reader.ListReplaySnapshots(ctx, fixture.selector().ReplayDayScope)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].ManifestKey != fixture.manifestKey || items[0].ManifestSHA256 != fixture.manifest.ManifestSHA256 {
		t.Fatalf("replay snapshots = %+v", items)
	}
	revision := uint64(1)
	selectors := []ReplaySnapshotSelector{
		fixture.selector(),
		{ReplayDayScope: fixture.selector().ReplayDayScope, Revision: &revision},
		{Manifest: fixture.manifestKey},
		{Manifest: replayManifestDigestString(fixture.manifest.ManifestSHA256)},
	}
	for _, selector := range selectors {
		resolved, err := fixture.reader.ResolveReplaySnapshot(ctx, selector)
		if err != nil || resolved.ManifestKey != fixture.manifestKey {
			t.Fatalf("resolve %+v: key=%q err=%v", selector, resolved.ManifestKey, err)
		}
	}
	resolved, _ := fixture.reader.ResolveReplaySnapshot(ctx, ReplaySnapshotSelector{Manifest: fixture.manifestKey})
	plan, err := fixture.reader.BuildReplayFetchPlan(ctx, resolved)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Parts) != 1 || len(plan.Parquet) != 1 || plan.Parquet[0].RemoteKey != fixture.parquetKey || !bytes.Contains([]byte(plan.Parquet[0].CachePath), []byte(replayManifestDigestString(fixture.part.PartSHA256))) {
		t.Fatalf("replay fetch plan = %+v", plan)
	}
	result, err := fixture.reader.FetchReplay(ctx, plan, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(result.ParquetPaths[fixture.part.PartKey]); err != nil {
		t.Fatal(err)
	}
	report, err := fixture.reader.VerifyReplayDay(ctx, ReplaySnapshotSelector{Manifest: fixture.manifestKey})
	if err != nil {
		t.Fatal(err)
	}
	if report.GenesisVerified || report.VerificationScope != VerificationScopeReplayAnchoredDay || !report.RawBindingVerified || !report.RawDaySemanticsVerified || !report.PartManifestChainVerified || !report.PartSetRootVerified || !report.ParquetSchemaVerified || !report.ParquetRowsVerified || !report.ParquetFileHashesVerified || !report.CanonicalRowChainRootVerified || report.EmptyDay || report.RowCount != 1 {
		t.Fatalf("replay verification report = %+v", report)
	}
}

func TestReplayFetchCacheAndRemoteFailuresFailClosed(t *testing.T) {
	fixture := newReplayDeliveryFixture(t)
	ctx := context.Background()
	resolved, _ := fixture.reader.ResolveReplaySnapshot(ctx, ReplaySnapshotSelector{Manifest: fixture.manifestKey})
	plan, err := fixture.reader.BuildReplayFetchPlan(ctx, resolved)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.reader.FetchReplay(ctx, plan, ""); err != nil {
		t.Fatal(err)
	}
	fixture.backend.openError[fixture.parquetKey] = errors.New("remote unavailable")
	if _, err := fixture.reader.FetchReplay(ctx, plan, ""); err != nil {
		t.Fatalf("verified cache was not reused: %v", err)
	}
	delete(fixture.backend.openError, fixture.parquetKey)
	cachePath := plan.Parquet[0].CachePath
	if err := os.WriteFile(cachePath, []byte("corrupt"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.reader.FetchReplay(ctx, plan, ""); !errors.Is(err, archive.ErrIntegrity) {
		t.Fatalf("corrupt cache error = %v", err)
	}
	corrupt, _ := os.ReadFile(cachePath)
	if string(corrupt) != "corrupt" {
		t.Fatal("corrupt cache was overwritten")
	}
	if err := os.Remove(cachePath); err != nil {
		t.Fatal(err)
	}
	original := fixture.backend.objects[fixture.parquetKey]
	fixture.backend.objects[fixture.parquetKey] = original[:len(original)-1]
	if _, err := fixture.reader.FetchReplay(ctx, plan, ""); !errors.Is(err, archive.ErrIntegrity) {
		t.Fatalf("short remote error = %v", err)
	}
	if _, err := os.Stat(cachePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("short remote promoted a cache file")
	}
	mutated := append([]byte(nil), original...)
	mutated[len(mutated)/2] ^= 0xff
	fixture.backend.objects[fixture.parquetKey] = mutated
	if _, err := fixture.reader.FetchReplay(ctx, plan, ""); !errors.Is(err, archive.ErrIntegrity) {
		t.Fatalf("hash mismatch error = %v", err)
	}
	if _, err := os.Stat(cachePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("hash mismatch promoted a cache file")
	}
	fixture.backend.objects[fixture.parquetKey] = original
	fixture.backend.openError[fixture.parquetKey] = errors.New("temporary remote failure")
	if _, err := fixture.reader.FetchReplay(ctx, plan, ""); err == nil {
		t.Fatal("temporary remote failure was treated as success")
	}
	if _, err := os.Stat(cachePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("temporary remote failure promoted a cache file")
	}
	delete(fixture.backend.openError, fixture.parquetKey)
	fixture.backend.closeError[fixture.parquetKey] = errors.New("temporary close failure")
	if _, err := fixture.reader.FetchReplay(ctx, plan, ""); err == nil {
		t.Fatal("remote close failure was treated as success")
	}
	if _, err := os.Stat(cachePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("remote close failure promoted a cache file")
	}
}

func TestReplayFetchRejectsConfiguredOversizeWithoutPromotion(t *testing.T) {
	fixture := newReplayDeliveryFixture(t)
	config := testReaderConfig(t.TempDir())
	config.MaxRawObjectBytes = 1
	reader, err := NewArchiveReaderV1WithBackend(config, fixture.backend)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := reader.ResolveReplaySnapshot(context.Background(), ReplaySnapshotSelector{Manifest: fixture.manifestKey})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := reader.BuildReplayFetchPlan(context.Background(), resolved)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reader.FetchReplay(context.Background(), plan, ""); !errors.Is(err, archive.ErrIntegrity) {
		t.Fatalf("oversize error = %v", err)
	}
	if _, err := os.Stat(plan.Parquet[0].CachePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("oversize replay object was promoted")
	}
}

func TestReplayDeliveryRejectsAmbiguousAndMismatchedSelectors(t *testing.T) {
	fixture := newReplayDeliveryFixture(t)
	ctx := context.Background()
	wrongRevision := uint64(2)
	if _, err := fixture.reader.ResolveReplaySnapshot(ctx, ReplaySnapshotSelector{ReplayDayScope: fixture.selector().ReplayDayScope, Revision: &wrongRevision}); err == nil {
		t.Fatal("missing exact revision was accepted")
	}
	wrongScope := fixture.selector()
	wrongScope.ConversionID = "other-conversion"
	if _, err := fixture.reader.ResolveReplaySnapshot(ctx, wrongScope); err == nil {
		t.Fatal("wrong conversion was accepted")
	}
	body := fixture.backend.objects[fixture.manifestKey]
	manifest, err := protocol.VerifyReplayDayManifest(body)
	if err != nil {
		t.Fatal(err)
	}
	manifest.RawDayManifestSHA256[0] ^= 1
	manifest.ManifestSHA256 = [32]byte{}
	mutated, err := protocol.ReplayDayManifestCanonicalJSON(manifest)
	if err != nil {
		t.Fatal(err)
	}
	mutatedKey, err := fixture.layout.ReplayDayManifestKey(manifest)
	if err != nil {
		t.Fatal(err)
	}
	fixture.backend.objects[mutatedKey] = mutated
	if _, err := fixture.reader.ListReplaySnapshots(ctx, fixture.selector().ReplayDayScope); err == nil {
		t.Fatal("raw binding mismatch was accepted")
	}
}

func TestReplayDeliveryRejectsDuplicateListDescriptor(t *testing.T) {
	fixture := newReplayDeliveryFixture(t)
	fixture.backend.duplicateListKey = fixture.manifestKey
	if _, err := fixture.reader.ListReplaySnapshots(context.Background(), fixture.selector().ReplayDayScope); err == nil {
		t.Fatal("duplicate replay list descriptor was accepted")
	}
}

func TestReplayFetchRejectsCallerSuppliedRemoteKeyAndCachePath(t *testing.T) {
	fixture := newReplayDeliveryFixture(t)
	resolved, err := fixture.reader.ResolveReplaySnapshot(context.Background(), ReplaySnapshotSelector{Manifest: fixture.manifestKey})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := fixture.reader.BuildReplayFetchPlan(context.Background(), resolved)
	if err != nil {
		t.Fatal(err)
	}
	wrongRemote := plan
	wrongRemote.Parquet = append([]ReplayFetchObject(nil), plan.Parquet...)
	wrongRemote.Parquet[0].RemoteKey = "v1/attacker-selected.parquet"
	if _, err := fixture.reader.FetchReplay(context.Background(), wrongRemote, ""); err == nil {
		t.Fatal("caller-supplied remote key was accepted")
	}
	wrongCache := plan
	wrongCache.Parquet = append([]ReplayFetchObject(nil), plan.Parquet...)
	wrongCache.Parquet[0].CachePath = t.TempDir() + "/caller-selected.parquet"
	if _, err := fixture.reader.FetchReplay(context.Background(), wrongCache, ""); err == nil {
		t.Fatal("caller-supplied cache path was accepted")
	}
}

func TestReplayVerifyRejectsPartRootAndRawSemanticMismatch(t *testing.T) {
	t.Run("part_set_root", func(t *testing.T) {
		fixture := newReplayDeliveryFixture(t)
		manifest := fixture.manifest
		manifest.PartSetRoot[0] ^= 1
		manifest.ManifestSHA256 = [32]byte{}
		body, err := protocol.ReplayDayManifestCanonicalJSON(manifest)
		if err != nil {
			t.Fatal(err)
		}
		key, err := fixture.layout.ReplayDayManifestKey(manifest)
		if err != nil {
			t.Fatal(err)
		}
		delete(fixture.backend.objects, fixture.manifestKey)
		fixture.backend.objects[key] = body
		resolved, err := fixture.reader.ResolveReplaySnapshot(context.Background(), ReplaySnapshotSelector{Manifest: key})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.reader.BuildReplayFetchPlan(context.Background(), resolved); err == nil {
			t.Fatal("wrong part_set_root was accepted")
		}
	})
	t.Run("raw_semantics", func(t *testing.T) {
		fixture := newReplayDeliveryFixture(t)
		rawKey, err := fixture.layout.RemoteKey(fixture.objects[0].Key)
		if err != nil {
			t.Fatal(err)
		}
		fixture.backend.objects[rawKey] = []byte("corrupt sealed WAL")
		if _, err := fixture.reader.VerifyReplayDay(context.Background(), ReplaySnapshotSelector{Manifest: fixture.manifestKey}); err == nil {
			t.Fatal("raw semantic mismatch was accepted")
		}
	})
}

func TestReplayDeliveryAcceptsCanonicalEmptyDay(t *testing.T) {
	fixture := newReplayDeliveryFixture(t)
	empty, err := archive.BuildReplayDayManifest(archive.ReplayDayManifestInput{Scope: fixture.scope, Conversion: fixture.conversion, CompletenessStatus: "settled_snapshot"})
	if err != nil {
		t.Fatal(err)
	}
	body, _ := protocol.ReplayDayManifestCanonicalJSON(empty)
	key, _ := fixture.layout.ReplayDayManifestKey(empty)
	delete(fixture.backend.objects, fixture.manifestKey)
	fixture.backend.objects[key] = body
	report, err := fixture.reader.VerifyReplayDay(context.Background(), ReplaySnapshotSelector{Manifest: key})
	if err != nil {
		t.Fatal(err)
	}
	if !report.EmptyDay || report.PartCount != 0 || report.RowCount != 0 || !report.CanonicalRowChainRootVerified {
		t.Fatalf("empty replay report = %+v", report)
	}
}

func addReplaySuccessor(t *testing.T, fixture *replayDeliveryFixture) (protocol.ReplayDayManifest, string) {
	t.Helper()
	raw, err := archive.BuildRawDayManifest(archive.RawDayManifestInput{Scope: fixture.deliveryFixture.scope, Date: fixture.manifestA.Date, RawObjects: fixture.objects, Previous: &fixture.manifestA, TerminalSyncStatus: "complete", CompletenessStatus: "settled_snapshot", LogicalCloseTimeS: 1710100001})
	if err != nil {
		t.Fatal(err)
	}
	rawBody, _ := archive.ManifestCanonicalJSON(raw)
	rawFullKey, _ := fixture.layout.ManifestKey(raw)
	fixture.backend.objects[rawFullKey] = rawBody
	rawRelative, _ := archive.RawDayManifestRelativeKey(fixture.deliveryFixture.scope, raw)
	scope := fixture.scope
	scope.RawDayManifestKey = rawRelative
	scope.RawDayManifestSHA256 = raw.ManifestSHA256
	successor, err := archive.BuildReplayDayManifest(archive.ReplayDayManifestInput{Scope: scope, Conversion: fixture.conversion, CompletenessStatus: "settled_snapshot", Previous: &fixture.manifest})
	if err != nil {
		t.Fatal(err)
	}
	body, _ := protocol.ReplayDayManifestCanonicalJSON(successor)
	key, _ := fixture.layout.ReplayDayManifestKey(successor)
	fixture.backend.objects[key] = body
	return successor, key
}

func TestReplayRevisionGraphRejectsBranchAndMissingPredecessor(t *testing.T) {
	t.Run("order_independent_terminal", func(t *testing.T) {
		fixture := newReplayDeliveryFixture(t)
		successor, key := addReplaySuccessor(t, &fixture)
		resolved, err := fixture.reader.ResolveReplaySnapshot(context.Background(), fixture.selector())
		if err != nil || resolved.ManifestKey != key || resolved.Manifest.Revision != 2 {
			t.Fatalf("terminal resolve key=%q revision=%d err=%v", resolved.ManifestKey, resolved.Manifest.Revision, err)
		}
		revision := uint64(2)
		exact, err := fixture.reader.ResolveReplaySnapshot(context.Background(), ReplaySnapshotSelector{ReplayDayScope: fixture.selector().ReplayDayScope, Revision: &revision})
		if err != nil || exact.ManifestSHA256 != successor.ManifestSHA256 {
			t.Fatalf("exact successor = %+v err=%v", exact.Descriptor, err)
		}
	})
	t.Run("missing_predecessor", func(t *testing.T) {
		fixture := newReplayDeliveryFixture(t)
		_, _ = addReplaySuccessor(t, &fixture)
		delete(fixture.backend.objects, fixture.manifestKey)
		if _, err := fixture.reader.ListReplaySnapshots(context.Background(), fixture.selector().ReplayDayScope); err == nil {
			t.Fatal("missing predecessor was accepted")
		}
	})
	t.Run("branch", func(t *testing.T) {
		fixture := newReplayDeliveryFixture(t)
		successor, _ := addReplaySuccessor(t, &fixture)
		branch := successor
		branch.CompletenessStatus = "provisional"
		branch.ManifestSHA256 = [32]byte{}
		body, err := protocol.ReplayDayManifestCanonicalJSON(branch)
		if err != nil {
			t.Fatal(err)
		}
		key, err := fixture.layout.ReplayDayManifestKey(branch)
		if err != nil {
			t.Fatal(err)
		}
		fixture.backend.objects[key] = body
		if _, err := fixture.reader.ListReplaySnapshots(context.Background(), fixture.selector().ReplayDayScope); err == nil {
			t.Fatal("branched replay revision was accepted")
		}
	})
}

func TestReplayResolveRejectsAmbiguousConversionButExactKeySucceeds(t *testing.T) {
	fixture := newReplayDeliveryFixture(t)
	otherScope := fixture.scope
	otherScope.ConversionID = "conversion-reader-v2"
	spec, err := parquet.NewConversionSpec(otherScope.ReplayContractID, otherScope.ConversionID, "reader-build-v2", "windows-amd64-go1.24.13", 10, 1<<20, 10)
	if err != nil {
		t.Fatal(err)
	}
	conversion, _ := archive.ConversionTupleFromSpec(spec)
	other, err := archive.BuildReplayDayManifest(archive.ReplayDayManifestInput{Scope: otherScope, Conversion: conversion, CompletenessStatus: "settled_snapshot"})
	if err != nil {
		t.Fatal(err)
	}
	body, _ := protocol.ReplayDayManifestCanonicalJSON(other)
	key, _ := fixture.layout.ReplayDayManifestKey(other)
	fixture.backend.objects[key] = body
	ambiguous := fixture.selector()
	ambiguous.ConversionID = ""
	if _, err := fixture.reader.ResolveReplaySnapshot(context.Background(), ambiguous); err == nil {
		t.Fatal("ambiguous conversion selected a winner")
	}
	if resolved, err := fixture.reader.ResolveReplaySnapshot(context.Background(), ReplaySnapshotSelector{Manifest: fixture.manifestKey}); err != nil || resolved.ManifestKey != fixture.manifestKey {
		t.Fatalf("exact manifest key did not resolve: key=%q err=%v", resolved.ManifestKey, err)
	}
}
