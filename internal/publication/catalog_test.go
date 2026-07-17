package publication

import (
	"context"
	"crypto/sha256"
	"path/filepath"
	"testing"
	"time"

	"tick-data-platform/internal/archive"
)

func TestCatalogPendingStatsFollowDurablePublicationMarker(t *testing.T) {
	root := t.TempDir()
	catalog, err := NewCatalog(filepath.Join(root, "catalog.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	if err := catalog.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer catalog.Stop(context.Background())

	segmentSHA := sha256.Sum256([]byte("segment"))
	segmentKey := archive.RawWALObjectKey(segmentSHA)
	updatedAt := time.Date(2024, 3, 9, 12, 0, 0, 0, time.UTC)
	segmentRecord := SegmentRecord{
		Identity: SegmentIdentity(segmentSHA), SealedPath: filepath.Join(root, "sealed"),
		RawKey: segmentKey, RawPath: filepath.Join(root, "raw"), SHA256: segmentSHA,
		Bytes: 100, StartSequence: 1, EndSequence: 1, AffectedDates: []string{"2024-03-09"},
		UpdatedAt: updatedAt,
	}
	if err := catalog.UpsertSegment(context.Background(), segmentRecord); err != nil {
		t.Fatal(err)
	}
	manifestSHA := sha256.Sum256([]byte("manifest"))
	if err := catalog.UpsertManifest(context.Background(), ManifestRecord{
		Date: "2024-03-09", Revision: 1, Path: filepath.Join(root, "manifest.json"),
		SHA256: manifestSHA, Bytes: 20, UpdatedAt: updatedAt,
	}); err != nil {
		t.Fatal(err)
	}
	if err := catalog.MarkManifestRetry(context.Background(), "2024-03-09", 1, "remote_timeout", updatedAt.Add(time.Minute), updatedAt); err != nil {
		t.Fatal(err)
	}
	stats, err := catalog.PendingStats(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stats.PendingSegments != 1 || stats.PendingManifests != 1 || stats.PendingBytes != 120 || stats.RetryCount != 1 || !stats.OldestPendingAt.Equal(updatedAt) {
		t.Fatalf("pending stats = %+v", stats)
	}

	manifest := archive.RawDayManifest{
		Date: "2024-03-09", Revision: 1,
		ChainObjects: []archive.RawChainObject{{Key: segmentKey, SHA256: segmentSHA, Bytes: 100}},
	}
	if err := catalog.MarkManifestPublished(context.Background(), manifest, updatedAt.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	segmentRecord.State = SegmentStatePromoted
	if err := catalog.UpsertSegment(context.Background(), segmentRecord); err != nil {
		t.Fatal(err)
	}
	stats, err = catalog.PendingStats(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stats != (PendingPublicationStats{}) {
		t.Fatalf("completed pending stats = %+v", stats)
	}
	segments, err := catalog.ListSegments(context.Background())
	if err != nil || len(segments) != 1 || segments[0].State != SegmentStatePublished {
		t.Fatalf("published segment was regressed by rescan: %+v err=%v", segments, err)
	}
}

func TestCatalogPendingStatsSurviveRestart(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "catalog.sqlite")
	updatedAt := time.Date(2024, 3, 9, 12, 0, 0, 0, time.UTC)
	catalog, err := NewCatalog(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := catalog.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	segmentSHA := sha256.Sum256([]byte("restart-segment"))
	if err := catalog.UpsertSegment(context.Background(), SegmentRecord{
		Identity: SegmentIdentity(segmentSHA), SealedPath: filepath.Join(root, "sealed"),
		RawKey: archive.RawWALObjectKey(segmentSHA), RawPath: filepath.Join(root, "raw"),
		SHA256: segmentSHA, Bytes: 321, StartSequence: 1, EndSequence: 2,
		AffectedDates: []string{"2024-03-09"}, UpdatedAt: updatedAt,
	}); err != nil {
		t.Fatal(err)
	}
	manifestSHA := sha256.Sum256([]byte("restart-manifest"))
	if err := catalog.UpsertManifest(context.Background(), ManifestRecord{
		Date: "2024-03-09", Revision: 1, Path: filepath.Join(root, "manifest.json"),
		SHA256: manifestSHA, Bytes: 45, UpdatedAt: updatedAt,
	}); err != nil {
		t.Fatal(err)
	}
	if err := catalog.MarkManifestRetry(context.Background(), "2024-03-09", 1, "remote_timeout", updatedAt.Add(time.Minute), updatedAt); err != nil {
		t.Fatal(err)
	}
	want, err := catalog.PendingStats(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := catalog.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	restarted, err := NewCatalog(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer restarted.Stop(context.Background())
	got, err := restarted.PendingStats(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("pending stats changed across restart: before=%+v after=%+v", want, got)
	}
}
