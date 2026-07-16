package wal_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"tick-data-platform/internal/wal"
	"tick-data-platform/producers/fake"
)

func TestWALAppendsAndRecoversIncompleteTail(t *testing.T) {
	root := t.TempDir()
	fixture, err := fake.BatchFixture()
	if err != nil {
		t.Fatal(err)
	}
	store, err := wal.Open(root, "gateway-test-01")
	if err != nil {
		t.Fatal(err)
	}
	entry, err := store.Append(fixture.Frame, 1710000000, 42)
	if err != nil {
		t.Fatal(err)
	}
	if entry.Sequence != 1 || store.Count() != 1 {
		t.Fatalf("unexpected WAL entry: %+v", entry)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(filepath.Join(root, "active.wal"), os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte{0x01, 0x02, 0x03}); err != nil {
		t.Fatal(err)
	}
	_ = file.Close()

	recovered, err := wal.Open(root, "gateway-test-01")
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	if recovered.Count() != 1 {
		t.Fatalf("incomplete tail changed accepted inventory: %d", recovered.Count())
	}
	entries := recovered.Entries()
	if string(entries[0].Frame) != string(fixture.Frame) {
		t.Fatal("recovered frame differs")
	}
}

func TestWALStopsOnCommittedEntryCorruption(t *testing.T) {
	root := t.TempDir()
	fixture, err := fake.BatchFixture()
	if err != nil {
		t.Fatal(err)
	}
	store, err := wal.Open(root, "gateway-test-01")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(fixture.Frame, 1710000000, 42); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "active.wal")
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Seek(40, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte{0xff}); err != nil {
		t.Fatal(err)
	}
	_ = file.Close()
	if _, err := wal.Open(root, "gateway-test-01"); err == nil || !errors.Is(err, wal.ErrIntegrity) {
		t.Fatalf("expected integrity stop, got %v", err)
	}
}

func TestWALOpensFromPruneAnchor(t *testing.T) {
	root := t.TempDir()
	fixture, err := fake.BatchFixture()
	if err != nil {
		t.Fatal(err)
	}
	first, err := wal.Open(root, "gateway-test-01")
	if err != nil {
		t.Fatal(err)
	}
	entry, err := first.Append(fixture.Frame, 1710000000, 42)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Seal(); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(root, "sealed", "segment-00000000000000000001-00000000000000000001.wal")); err != nil {
		t.Fatal(err)
	}
	anchor := &wal.PruneAnchor{EndSequence: 1, ChainRoot: entry.EntryHash}
	reopened, err := wal.OpenWithAnchor(root, "gateway-test-01", anchor)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if reopened.PrunedThrough() != 1 || reopened.Count() != 0 || reopened.ChainRoot() != entry.EntryHash {
		t.Fatalf("anchor state = through=%d count=%d root=%x", reopened.PrunedThrough(), reopened.Count(), reopened.ChainRoot())
	}
	second, err := reopened.Append(fixture.Frame, 1710000001, 43)
	if err != nil || second.Sequence != 2 || second.PreviousEntryHash != entry.EntryHash {
		t.Fatalf("anchored append = %+v, err=%v", second, err)
	}
}
