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
