package archive_test

import (
	"bytes"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"tick-data-platform/internal/archive"
	"tick-data-platform/internal/wal"
	"tick-data-platform/producers/fake"
)

func TestPromoteSealedSegmentIsByteExactAndIdempotent(t *testing.T) {
	sealed := createSealedSegment(t)
	sourceBytes, err := os.ReadFile(sealed.Path)
	if err != nil {
		t.Fatal(err)
	}
	outbox := t.TempDir()

	first, err := archive.PromoteSealedSegment(outbox, sealed.Path)
	if err != nil {
		t.Fatal(err)
	}
	hashHex := hex.EncodeToString(sealed.ObjectSHA256[:])
	if !strings.Contains(first.Key, hashHex) || first.SHA256 != sealed.ObjectSHA256 {
		t.Fatalf("raw object key or hash does not use complete-file SHA-256: %+v", first)
	}
	destinationBytes, err := os.ReadFile(first.Path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(destinationBytes, sourceBytes) {
		t.Fatal("promoted raw object differs from sealed WAL bytes")
	}
	if _, err := os.Stat(sealed.Path); err != nil {
		t.Fatalf("promote removed the sealed WAL: %v", err)
	}

	second, err := archive.PromoteSealedSegment(outbox, sealed.Path)
	if err != nil {
		t.Fatal(err)
	}
	if second.Key != first.Key || second.Path != first.Path || second.SHA256 != first.SHA256 {
		t.Fatalf("same-content retry changed object identity: first=%+v second=%+v", first, second)
	}
}

func TestPromoteSealedSegmentRejectsActiveWAL(t *testing.T) {
	root := t.TempDir()
	fixture, err := fake.BatchFixture()
	if err != nil {
		t.Fatal(err)
	}
	store, err := wal.Open(root, "gateway-test-01")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := store.Append(fixture.Frame, 1710000000, 42); err != nil {
		t.Fatal(err)
	}
	if _, err := archive.PromoteSealedSegment(t.TempDir(), store.Path()); !errors.Is(err, archive.ErrIntegrity) {
		t.Fatalf("PromoteSealedSegment error = %v, want ErrIntegrity", err)
	}
}

func TestPromoteSealedSegmentDoesNotOverwriteDifferentBytes(t *testing.T) {
	sealed := createSealedSegment(t)
	outbox := t.TempDir()
	object, err := archive.PromoteSealedSegment(outbox, sealed.Path)
	if err != nil {
		t.Fatal(err)
	}
	different := []byte("different bytes at a content-addressed key")
	if err := os.WriteFile(object.Path, different, 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := archive.PromoteSealedSegment(outbox, sealed.Path); !errors.Is(err, archive.ErrIntegrity) {
		t.Fatalf("PromoteSealedSegment error = %v, want ErrIntegrity", err)
	}
	remaining, err := os.ReadFile(object.Path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(remaining, different) {
		t.Fatal("failed promote overwrote an existing object")
	}
}

func TestPromoteSealedSegmentPublishesOnceUnderConcurrency(t *testing.T) {
	sealed := createSealedSegment(t)
	outbox := t.TempDir()
	const workers = 8
	results := make([]archive.RawObject, workers)
	errs := make([]error, workers)
	var group sync.WaitGroup
	for i := 0; i < workers; i++ {
		group.Add(1)
		go func(index int) {
			defer group.Done()
			results[index], errs[index] = archive.PromoteSealedSegment(outbox, sealed.Path)
		}(i)
	}
	group.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("worker %d: %v", i, err)
		}
		if results[i].Key != results[0].Key || results[i].SHA256 != results[0].SHA256 {
			t.Fatalf("worker %d returned a different object: %+v", i, results[i])
		}
	}
	matches, err := filepath.Glob(filepath.Join(outbox, "raw-wal-segment-v1", "sha256", "*", "*.wal"))
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 1 {
		t.Fatalf("published object count = %d, want 1", len(matches))
	}
	temporary, err := filepath.Glob(filepath.Join(filepath.Dir(matches[0]), ".raw-wal-segment-v1-*.tmp"))
	if err != nil {
		t.Fatal(err)
	}
	if len(temporary) != 0 {
		t.Fatalf("temporary outbox files remain: %v", temporary)
	}
}

func createSealedSegment(t *testing.T) wal.VerifiedSegment {
	t.Helper()
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
		_ = store.Close()
		t.Fatal(err)
	}
	sealed, err := store.Seal()
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	return sealed
}
