package wal_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"tick-data-platform/internal/protocol"
	"tick-data-platform/internal/wal"
	"tick-data-platform/producers/fake"
)

func TestVerifySealedSegmentAcceptsProtocolV1GoldenFixture(t *testing.T) {
	var fixture map[string]any
	data, err := os.ReadFile(filepath.Join("..", "..", "testdata", "tickdata", "golden", "wal-entry-v1.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	expected := fixture["expected_hashes"].(map[string]any)
	segmentBytes := append(decodeHex(t, fixture["file_header_hex"].(string)), decodeHex(t, fixture["entry_hex"].(string))...)
	segmentBytes = append(segmentBytes, decodeHex(t, fixture["trailer_hex"].(string))...)
	path := filepath.Join(t.TempDir(), "golden.wal")
	if err := os.WriteFile(path, segmentBytes, 0o600); err != nil {
		t.Fatal(err)
	}

	verified, err := wal.VerifySealedSegment(path)
	if err != nil {
		t.Fatal(err)
	}
	if verified.StartSequence != 1 || verified.LastSequence != 1 || verified.EntryCount != 1 {
		t.Fatalf("unexpected golden segment range: %+v", verified)
	}
	if hex.EncodeToString(verified.ChainRoot[:]) != expected["wal_entry_hash"].(string) {
		t.Fatalf("chain root = %x, want %s", verified.ChainRoot, expected["wal_entry_hash"])
	}
	if hex.EncodeToString(verified.TrailerFileSHA256[:]) != expected["file_sha256"].(string) {
		t.Fatalf("trailer file hash = %x, want %s", verified.TrailerFileSHA256, expected["file_sha256"])
	}
	if verified.ObjectSHA256 != sha256.Sum256(segmentBytes) {
		t.Fatal("sealed object hash does not cover the complete file")
	}
}

func TestWALSealsRotatesAndRecoversAcrossSegments(t *testing.T) {
	root := t.TempDir()
	fixture, err := fake.BatchFixture()
	if err != nil {
		t.Fatal(err)
	}
	store, err := wal.Open(root, "gateway-test-01")
	if err != nil {
		t.Fatal(err)
	}
	first, err := store.Append(fixture.Frame, 1710000000, 42)
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := store.Seal()
	if err != nil {
		t.Fatal(err)
	}
	if sealed.StartSequence != 1 || sealed.LastSequence != 1 || sealed.ChainRoot != first.EntryHash {
		t.Fatalf("unexpected first sealed segment: %+v", sealed)
	}
	second, err := store.Append(fixture.Frame, 1710000001, 43)
	if err != nil {
		t.Fatal(err)
	}
	if second.Sequence != 2 || second.PreviousEntryHash != first.EntryHash {
		t.Fatalf("second segment did not continue the chain: %+v", second)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := wal.Open(root, "gateway-test-01")
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if reopened.Count() != 2 {
		t.Fatalf("reopened WAL count = %d, want 2", reopened.Count())
	}
	sequence, rootHash := reopened.Last()
	if sequence != 2 || rootHash != second.EntryHash {
		t.Fatalf("reopened WAL last = (%d, %x)", sequence, rootHash)
	}
	segments := reopened.SealedSegments()
	if len(segments) != 1 || segments[0].ObjectSHA256 != sealed.ObjectSHA256 {
		t.Fatalf("unexpected sealed inventory: %+v", segments)
	}
}

func TestWALRecoversPartialTrailerAsActive(t *testing.T) {
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
	path := store.Path()
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write(append([]byte("TWTR"), make([]byte, 28)...)); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := wal.Open(root, "gateway-test-01")
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if after.Size() != before.Size() || reopened.Count() != 1 {
		t.Fatalf("partial trailer recovery size=%d count=%d", after.Size(), reopened.Count())
	}
	if len(reopened.SealedSegments()) != 0 {
		t.Fatal("partial trailer must not seal the active WAL")
	}
}

func TestWALDoesNotTreatEntryPayloadMagicAsSealedTrailer(t *testing.T) {
	root := t.TempDir()
	fixture, err := fake.BatchFixture()
	if err != nil {
		t.Fatal(err)
	}
	frame, err := protocol.DecodeFrame(fixture.Frame)
	if err != nil {
		t.Fatal(err)
	}
	message, err := protocol.DecodeMessage(frame)
	if err != nil {
		t.Fatal(err)
	}
	batch := message.(protocol.BatchFrameV1)
	batch.Records[0].Flags = 0x52545754
	raw, err := protocol.EncodeMessage(batch)
	if err != nil {
		t.Fatal(err)
	}
	store, err := wal.Open(root, "gateway-test-01")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(raw, 1710000000, 42); err != nil {
		t.Fatal(err)
	}
	activePath := store.Path()
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	activeBytes, err := os.ReadFile(activePath)
	if err != nil {
		t.Fatal(err)
	}
	if string(activeBytes[len(activeBytes)-96:len(activeBytes)-92]) != "TWTR" {
		t.Fatal("test frame does not place TWTR 96 bytes before active WAL end")
	}

	reopened, err := wal.Open(root, "gateway-test-01")
	if err != nil {
		t.Fatalf("valid active WAL was mistaken for a sealed segment: %v", err)
	}
	defer reopened.Close()
	if reopened.Count() != 1 || len(reopened.SealedSegments()) != 0 {
		t.Fatalf(
			"valid active WAL recovery count=%d sealed=%d",
			reopened.Count(),
			len(reopened.SealedSegments()),
		)
	}
}

func TestWALCompletesRotationWhenSealedTrailerRemainsActive(t *testing.T) {
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
	sealed, err := store.Seal()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	activePath := filepath.Join(root, "active.wal")
	if err := os.Remove(activePath); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(sealed.Path, activePath); err != nil {
		t.Fatal(err)
	}

	reopened, err := wal.Open(root, "gateway-test-01")
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	segments := reopened.SealedSegments()
	if len(segments) != 1 || segments[0].ObjectSHA256 != sealed.ObjectSHA256 {
		t.Fatalf("rotation recovery did not restore sealed inventory: %+v", segments)
	}
	if _, err := os.Stat(activePath); err != nil {
		t.Fatalf("rotation recovery did not create next active WAL: %v", err)
	}
	next, err := reopened.Append(fixture.Frame, 1710000001, 43)
	if err != nil {
		t.Fatal(err)
	}
	if next.Sequence != 2 || next.PreviousEntryHash != sealed.ChainRoot {
		t.Fatalf("recovered active WAL did not continue the chain: %+v", next)
	}
}

func TestWALRecoversLinkCompletedBeforeActiveRemoval(t *testing.T) {
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
	sealed, err := store.Seal()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	activePath := filepath.Join(root, "active.wal")
	if err := os.Remove(activePath); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(sealed.Path, activePath); err != nil {
		t.Fatal(err)
	}

	reopened, err := wal.Open(root, "gateway-test-01")
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if reopened.Count() != 1 || len(reopened.SealedSegments()) != 1 {
		t.Fatalf(
			"duplicate-link recovery inventory count=%d sealed=%d",
			reopened.Count(),
			len(reopened.SealedSegments()),
		)
	}
	next, err := reopened.Append(fixture.Frame, 1710000001, 43)
	if err != nil {
		t.Fatal(err)
	}
	if next.Sequence != 2 || next.PreviousEntryHash != sealed.ChainRoot {
		t.Fatalf("duplicate-link recovery did not continue the chain: %+v", next)
	}
}

func TestWALRecreatesIncompleteNextActiveHeader(t *testing.T) {
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
	sealed, err := store.Seal()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	activePath := filepath.Join(root, "active.wal")
	header, err := os.ReadFile(activePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(activePath, header[:32], 0o600); err != nil {
		t.Fatal(err)
	}

	reopened, err := wal.Open(root, "gateway-test-01")
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	next, err := reopened.Append(fixture.Frame, 1710000001, 43)
	if err != nil {
		t.Fatal(err)
	}
	if next.Sequence != 2 || next.PreviousEntryHash != sealed.ChainRoot {
		t.Fatalf("recreated active WAL did not continue sealed chain: %+v", next)
	}
}

func TestWALDoesNotRepairInvalidShortActiveHeader(t *testing.T) {
	root := t.TempDir()
	store, err := wal.Open(root, "gateway-test-01")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "active.wal"), []byte("BROKEN"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := wal.Open(root, "gateway-test-01"); err == nil || !errors.Is(err, wal.ErrIntegrity) {
		t.Fatalf("expected invalid short header integrity stop, got %v", err)
	}
}

func TestVerifySealedSegmentStopsOnTrailerMutation(t *testing.T) {
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
	sealed, err := store.Seal()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(sealed.Path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Seek(-5, 2); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if _, err := file.Write([]byte{0xff}); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := wal.VerifySealedSegment(sealed.Path); err == nil || !errors.Is(err, wal.ErrIntegrity) {
		t.Fatalf("expected trailer integrity error, got %v", err)
	}
	if _, err := wal.Open(root, "gateway-test-01"); err == nil || !errors.Is(err, wal.ErrIntegrity) {
		t.Fatalf("expected startup integrity stop, got %v", err)
	}
}

func TestWALSealDoesNotOverwriteDifferentSegmentAtSameRange(t *testing.T) {
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

	other, err := wal.Open(t.TempDir(), "gateway-test-01")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := other.Append(fixture.Frame, 1710000999, 999); err != nil {
		t.Fatal(err)
	}
	otherSegment, err := other.Seal()
	if err != nil {
		t.Fatal(err)
	}
	if err := other.Close(); err != nil {
		t.Fatal(err)
	}
	differentBytes, err := os.ReadFile(otherSegment.Path)
	if err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(
		root,
		"sealed",
		"segment-00000000000000000001-00000000000000000001.wal",
	)
	if err := os.WriteFile(destination, differentBytes, 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := store.Seal(); err == nil || !errors.Is(err, wal.ErrIntegrity) {
		t.Fatalf("Seal error = %v, want ErrIntegrity", err)
	}
	remaining, err := os.ReadFile(destination)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(remaining, differentBytes) {
		t.Fatal("failed seal overwrote a different segment")
	}
}

func TestWALDoesNotSealEmptyActiveSegment(t *testing.T) {
	store, err := wal.Open(t.TempDir(), "gateway-test-01")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := store.Seal(); !errors.Is(err, wal.ErrEmptySegment) {
		t.Fatalf("Seal error = %v, want ErrEmptySegment", err)
	}
}

func decodeHex(t *testing.T, value string) []byte {
	t.Helper()
	decoded, err := hex.DecodeString(value)
	if err != nil {
		t.Fatal(err)
	}
	return decoded
}
