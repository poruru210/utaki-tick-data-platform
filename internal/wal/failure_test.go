package wal

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"testing"

	"tick-data-platform/internal/protocol"
	"tick-data-platform/producers/fake"
)

func TestAppendPoisonsStoreAfterWALSyncFailure(t *testing.T) {
	root := t.TempDir()
	fixture, err := fake.BatchFixture()
	if err != nil {
		t.Fatal(err)
	}
	store, err := Open(root, "gateway-test-01")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	firstSync := true
	store.syncFile = func() error {
		if firstSync {
			firstSync = false
			return errors.New("injected sync failure")
		}
		return store.file.Sync()
	}
	if _, err := store.Append(fixture.Frame, 1710000000, 42); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("first append error = %v, want ErrUnavailable", err)
	}
	if store.Count() != 0 {
		t.Fatalf("failed append changed in-memory entry count: %d", store.Count())
	}
	if _, err := store.Append(fixture.Frame, 1710000001, 43); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("second append error = %v, want ErrUnavailable", err)
	}
}

func TestOpenStopsOnIndividuallyValidCrossSegmentChainMismatch(t *testing.T) {
	root := t.TempDir()
	fixture, err := fake.BatchFixture()
	if err != nil {
		t.Fatal(err)
	}
	store, err := Open(root, "gateway-test-01")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(fixture.Frame, 1710000000, 42); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Seal(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(fixture.Frame, 1710000001, 43); err != nil {
		t.Fatal(err)
	}
	second, err := store.Seal()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(second.Path)
	if err != nil {
		t.Fatal(err)
	}
	headerLength := int(binary.LittleEndian.Uint16(data[6:8]))
	entryLength := int(binary.LittleEndian.Uint32(data[headerLength : headerLength+4]))
	entry := data[headerLength : headerLength+entryLength]
	for i := 32; i < 64; i++ {
		entry[i] = 0
	}
	frameLength := int(binary.LittleEndian.Uint32(entry[64:68]))
	frameEnd := 68 + frameLength
	var previous [32]byte
	var batchHash [32]byte
	copy(batchHash[:], entry[frameEnd:frameEnd+32])
	entryHash := protocol.WALEntryHash(
		binary.LittleEndian.Uint64(entry[8:16]),
		previous,
		int64(binary.LittleEndian.Uint64(entry[16:24])),
		binary.LittleEndian.Uint64(entry[24:32]),
		batchHash,
		entry[68:frameEnd],
	)
	copy(entry[frameEnd+32:frameEnd+64], entryHash[:])
	entryCRC := crc32.Checksum(entry[:len(entry)-4], crc32.MakeTable(crc32.Castagnoli))
	binary.LittleEndian.PutUint32(entry[len(entry)-4:], entryCRC)

	trailerOffset := len(data) - trailerBytes
	trailer := data[trailerOffset:]
	copy(trailer[28:60], entryHash[:])
	fileHash := sha256.Sum256(data[:trailerOffset])
	copy(trailer[60:92], fileHash[:])
	trailerCRC := crc32.Checksum(trailer[:92], crc32.MakeTable(crc32.Castagnoli))
	binary.LittleEndian.PutUint32(trailer[92:96], trailerCRC)
	if err := os.WriteFile(second.Path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := VerifySealedSegment(second.Path); err != nil {
		t.Fatalf("mutated segment must remain locally valid: %v", err)
	}
	if _, err := Open(root, "gateway-test-01"); err == nil || !errors.Is(err, ErrIntegrity) {
		t.Fatalf("expected cross-segment integrity stop, got %v", err)
	}
}
