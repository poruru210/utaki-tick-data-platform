package wal

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestEncodeTrailerMatchesProtocolV1GoldenFixture(t *testing.T) {
	var fixture map[string]any
	data, err := os.ReadFile(filepath.Join("..", "..", "testdata", "tickdata", "golden", "wal-entry-v1.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	expectedHashes := fixture["expected_hashes"].(map[string]any)
	chainRootBytes, err := hex.DecodeString(expectedHashes["wal_entry_hash"].(string))
	if err != nil {
		t.Fatal(err)
	}
	fileHashBytes, err := hex.DecodeString(expectedHashes["file_sha256"].(string))
	if err != nil {
		t.Fatal(err)
	}
	var chainRoot [32]byte
	var fileHash [32]byte
	copy(chainRoot[:], chainRootBytes)
	copy(fileHash[:], fileHashBytes)
	got := encodeTrailer(1, 1, 1, chainRoot, fileHash)
	want, err := hex.DecodeString(fixture["trailer_hex"].(string))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("encoded trailer differs from Protocol V1 golden fixture:\n got %x\nwant %x", got, want)
	}
}
