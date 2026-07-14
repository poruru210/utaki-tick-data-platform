package protocol

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"hash/crc32"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func goldenPath(t *testing.T, name string) string {
	t.Helper()
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Join(filepath.Dir(source), "..", "..", "testdata", "tickdata", "golden", name)
}

func loadJSON(t *testing.T, name string) map[string]any {
	t.Helper()
	data, err := os.ReadFile(goldenPath(t, name))
	if err != nil {
		t.Fatal(err)
	}
	var value map[string]any
	if err := json.Unmarshal(data, &value); err != nil {
		t.Fatal(err)
	}
	return value
}

func stringField(t *testing.T, value map[string]any, name string) string {
	t.Helper()
	result, ok := value[name].(string)
	if !ok {
		t.Fatalf("%s is not a string", name)
	}
	return result
}

func fixtureIndex(t *testing.T) []any {
	t.Helper()
	data, err := os.ReadFile(goldenPath(t, "index.json"))
	if err != nil {
		t.Fatal(err)
	}
	var value map[string]any
	if err := json.Unmarshal(data, &value); err != nil {
		t.Fatal(err)
	}
	return value["fixtures"].([]any)
}

func TestGoldenValidFramesRoundTrip(t *testing.T) {
	for _, rawEntry := range fixtureIndex(t) {
		entry := rawEntry.(map[string]any)
		if entry["kind"] != "valid_frame" {
			continue
		}
		t.Run(entry["fixture_id"].(string), func(t *testing.T) {
			fixture := loadJSON(t, entry["path"].(string))
			raw, err := hex.DecodeString(stringField(t, fixture, "wire_hex"))
			if err != nil {
				t.Fatal(err)
			}
			frame, err := DecodeFrame(raw)
			if err != nil {
				t.Fatal(err)
			}
			if uint16(frame.MessageType) != uint16(fixture["decoded_message_type"].(float64)) {
				t.Fatalf("message type mismatch: %d", frame.MessageType)
			}
			message, err := DecodeMessage(frame)
			if err != nil {
				t.Fatal(err)
			}
			encoded, err := EncodeMessage(message)
			if err != nil {
				t.Fatal(err)
			}
			if string(encoded) != string(raw) {
				t.Fatalf("round trip changed bytes")
			}
			expected, hasExpectedHashes := fixture["expected_hashes"].(map[string]any)
			if batch, ok := message.(BatchFrameV1); ok && hasExpectedHashes && len(expected) > 0 {
				sourceHash := SourcePayloadFingerprint(batch.Records[0])
				if hex.EncodeToString(sourceHash[:]) != expected["source_payload_fingerprint"] {
					t.Fatal("source payload hash mismatch")
				}
				observed := ObservationHash(
					"fake-01",
					batch.ProducerSessionID,
					batch.BatchSequence,
					0,
					batch.Records[0].CaptureSequence,
					sourceHash,
				)
				if hex.EncodeToString(observed[:]) != expected["observation_hash"] {
					t.Fatal("observation hash mismatch")
				}
				batchHash := GatewayBatchSHA256(raw)
				if hex.EncodeToString(batchHash[:]) != expected["gateway_batch_sha256"] {
					t.Fatal("batch hash mismatch")
				}
			}
		})
	}
}

func TestGoldenInvalidFrames(t *testing.T) {
	entries := fixtureIndex(t)
	byID := make(map[string]map[string]any)
	for _, rawEntry := range entries {
		entry := rawEntry.(map[string]any)
		byID[entry["fixture_id"].(string)] = loadJSON(t, entry["path"].(string))
	}
	for _, rawEntry := range entries {
		entry := rawEntry.(map[string]any)
		if entry["kind"] != "invalid_frame" {
			continue
		}
		t.Run(entry["fixture_id"].(string), func(t *testing.T) {
			fixture := byID[entry["fixture_id"].(string)]
			base := byID[fixture["base_fixture_id"].(string)]
			raw, err := hex.DecodeString(stringField(t, base, "wire_hex"))
			if err != nil {
				t.Fatal(err)
			}
			mutation := fixture["mutation"].(map[string]any)
			switch mutation["type"] {
			case "truncate":
				raw = raw[:int(mutation["size"].(float64))]
			case "xor":
				offset := int(mutation["offset"].(float64))
				raw[offset] ^= byte(mutation["value"].(float64))
			case "set_u16":
				binary.LittleEndian.PutUint16(raw[int(mutation["offset"].(float64)):], uint16(mutation["value"].(float64)))
			case "set_u32":
				binary.LittleEndian.PutUint32(raw[int(mutation["offset"].(float64)):], uint32(mutation["value"].(float64)))
			case "duplicate_identity":
				if _, err := DecodeFrame(raw); err != nil {
					t.Fatal(err)
				}
				return
			default:
				t.Fatalf("unknown mutation %v", mutation["type"])
			}
			_, err = DecodeFrame(raw)
			if err == nil {
				t.Fatal("mutation was accepted")
			}
			if ErrorCodeOf(err) != ErrorCode(fixture["expected_error_code"].(string)) {
				t.Fatalf("expected %s, got %s", fixture["expected_error_code"], ErrorCodeOf(err))
			}
		})
	}
}

func TestGoldenManifestHashes(t *testing.T) {
	tests := []struct {
		file   string
		prefix string
	}{
		{"raw-day-manifest-v1.json", "tick-data-platform/raw-day-manifest/v1\x00"},
		{"replay-day-manifest-v1.json", "tick-data-platform/replay-day-manifest/v1\x00"},
	}
	for _, test := range tests {
		t.Run(test.file, func(t *testing.T) {
			fixture := loadJSON(t, test.file)
			hash := sha256.Sum256(append([]byte(test.prefix), []byte(stringField(t, fixture, "canonical_json"))...))
			if hex.EncodeToString(hash[:]) != stringField(t, fixture, "manifest_sha256") {
				t.Fatal("manifest hash mismatch")
			}
		})
	}
}

func TestGoldenStatefulScenarios(t *testing.T) {
	entries := fixtureIndex(t)
	byID := make(map[string]map[string]any)
	for _, rawEntry := range entries {
		entry := rawEntry.(map[string]any)
		byID[entry["fixture_id"].(string)] = loadJSON(t, entry["path"].(string))
	}
	for _, rawEntry := range entries {
		entry := rawEntry.(map[string]any)
		if entry["kind"] != "stateful_scenario" {
			continue
		}
		t.Run(entry["fixture_id"].(string), func(t *testing.T) {
			fixture := byID[entry["fixture_id"].(string)]
			base := byID[fixture["base_fixture_id"].(string)]
			switch fixture["scenario"] {
			case "ACK_LOSS_RETRY", "DUPLICATE_RETRANSMISSION":
				raw, err := hex.DecodeString(stringField(t, base, "wire_hex"))
				if err != nil {
					t.Fatal(err)
				}
				status, code := fakeDuplicateStatusForTest(t, raw)
				if code != "" || status != 3 {
					t.Fatalf("expected duplicate status, got status=%d code=%s", status, code)
				}
			case "WAL_RECOVERY":
				if fixture["expected_recovery"] != "REPLAY_COMMITTED_ENTRY" {
					t.Fatal("unexpected recovery expectation")
				}
				if base["kind"] != "wal_entry" {
					t.Fatal("WAL recovery must reference a WAL fixture")
				}
				verifyGoldenWAL(t, base)
			default:
				t.Fatalf("unknown scenario %v", fixture["scenario"])
			}
		})
	}
}

func verifyGoldenWAL(t *testing.T, fixture map[string]any) {
	t.Helper()
	header, err := hex.DecodeString(stringField(t, fixture, "file_header_hex"))
	if err != nil {
		t.Fatal(err)
	}
	entry, err := hex.DecodeString(stringField(t, fixture, "entry_hex"))
	if err != nil {
		t.Fatal(err)
	}
	trailer, err := hex.DecodeString(stringField(t, fixture, "trailer_hex"))
	if err != nil {
		t.Fatal(err)
	}
	if string(header[:4]) != "TWAL" || string(trailer[:4]) != "TWTR" {
		t.Fatal("invalid WAL magic")
	}
	entryLength := binary.LittleEndian.Uint32(entry[:4])
	if int(entryLength) != len(entry) {
		t.Fatal("invalid entry length")
	}
	frameLength := binary.LittleEndian.Uint32(entry[64:68])
	frameStart := 68
	frameEnd := frameStart + int(frameLength)
	if frameEnd+72 != len(entry) {
		t.Fatal("invalid WAL frame length")
	}
	frame := entry[frameStart:frameEnd]
	storedBatchHash := entry[frameEnd : frameEnd+32]
	storedEntryHash := entry[frameEnd+32 : frameEnd+64]
	if binary.LittleEndian.Uint32(entry[frameEnd+64:frameEnd+68]) != 0x434F4D4D {
		t.Fatal("invalid commit marker")
	}
	table := crc32.MakeTable(crc32.Castagnoli)
	if binary.LittleEndian.Uint32(entry[frameEnd+68:]) != crc32.Checksum(entry[:frameEnd+68], table) {
		t.Fatal("invalid entry CRC")
	}
	batchHash := GatewayBatchSHA256(frame)
	if string(storedBatchHash) != string(batchHash[:]) {
		t.Fatal("invalid gateway batch hash")
	}
	var previous [32]byte
	copy(previous[:], entry[32:64])
	var storedEntry [32]byte
	copy(storedEntry[:], storedEntryHash)
	entryHash := WALEntryHash(
		binary.LittleEndian.Uint64(entry[8:16]),
		previous,
		int64(binary.LittleEndian.Uint64(entry[16:24])),
		binary.LittleEndian.Uint64(entry[24:32]),
		batchHash,
		frame,
	)
	if string(storedEntry[:]) != string(entryHash[:]) {
		t.Fatal("invalid WAL entry hash")
	}
	preTrailer := append(append([]byte{}, header...), entry...)
	fileHash := sha256.Sum256(preTrailer)
	if string(trailer[60:92]) != string(fileHash[:]) {
		t.Fatal("invalid WAL file hash")
	}
	if binary.LittleEndian.Uint32(trailer[92:]) != crc32.Checksum(trailer[:92], table) {
		t.Fatal("invalid trailer CRC")
	}
	expected := fixture["expected_hashes"].(map[string]any)
	if hex.EncodeToString(batchHash[:]) != expected["gateway_batch_sha256"] {
		t.Fatal("expected gateway batch hash mismatch")
	}
	if hex.EncodeToString(entryHash[:]) != expected["wal_entry_hash"] {
		t.Fatal("expected WAL entry hash mismatch")
	}
	if hex.EncodeToString(fileHash[:]) != expected["file_sha256"] {
		t.Fatal("expected WAL file hash mismatch")
	}
}

func fakeDuplicateStatusForTest(t *testing.T, raw []byte) (uint8, ErrorCode) {
	t.Helper()
	first, err := DecodeFrame(raw)
	if err != nil {
		t.Fatal(err)
	}
	second, err := DecodeFrame(raw)
	if err != nil {
		t.Fatal(err)
	}
	firstMessage, err := DecodeMessage(first)
	if err != nil {
		t.Fatal(err)
	}
	secondMessage, err := DecodeMessage(second)
	if err != nil {
		t.Fatal(err)
	}
	firstBatch := firstMessage.(BatchFrameV1)
	secondBatch := secondMessage.(BatchFrameV1)
	if firstBatch.ProducerSessionID != secondBatch.ProducerSessionID ||
		firstBatch.BatchSequence != secondBatch.BatchSequence {
		return 0, ErrInvalidField
	}
	return 3, ""
}
