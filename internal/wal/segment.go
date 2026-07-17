package wal

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"unicode/utf8"

	"tick-data-platform/internal/protocol"
)

const (
	trailerMagic          = "TWTR"
	trailerVersion uint16 = 1
	trailerBytes          = 96
)

var errSealedTrailerAtEntryBoundary = errors.New("sealed WAL trailer starts at an entry boundary")

// VerifiedSegment describes a byte-exact sealed GatewayWalSegmentV1.
type VerifiedSegment struct {
	Path              string
	GatewayInstanceID string
	StartSequence     uint64
	LastSequence      uint64
	EntryCount        uint32
	ChainStart        [32]byte
	ChainRoot         [32]byte
	TrailerFileSHA256 [32]byte
	ObjectSHA256      [32]byte
	FileBytes         int64
	Entries           []Entry
}

type walHeader struct {
	length        int
	startSequence uint64
	gatewayID     string
}

// VerifySealedSegment reopens and verifies every byte covered by the Protocol V1
// sealed WAL contract. It validates the segment-local chain and returns the
// predecessor anchor as ChainStart so a caller can validate cross-segment order.
func VerifySealedSegment(path string) (VerifiedSegment, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return VerifiedSegment{}, fmt.Errorf("read sealed WAL segment: %w", err)
	}
	return verifySealedBytes(path, data)
}

// VerifySealedBytes verifies a sealed WAL object already held by a bounded
// reader. The path is only an audit label; no filesystem access is performed.
// This keeps remote retention observation from materializing untrusted bytes
// into a caller-selected local path before the WAL contract is checked.
func VerifySealedBytes(path string, data []byte) (VerifiedSegment, error) {
	return verifySealedBytes(path, data)
}

func verifySealedBytes(path string, data []byte) (VerifiedSegment, error) {
	if len(data) < 30+trailerBytes {
		return VerifiedSegment{}, fmt.Errorf("%w: sealed WAL is too short", ErrIntegrity)
	}
	trailerOffset := len(data) - trailerBytes
	trailer := data[trailerOffset:]
	if string(trailer[:4]) != trailerMagic {
		return VerifiedSegment{}, fmt.Errorf("%w: sealed WAL trailer magic is missing", ErrIntegrity)
	}
	if binary.LittleEndian.Uint16(trailer[4:6]) != trailerVersion ||
		binary.LittleEndian.Uint16(trailer[6:8]) != trailerBytes {
		return VerifiedSegment{}, fmt.Errorf("%w: unsupported sealed WAL trailer", ErrIntegrity)
	}
	wantTrailerCRC := binary.LittleEndian.Uint32(trailer[92:96])
	gotTrailerCRC := crc32.Checksum(trailer[:92], crc32.MakeTable(crc32.Castagnoli))
	if wantTrailerCRC != gotTrailerCRC {
		return VerifiedSegment{}, fmt.Errorf("%w: sealed WAL trailer CRC mismatch", ErrIntegrity)
	}

	header, err := parseHeader(data[:trailerOffset], "")
	if err != nil {
		return VerifiedSegment{}, err
	}
	entries, entriesEnd, partial, err := parseEntries(data[:trailerOffset], header, nil, false)
	if err != nil {
		return VerifiedSegment{}, err
	}
	if partial || entriesEnd != trailerOffset || len(entries) == 0 {
		return VerifiedSegment{}, fmt.Errorf("%w: sealed WAL entry region is incomplete", ErrIntegrity)
	}

	firstSequence := binary.LittleEndian.Uint64(trailer[8:16])
	lastSequence := binary.LittleEndian.Uint64(trailer[16:24])
	entryCount := binary.LittleEndian.Uint32(trailer[24:28])
	var chainRoot [32]byte
	copy(chainRoot[:], trailer[28:60])
	var trailerFileHash [32]byte
	copy(trailerFileHash[:], trailer[60:92])
	gotTrailerFileHash := sha256.Sum256(data[:trailerOffset])

	if firstSequence != header.startSequence || firstSequence != entries[0].Sequence {
		return VerifiedSegment{}, fmt.Errorf("%w: sealed WAL first sequence mismatch", ErrIntegrity)
	}
	if lastSequence < firstSequence {
		return VerifiedSegment{}, fmt.Errorf("%w: sealed WAL sequence range is reversed", ErrIntegrity)
	}
	if lastSequence != entries[len(entries)-1].Sequence {
		return VerifiedSegment{}, fmt.Errorf("%w: sealed WAL last sequence mismatch", ErrIntegrity)
	}
	if uint64(lastSequence-firstSequence)+1 != uint64(entryCount) || int(entryCount) != len(entries) {
		return VerifiedSegment{}, fmt.Errorf("%w: sealed WAL entry count mismatch", ErrIntegrity)
	}
	if chainRoot != entries[len(entries)-1].EntryHash {
		return VerifiedSegment{}, fmt.Errorf("%w: sealed WAL chain root mismatch", ErrIntegrity)
	}
	if trailerFileHash != gotTrailerFileHash {
		return VerifiedSegment{}, fmt.Errorf("%w: sealed WAL file hash mismatch", ErrIntegrity)
	}

	return VerifiedSegment{
		Path:              path,
		GatewayInstanceID: header.gatewayID,
		StartSequence:     firstSequence,
		LastSequence:      lastSequence,
		EntryCount:        entryCount,
		ChainStart:        entries[0].PreviousEntryHash,
		ChainRoot:         chainRoot,
		TrailerFileSHA256: trailerFileHash,
		ObjectSHA256:      sha256.Sum256(data),
		FileBytes:         int64(len(data)),
		Entries:           entries,
	}, nil
}

func parseHeader(data []byte, expectedGatewayID string) (walHeader, error) {
	if len(data) < 30 || string(data[:4]) != fileMagic {
		return walHeader{}, fmt.Errorf("%w: invalid WAL header", ErrIntegrity)
	}
	if binary.LittleEndian.Uint16(data[4:6]) != walSchemaVersion {
		return walHeader{}, fmt.Errorf("%w: unsupported WAL schema", ErrIntegrity)
	}
	headerLength := int(binary.LittleEndian.Uint16(data[6:8]))
	idLength := int(binary.LittleEndian.Uint16(data[28:30]))
	if headerLength != 30+idLength || headerLength > len(data) || idLength == 0 || idLength > 255 {
		return walHeader{}, fmt.Errorf("%w: invalid WAL header length", ErrIntegrity)
	}
	if binary.LittleEndian.Uint32(data[24:28]) != 0 {
		return walHeader{}, fmt.Errorf("%w: unsupported WAL header flags", ErrIntegrity)
	}
	gatewayIDBytes := data[30:headerLength]
	if !utf8.Valid(gatewayIDBytes) {
		return walHeader{}, fmt.Errorf("%w: WAL gateway identity is not UTF-8", ErrIntegrity)
	}
	gatewayID := string(gatewayIDBytes)
	if expectedGatewayID != "" && gatewayID != expectedGatewayID {
		return walHeader{}, fmt.Errorf("%w: WAL gateway identity mismatch", ErrIntegrity)
	}
	startSequence := binary.LittleEndian.Uint64(data[8:16])
	if startSequence == 0 {
		return walHeader{}, fmt.Errorf("%w: invalid WAL segment start sequence", ErrIntegrity)
	}
	return walHeader{
		length:        headerLength,
		startSequence: startSequence,
		gatewayID:     gatewayID,
	}, nil
}

func parseEntries(
	data []byte,
	header walHeader,
	expectedPrevious *[32]byte,
	allowPartial bool,
) ([]Entry, int, bool, error) {
	offset := header.length
	expectedSequence := header.startSequence
	var previous [32]byte
	previousKnown := false
	if expectedPrevious != nil {
		previous = *expectedPrevious
		previousKnown = true
	}
	var entries []Entry

	for offset < len(data) {
		remaining := data[offset:]
		if allowPartial &&
			len(remaining) == trailerBytes &&
			string(remaining[:4]) == trailerMagic {
			return entries, offset, false, errSealedTrailerAtEntryBoundary
		}
		if allowPartial && isPartialTrailer(remaining) {
			return entries, offset, true, nil
		}
		if len(remaining) >= 4 && string(remaining[:4]) == trailerMagic {
			return nil, offset, false, fmt.Errorf("%w: unexpected or invalid WAL trailer", ErrIntegrity)
		}
		if len(remaining) < 4 {
			if allowPartial {
				return entries, offset, true, nil
			}
			return nil, offset, false, fmt.Errorf("%w: incomplete WAL entry length", ErrIntegrity)
		}
		entryLength := int(binary.LittleEndian.Uint32(remaining[:4]))
		if entryLength < entryFixedBytes || entryLength > entryFixedBytes+int(protocol.MaxFrameBytes) {
			return nil, offset, false, fmt.Errorf("%w: invalid entry length at offset %d", ErrIntegrity, offset)
		}
		if len(remaining) < entryLength {
			if allowPartial {
				return entries, offset, true, nil
			}
			return nil, offset, false, fmt.Errorf("%w: incomplete WAL entry at offset %d", ErrIntegrity, offset)
		}
		entry, err := parseEntry(remaining[:entryLength])
		if err != nil {
			return nil, offset, false, fmt.Errorf("%w: entry at offset %d: %v", ErrIntegrity, offset, err)
		}
		if entry.Sequence != expectedSequence {
			return nil, offset, false, fmt.Errorf(
				"%w: expected sequence %d, got %d",
				ErrIntegrity,
				expectedSequence,
				entry.Sequence,
			)
		}
		if !previousKnown {
			previous = entry.PreviousEntryHash
			previousKnown = true
		}
		if entry.PreviousEntryHash != previous {
			return nil, offset, false, fmt.Errorf(
				"%w: previous hash mismatch at sequence %d",
				ErrIntegrity,
				entry.Sequence,
			)
		}
		entry.Offset = int64(offset)
		entries = append(entries, entry)
		previous = entry.EntryHash
		expectedSequence++
		offset += entryLength
	}
	return entries, offset, false, nil
}

func isPartialTrailer(remaining []byte) bool {
	magic := []byte(trailerMagic)
	if len(remaining) < len(magic) {
		return bytes.Equal(remaining, magic[:len(remaining)])
	}
	return bytes.Equal(remaining[:len(magic)], magic) && len(remaining) < trailerBytes
}

func encodeTrailer(
	firstSequence uint64,
	lastSequence uint64,
	entryCount uint32,
	chainRoot [32]byte,
	fileHash [32]byte,
) []byte {
	trailer := make([]byte, trailerBytes)
	copy(trailer[:4], trailerMagic)
	binary.LittleEndian.PutUint16(trailer[4:6], trailerVersion)
	binary.LittleEndian.PutUint16(trailer[6:8], trailerBytes)
	binary.LittleEndian.PutUint64(trailer[8:16], firstSequence)
	binary.LittleEndian.PutUint64(trailer[16:24], lastSequence)
	binary.LittleEndian.PutUint32(trailer[24:28], entryCount)
	copy(trailer[28:60], chainRoot[:])
	copy(trailer[60:92], fileHash[:])
	crc := crc32.Checksum(trailer[:92], crc32.MakeTable(crc32.Castagnoli))
	binary.LittleEndian.PutUint32(trailer[92:96], crc)
	return trailer
}
