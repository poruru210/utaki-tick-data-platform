// Package wal owns the append-only local WAL and its crash-recovery boundary.
package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
	"unicode/utf8"

	"tick-data-platform/internal/protocol"
)

const (
	walSchemaVersion uint16 = 1
	entryVersion     uint16 = 1
	entryFixedBytes         = 140
	commitMarker     uint32 = 0x434F4D4D
	fileMagic               = "TWAL"
)

var ErrIntegrity = errors.New("gateway WAL integrity failure")
var ErrUnavailable = errors.New("gateway WAL is unavailable; reopen required")

type Entry struct {
	Offset             int64
	Sequence           uint64
	ReceiveWallS       int64
	ReceiveMonotonicUS uint64
	PreviousEntryHash  [32]byte
	Frame              []byte
	BatchHash          [32]byte
	EntryHash          [32]byte
}

type Store struct {
	mu        sync.Mutex
	root      string
	path      string
	gatewayID string
	file      *os.File
	syncFile  func() error
	statFile  func() (os.FileInfo, error)
	entries   []Entry
	last      [32]byte
	next      uint64
	start     uint64
	fileBytes int64
	poisoned  bool
}

func Open(root, gatewayID string) (*Store, error) {
	if root == "" {
		return nil, fmt.Errorf("WAL root is empty")
	}
	if gatewayID == "" || !utf8.ValidString(gatewayID) || len(gatewayID) > 255 {
		return nil, fmt.Errorf("invalid gateway instance id")
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, fmt.Errorf("create WAL root: %w", err)
	}
	path := filepath.Join(root, "active.wal")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open WAL: %w", err)
	}
	store := &Store{
		root:      root,
		path:      path,
		gatewayID: gatewayID,
		file:      file,
		syncFile:  file.Sync,
		statFile:  file.Stat,
		start:     1,
		next:      1,
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("stat WAL: %w", err)
	}
	if info.Size() == 0 {
		if err := store.writeHeader(time.Now().Unix()); err != nil {
			_ = file.Close()
			return nil, err
		}
	} else if err := store.load(); err != nil {
		_ = file.Close()
		return nil, err
	}
	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("seek WAL end: %w", err)
	}
	return store, nil
}

func (s *Store) Path() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.path
}

func (s *Store) Entries() []Entry {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]Entry, len(s.entries))
	for i, entry := range s.entries {
		result[i] = entry
		result[i].Frame = append([]byte(nil), entry.Frame...)
	}
	return result
}

func (s *Store) Last() (uint64, [32]byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.entries) == 0 {
		return 0, [32]byte{}
	}
	return s.entries[len(s.entries)-1].Sequence, s.last
}

func (s *Store) Count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.entries)
}

func (s *Store) FileBytes() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.fileBytes
}

func (s *Store) Append(frame []byte, receiveWallS int64, receiveMonotonicUS uint64) (Entry, error) {
	decoded, err := protocol.DecodeFrame(frame)
	if err != nil {
		return Entry{}, err
	}
	message, err := protocol.DecodeMessage(decoded)
	if err != nil {
		return Entry{}, err
	}
	if _, ok := message.(protocol.BatchFrameV1); !ok {
		return Entry{}, fmt.Errorf("WAL accepts BatchFrameV1 only")
	}
	if len(frame) < int(protocol.MinFrameBytes) || len(frame) > int(protocol.MaxFrameBytes) {
		return Entry{}, fmt.Errorf("invalid batch frame length %d", len(frame))
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.file == nil || s.poisoned {
		return Entry{}, ErrUnavailable
	}
	sequence := s.next
	previous := s.last
	batchHash := protocol.GatewayBatchSHA256(frame)
	entryHash := protocol.WALEntryHash(sequence, previous, receiveWallS, receiveMonotonicUS, batchHash, frame)
	entryLength := entryFixedBytes + len(frame)
	entryBytes := make([]byte, entryLength)
	binary.LittleEndian.PutUint32(entryBytes[0:4], uint32(entryLength))
	binary.LittleEndian.PutUint16(entryBytes[4:6], entryVersion)
	binary.LittleEndian.PutUint16(entryBytes[6:8], 0)
	binary.LittleEndian.PutUint64(entryBytes[8:16], sequence)
	binary.LittleEndian.PutUint64(entryBytes[16:24], uint64(receiveWallS))
	binary.LittleEndian.PutUint64(entryBytes[24:32], receiveMonotonicUS)
	copy(entryBytes[32:64], previous[:])
	binary.LittleEndian.PutUint32(entryBytes[64:68], uint32(len(frame)))
	copy(entryBytes[68:68+len(frame)], frame)
	frameEnd := 68 + len(frame)
	copy(entryBytes[frameEnd:frameEnd+32], batchHash[:])
	copy(entryBytes[frameEnd+32:frameEnd+64], entryHash[:])
	binary.LittleEndian.PutUint32(entryBytes[frameEnd+64:frameEnd+68], commitMarker)
	crc := crc32.Checksum(entryBytes[:len(entryBytes)-4], crc32.MakeTable(crc32.Castagnoli))
	binary.LittleEndian.PutUint32(entryBytes[len(entryBytes)-4:], crc)
	if err := writeAll(s.file, entryBytes); err != nil {
		s.poisoned = true
		return Entry{}, fmt.Errorf("%w: append WAL entry: %v", ErrUnavailable, err)
	}
	if err := s.syncFile(); err != nil {
		s.poisoned = true
		return Entry{}, fmt.Errorf("%w: sync WAL entry: %v", ErrUnavailable, err)
	}
	info, err := s.statFile()
	if err != nil {
		s.poisoned = true
		return Entry{}, fmt.Errorf("%w: stat WAL after append: %v", ErrUnavailable, err)
	}
	entry := Entry{
		Offset:             info.Size() - int64(entryLength),
		Sequence:           sequence,
		ReceiveWallS:       receiveWallS,
		ReceiveMonotonicUS: receiveMonotonicUS,
		PreviousEntryHash:  previous,
		Frame:              append([]byte(nil), frame...),
		BatchHash:          batchHash,
		EntryHash:          entryHash,
	}
	s.entries = append(s.entries, entry)
	s.last = entryHash
	s.next++
	s.fileBytes = info.Size()
	return entry, nil
}

func (s *Store) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.file == nil || s.poisoned {
		return ErrUnavailable
	}
	if err := s.syncFile(); err != nil {
		s.poisoned = true
		return fmt.Errorf("%w: sync WAL: %v", ErrUnavailable, err)
	}
	return nil
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.file == nil {
		return nil
	}
	if err := s.syncFile(); err != nil {
		_ = s.file.Close()
		s.file = nil
		return err
	}
	err := s.file.Close()
	s.file = nil
	return err
}

func (s *Store) writeHeader(createdWallS int64) error {
	headerLength := 30 + len(s.gatewayID)
	header := make([]byte, headerLength)
	copy(header[0:4], fileMagic)
	binary.LittleEndian.PutUint16(header[4:6], walSchemaVersion)
	binary.LittleEndian.PutUint16(header[6:8], uint16(headerLength))
	binary.LittleEndian.PutUint64(header[8:16], s.start)
	binary.LittleEndian.PutUint64(header[16:24], uint64(createdWallS))
	binary.LittleEndian.PutUint32(header[24:28], 0)
	binary.LittleEndian.PutUint16(header[28:30], uint16(len(s.gatewayID)))
	copy(header[30:], s.gatewayID)
	if err := writeAll(s.file, header); err != nil {
		return fmt.Errorf("write WAL header: %w", err)
	}
	if err := s.file.Sync(); err != nil {
		return fmt.Errorf("sync WAL header: %w", err)
	}
	s.fileBytes = int64(len(header))
	return nil
}

func (s *Store) load() error {
	if _, err := s.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek WAL start: %w", err)
	}
	data, err := io.ReadAll(s.file)
	if err != nil {
		return fmt.Errorf("read WAL: %w", err)
	}
	if len(data) < 30 || string(data[:4]) != fileMagic {
		return fmt.Errorf("%w: invalid WAL header", ErrIntegrity)
	}
	if binary.LittleEndian.Uint16(data[4:6]) != walSchemaVersion {
		return fmt.Errorf("%w: unsupported WAL schema", ErrIntegrity)
	}
	headerLength := int(binary.LittleEndian.Uint16(data[6:8]))
	idLength := int(binary.LittleEndian.Uint16(data[28:30]))
	if headerLength != 30+idLength || headerLength > len(data) || idLength > 255 {
		return fmt.Errorf("%w: invalid WAL header length", ErrIntegrity)
	}
	if binary.LittleEndian.Uint32(data[24:28]) != 0 || string(data[30:headerLength]) != s.gatewayID {
		return fmt.Errorf("%w: WAL gateway identity mismatch", ErrIntegrity)
	}
	s.start = binary.LittleEndian.Uint64(data[8:16])
	if s.start == 0 {
		return fmt.Errorf("%w: invalid WAL segment start sequence", ErrIntegrity)
	}
	s.next = s.start
	offset := headerLength
	expectedSequence := s.start
	var previous [32]byte
	for offset < len(data) {
		remaining := len(data) - offset
		if remaining < 4 {
			break
		}
		entryLength := int(binary.LittleEndian.Uint32(data[offset : offset+4]))
		if entryLength < entryFixedBytes || entryLength > entryFixedBytes+int(protocol.MaxFrameBytes) {
			return fmt.Errorf("%w: invalid entry length at offset %d", ErrIntegrity, offset)
		}
		if remaining < entryLength {
			break
		}
		entry, err := parseEntry(data[offset : offset+entryLength])
		if err != nil {
			return fmt.Errorf("%w: entry at offset %d: %v", ErrIntegrity, offset, err)
		}
		if entry.Sequence != expectedSequence {
			return fmt.Errorf("%w: expected sequence %d, got %d", ErrIntegrity, expectedSequence, entry.Sequence)
		}
		if entry.PreviousEntryHash != previous {
			return fmt.Errorf("%w: previous hash mismatch at sequence %d", ErrIntegrity, entry.Sequence)
		}
		entry.Offset = int64(offset)
		s.entries = append(s.entries, entry)
		previous = entry.EntryHash
		expectedSequence++
		offset += entryLength
	}
	if offset < len(data) {
		if err := s.file.Truncate(int64(offset)); err != nil {
			return fmt.Errorf("truncate incomplete WAL tail: %w", err)
		}
		if err := s.file.Sync(); err != nil {
			return fmt.Errorf("sync truncated WAL tail: %w", err)
		}
	}
	s.last = previous
	s.next = expectedSequence
	s.fileBytes = int64(offset)
	return nil
}

func parseEntry(raw []byte) (Entry, error) {
	if len(raw) < entryFixedBytes {
		return Entry{}, errors.New("entry is shorter than fixed layout")
	}
	if int(binary.LittleEndian.Uint32(raw[0:4])) != len(raw) {
		return Entry{}, errors.New("entry length mismatch")
	}
	if binary.LittleEndian.Uint16(raw[4:6]) != entryVersion || binary.LittleEndian.Uint16(raw[6:8]) != 0 {
		return Entry{}, errors.New("unsupported entry version or flags")
	}
	frameLength := int(binary.LittleEndian.Uint32(raw[64:68]))
	if frameLength < int(protocol.MinFrameBytes) || frameLength > int(protocol.MaxFrameBytes) || len(raw) != entryFixedBytes+frameLength {
		return Entry{}, errors.New("invalid batch frame length")
	}
	frameEnd := 68 + frameLength
	frame := append([]byte(nil), raw[68:frameEnd]...)
	decoded, err := protocol.DecodeFrame(frame)
	if err != nil {
		return Entry{}, fmt.Errorf("invalid batch frame: %w", err)
	}
	message, err := protocol.DecodeMessage(decoded)
	if err != nil {
		return Entry{}, fmt.Errorf("invalid batch message: %w", err)
	}
	if _, ok := message.(protocol.BatchFrameV1); !ok {
		return Entry{}, errors.New("WAL frame is not BatchFrameV1")
	}
	var previous [32]byte
	copy(previous[:], raw[32:64])
	var batchHash [32]byte
	copy(batchHash[:], raw[frameEnd:frameEnd+32])
	var entryHash [32]byte
	copy(entryHash[:], raw[frameEnd+32:frameEnd+64])
	if binary.LittleEndian.Uint32(raw[frameEnd+64:frameEnd+68]) != commitMarker {
		return Entry{}, errors.New("missing WAL commit marker")
	}
	wantCRC := binary.LittleEndian.Uint32(raw[len(raw)-4:])
	gotCRC := crc32.Checksum(raw[:len(raw)-4], crc32.MakeTable(crc32.Castagnoli))
	if wantCRC != gotCRC {
		return Entry{}, errors.New("WAL entry CRC mismatch")
	}
	wantBatchHash := protocol.GatewayBatchSHA256(frame)
	if batchHash != wantBatchHash {
		return Entry{}, errors.New("WAL batch hash mismatch")
	}
	wantEntryHash := protocol.WALEntryHash(
		binary.LittleEndian.Uint64(raw[8:16]),
		previous,
		int64(binary.LittleEndian.Uint64(raw[16:24])),
		binary.LittleEndian.Uint64(raw[24:32]),
		batchHash,
		frame,
	)
	if entryHash != wantEntryHash {
		return Entry{}, errors.New("WAL entry hash mismatch")
	}
	return Entry{
		Sequence:           binary.LittleEndian.Uint64(raw[8:16]),
		ReceiveWallS:       int64(binary.LittleEndian.Uint64(raw[16:24])),
		ReceiveMonotonicUS: binary.LittleEndian.Uint64(raw[24:32]),
		PreviousEntryHash:  previous,
		Frame:              frame,
		BatchHash:          batchHash,
		EntryHash:          entryHash,
	}, nil
}

func writeAll(file *os.File, data []byte) error {
	for len(data) > 0 {
		written, err := file.Write(data)
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
		data = data[written:]
	}
	return nil
}
