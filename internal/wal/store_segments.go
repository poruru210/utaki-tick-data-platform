package wal

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const sealedDirectory = "sealed"

func (s *Store) initialize() error {
	if err := os.MkdirAll(s.sealedRoot(), 0o700); err != nil {
		return fmt.Errorf("create sealed WAL directory: %w", err)
	}
	if err := s.loadSealedSegments(); err != nil {
		return err
	}

	data, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return s.createActive()
	}
	if err != nil {
		return fmt.Errorf("read active WAL: %w", err)
	}
	expectedHeaderLength := 30 + len(s.gatewayID)
	if len(data) < expectedHeaderLength {
		if !matchesIncompleteHeader(data, s.gatewayID, s.next) {
			return fmt.Errorf("%w: invalid incomplete active WAL header", ErrIntegrity)
		}
		file, err := os.OpenFile(s.path, os.O_RDWR|os.O_TRUNC, 0o600)
		if err != nil {
			return fmt.Errorf("open incomplete active WAL header: %w", err)
		}
		s.setActiveFile(file)
		s.start = s.next
		s.activeAt = len(s.entries)
		if err := s.writeHeader(time.Now().Unix()); err != nil {
			return err
		}
		_, err = s.file.Seek(0, io.SeekEnd)
		return err
	}

	file, err := os.OpenFile(s.path, os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("open active WAL: %w", err)
	}
	s.setActiveFile(file)
	if err := s.loadActive(); err != nil {
		if !errors.Is(err, errSealedTrailerAtEntryBoundary) {
			return err
		}
		if closeErr := s.file.Close(); closeErr != nil {
			s.file = nil
			return fmt.Errorf("close sealed active WAL during recovery: %w", closeErr)
		}
		s.file = nil
		segment, verifyErr := VerifySealedSegment(s.path)
		if verifyErr != nil {
			return verifyErr
		}
		if recoverErr := s.recoverSealedActive(segment); recoverErr != nil {
			return recoverErr
		}
		return s.createActive()
	}
	if _, err := s.file.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("seek WAL end: %w", err)
	}
	return nil
}

func (s *Store) loadSealedSegments() error {
	dirEntries, err := os.ReadDir(s.sealedRoot())
	if err != nil {
		return fmt.Errorf("read sealed WAL directory: %w", err)
	}
	segments := make([]VerifiedSegment, 0, len(dirEntries))
	for _, dirEntry := range dirEntries {
		if dirEntry.IsDir() || !strings.HasSuffix(dirEntry.Name(), ".wal") {
			continue
		}
		path := filepath.Join(s.sealedRoot(), dirEntry.Name())
		segment, err := VerifySealedSegment(path)
		if err != nil {
			return err
		}
		segments = append(segments, segment)
	}
	sort.Slice(segments, func(i, j int) bool {
		return segments[i].StartSequence < segments[j].StartSequence
	})
	for _, segment := range segments {
		if err := s.validateNextSegment(segment); err != nil {
			return err
		}
		s.acceptLoadedSegment(segment)
	}
	return nil
}

func (s *Store) validateNextSegment(segment VerifiedSegment) error {
	if segment.GatewayInstanceID != s.gatewayID {
		return fmt.Errorf("%w: sealed WAL gateway identity mismatch", ErrIntegrity)
	}
	if segment.StartSequence != s.next {
		return fmt.Errorf(
			"%w: sealed WAL starts at sequence %d, want %d",
			ErrIntegrity,
			segment.StartSequence,
			s.next,
		)
	}
	if segment.ChainStart != s.last {
		return fmt.Errorf(
			"%w: sealed WAL chain start mismatch at sequence %d",
			ErrIntegrity,
			segment.StartSequence,
		)
	}
	if segment.LastSequence == math.MaxUint64 {
		return fmt.Errorf("%w: WAL sequence space exhausted", ErrIntegrity)
	}
	return nil
}

func (s *Store) acceptLoadedSegment(segment VerifiedSegment) {
	s.entries = append(s.entries, segment.Entries...)
	segment.Entries = nil
	s.sealed = append(s.sealed, segment)
	s.sealedBytes += segment.FileBytes
	s.fileBytes = s.sealedBytes
	s.last = segment.ChainRoot
	s.next = segment.LastSequence + 1
	s.activeAt = len(s.entries)
}

func (s *Store) recoverSealedActive(segment VerifiedSegment) error {
	if segment.StartSequence == s.next {
		if err := s.validateNextSegment(segment); err != nil {
			return err
		}
		installed, err := s.installActiveAsSealed(segment)
		if err != nil {
			return err
		}
		s.acceptLoadedSegment(installed)
		return nil
	}
	if len(s.sealed) == 0 {
		return fmt.Errorf("%w: active WAL contains an unexpected sealed segment", ErrIntegrity)
	}
	last := s.sealed[len(s.sealed)-1]
	if segment.StartSequence != last.StartSequence ||
		segment.LastSequence != last.LastSequence ||
		segment.ObjectSHA256 != last.ObjectSHA256 {
		return fmt.Errorf("%w: active WAL duplicates a different sealed segment", ErrIntegrity)
	}
	equal, err := filesEqual(s.path, last.Path)
	if err != nil {
		return err
	}
	if !equal {
		return fmt.Errorf("%w: active WAL duplicate bytes differ", ErrIntegrity)
	}
	if err := os.Remove(s.path); err != nil {
		return fmt.Errorf("remove duplicate sealed active WAL: %w", err)
	}
	return nil
}

// Seal appends a durable TWTR trailer, moves the segment into the sealed WAL
// inventory, and creates the next active WAL with a continuous sequence and
// entry-hash chain.
func (s *Store) Seal() (VerifiedSegment, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.file == nil || s.poisoned {
		return VerifiedSegment{}, ErrUnavailable
	}
	activeCount := len(s.entries) - s.activeAt
	if activeCount == 0 {
		return VerifiedSegment{}, ErrEmptySegment
	}
	if uint64(activeCount) > math.MaxUint32 {
		return VerifiedSegment{}, fmt.Errorf("%w: active WAL has too many entries to seal", ErrIntegrity)
	}
	if err := s.syncFile(); err != nil {
		s.poisoned = true
		return VerifiedSegment{}, fmt.Errorf("%w: sync WAL before seal: %v", ErrUnavailable, err)
	}
	if _, err := s.file.Seek(0, io.SeekStart); err != nil {
		s.poisoned = true
		return VerifiedSegment{}, fmt.Errorf("%w: seek WAL before seal: %v", ErrUnavailable, err)
	}
	data, err := io.ReadAll(s.file)
	if err != nil {
		s.poisoned = true
		return VerifiedSegment{}, fmt.Errorf("%w: read WAL before seal: %v", ErrUnavailable, err)
	}
	first := s.entries[s.activeAt]
	last := s.entries[len(s.entries)-1]
	if last.Sequence == math.MaxUint64 {
		return VerifiedSegment{}, fmt.Errorf("%w: WAL sequence space exhausted", ErrIntegrity)
	}
	fileHash := sha256.Sum256(data)
	trailer := encodeTrailer(first.Sequence, last.Sequence, uint32(activeCount), last.EntryHash, fileHash)
	if _, err := s.file.Seek(0, io.SeekEnd); err != nil {
		s.poisoned = true
		return VerifiedSegment{}, fmt.Errorf("%w: seek WAL end before seal: %v", ErrUnavailable, err)
	}
	if err := writeAll(s.file, trailer); err != nil {
		s.poisoned = true
		return VerifiedSegment{}, fmt.Errorf("%w: append WAL trailer: %v", ErrUnavailable, err)
	}
	if err := s.syncFile(); err != nil {
		s.poisoned = true
		return VerifiedSegment{}, fmt.Errorf("%w: sync sealed WAL: %v", ErrUnavailable, err)
	}
	if err := s.file.Close(); err != nil {
		s.file = nil
		s.poisoned = true
		return VerifiedSegment{}, fmt.Errorf("%w: close sealed WAL: %v", ErrUnavailable, err)
	}
	s.file = nil

	segment, err := VerifySealedSegment(s.path)
	if err != nil {
		s.poisoned = true
		return VerifiedSegment{}, err
	}
	if segment.GatewayInstanceID != s.gatewayID ||
		segment.StartSequence != s.start ||
		segment.LastSequence != last.Sequence ||
		int(segment.EntryCount) != activeCount ||
		segment.ChainStart != first.PreviousEntryHash ||
		segment.ChainRoot != last.EntryHash {
		s.poisoned = true
		return VerifiedSegment{}, fmt.Errorf("%w: sealed WAL metadata changed during rotation", ErrIntegrity)
	}
	segment, err = s.installActiveAsSealed(segment)
	if err != nil {
		s.poisoned = true
		return VerifiedSegment{}, err
	}
	metadata := segment
	metadata.Entries = nil
	s.sealed = append(s.sealed, metadata)
	s.sealedBytes += segment.FileBytes
	s.fileBytes = s.sealedBytes
	if err := s.createActive(); err != nil {
		s.poisoned = true
		return VerifiedSegment{}, err
	}
	return cloneVerifiedSegment(segment), nil
}

func (s *Store) installActiveAsSealed(segment VerifiedSegment) (VerifiedSegment, error) {
	destination := filepath.Join(
		s.sealedRoot(),
		fmt.Sprintf("segment-%020d-%020d.wal", segment.StartSequence, segment.LastSequence),
	)
	if err := os.Link(s.path, destination); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return VerifiedSegment{}, fmt.Errorf("atomically publish sealed WAL into inventory: %w", err)
		}
		existing, verifyErr := VerifySealedSegment(destination)
		if verifyErr != nil {
			return VerifiedSegment{}, verifyErr
		}
		if existing.ObjectSHA256 != segment.ObjectSHA256 || existing.FileBytes != segment.FileBytes {
			return VerifiedSegment{}, fmt.Errorf("%w: sealed WAL destination already has different bytes", ErrIntegrity)
		}
		equal, compareErr := filesEqual(s.path, destination)
		if compareErr != nil {
			return VerifiedSegment{}, compareErr
		}
		if !equal {
			return VerifiedSegment{}, fmt.Errorf("%w: sealed WAL destination byte mismatch", ErrIntegrity)
		}
		segment = existing
	}
	if err := os.Remove(s.path); err != nil {
		return VerifiedSegment{}, fmt.Errorf("remove sealed active WAL after publish: %w", err)
	}
	segment.Path = destination
	return segment, nil
}

func (s *Store) createActive() error {
	file, err := os.OpenFile(s.path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("create active WAL: %w", err)
	}
	s.setActiveFile(file)
	s.start = s.next
	s.activeAt = len(s.entries)
	s.poisoned = false
	if err := s.writeHeader(time.Now().Unix()); err != nil {
		return err
	}
	if _, err := s.file.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("seek new active WAL end: %w", err)
	}
	return nil
}

func (s *Store) setActiveFile(file *os.File) {
	s.file = file
	s.syncFile = file.Sync
	s.statFile = file.Stat
}

func (s *Store) sealedRoot() string {
	return filepath.Join(s.root, sealedDirectory)
}

func matchesIncompleteHeader(data []byte, gatewayID string, startSequence uint64) bool {
	expected := make([]byte, 30+len(gatewayID))
	copy(expected[0:4], fileMagic)
	binary.LittleEndian.PutUint16(expected[4:6], walSchemaVersion)
	binary.LittleEndian.PutUint16(expected[6:8], uint16(len(expected)))
	binary.LittleEndian.PutUint64(expected[8:16], startSequence)
	binary.LittleEndian.PutUint32(expected[24:28], 0)
	binary.LittleEndian.PutUint16(expected[28:30], uint16(len(gatewayID)))
	copy(expected[30:], gatewayID)
	for index, value := range data {
		if index >= 16 && index < 24 {
			continue
		}
		if value != expected[index] {
			return false
		}
	}
	return true
}

func filesEqual(left, right string) (bool, error) {
	leftBytes, err := os.ReadFile(left)
	if err != nil {
		return false, fmt.Errorf("read %s: %w", left, err)
	}
	rightBytes, err := os.ReadFile(right)
	if err != nil {
		return false, fmt.Errorf("read %s: %w", right, err)
	}
	return bytes.Equal(leftBytes, rightBytes), nil
}

func cloneEntries(entries []Entry) []Entry {
	result := make([]Entry, len(entries))
	for i, entry := range entries {
		result[i] = entry
		result[i].Frame = append([]byte(nil), entry.Frame...)
	}
	return result
}

func cloneVerifiedSegment(segment VerifiedSegment) VerifiedSegment {
	segment.Entries = cloneEntries(segment.Entries)
	return segment
}
