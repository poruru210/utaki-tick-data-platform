package retention

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"tick-data-platform/internal/protocol"
)

const (
	WallClockVersion  = "retention-wall-clock-v1"
	wallClockDirName  = "wall-clocks"
	maxWallClockFiles = uint64(1 << 20)
)

var ErrWallClockAbsent = errors.New("durable retention wall clock is absent")

type WallClockRecord struct {
	Version                string
	ObservedWallTimeUnixMS uint64
}

func (r WallClockRecord) Value() map[string]any {
	return map[string]any{
		"observed_wall_time_unix_ms": r.ObservedWallTimeUnixMS,
		"version":                    r.Version,
	}
}

func (r WallClockRecord) CanonicalJSON() ([]byte, error) {
	if r.Version != WallClockVersion || r.ObservedWallTimeUnixMS == 0 {
		return nil, fmt.Errorf("wall clock record is invalid")
	}
	return protocol.CanonicalJSON(r.Value())
}

func WallClockDigest(canonical []byte) [32]byte {
	return sha256.Sum256(append([]byte("tick-data-platform/retention-wall-clock/v1\x00"), canonical...))
}

func DecodeWallClock(data []byte) (WallClockRecord, error) {
	object, err := decodeObject(data, map[string]bool{"observed_wall_time_unix_ms": true, "version": true})
	if err != nil {
		return WallClockRecord{}, err
	}
	version, err := stringField(object, "version")
	if err != nil {
		return WallClockRecord{}, err
	}
	observed, err := uintField(object, "observed_wall_time_unix_ms")
	if err != nil {
		return WallClockRecord{}, err
	}
	record := WallClockRecord{Version: version, ObservedWallTimeUnixMS: observed}
	canonical, err := record.CanonicalJSON()
	if err != nil || !bytes.Equal(canonical, data) {
		return WallClockRecord{}, fmt.Errorf("wall clock bytes are not canonical")
	}
	return record, nil
}

func WallClockDirectory(root string) string {
	return filepath.Join(root, wallClockDirName)
}

func PublishWallClock(root string, observed uint64) error {
	if root == "" || observed == 0 {
		return fmt.Errorf("wall clock root and observation are required")
	}
	if err := trustedDirectory(root); err != nil {
		return err
	}
	latest, err := LoadLatestWallClock(root)
	if err != nil && !errors.Is(err, ErrWallClockAbsent) {
		return err
	}
	if err == nil && observed < latest.ObservedWallTimeUnixMS {
		return fmt.Errorf("%w: wall clock regressed", ErrPruneIntegrity)
	}
	if err == nil && observed == latest.ObservedWallTimeUnixMS {
		return nil
	}
	record := WallClockRecord{Version: WallClockVersion, ObservedWallTimeUnixMS: observed}
	canonical, err := record.CanonicalJSON()
	if err != nil {
		return err
	}
	directory := WallClockDirectory(root)
	if err := ensureWallClockDirectory(root); err != nil {
		return err
	}
	destination := filepath.Join(directory, wallClockFilename(observed))
	if existing, readErr := os.ReadFile(destination); readErr == nil {
		if bytes.Equal(existing, canonical) {
			return nil
		}
		return fmt.Errorf("%w: wall clock timestamp has different bytes", ErrPruneIntegrity)
	} else if !errors.Is(readErr, os.ErrNotExist) {
		return fmt.Errorf("%w: inspect wall clock destination: %v", ErrPruneAvailability, readErr)
	}
	temporary, err := os.CreateTemp(directory, ".wall-clock-*.tmp")
	if err != nil {
		return fmt.Errorf("%w: create wall clock temporary: %v", ErrPruneAvailability, err)
	}
	temporaryName := temporary.Name()
	defer func() { _ = os.Remove(temporaryName) }()
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("%w: wall clock permissions: %v", ErrPruneAvailability, err)
	}
	if _, err := temporary.Write(canonical); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("%w: write wall clock: %v", ErrPruneAvailability, err)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("%w: sync wall clock: %v", ErrPruneAvailability, err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("%w: close wall clock: %v", ErrPruneAvailability, err)
	}
	if err := os.Link(temporaryName, destination); err != nil {
		if errors.Is(err, os.ErrExist) {
			existing, readErr := os.ReadFile(destination)
			if readErr == nil && bytes.Equal(existing, canonical) {
				return nil
			}
			return fmt.Errorf("%w: wall clock publish raced with different bytes", ErrPruneIntegrity)
		}
		return fmt.Errorf("%w: publish wall clock: %v", ErrPruneAvailability, err)
	}
	return syncDirectory(directory)
}

func LoadLatestWallClock(root string) (WallClockRecord, error) {
	if root == "" {
		return WallClockRecord{}, fmt.Errorf("wall clock root is required")
	}
	if err := trustedDirectory(root); err != nil {
		return WallClockRecord{}, err
	}
	directory := WallClockDirectory(root)
	info, err := os.Lstat(directory)
	if errors.Is(err, os.ErrNotExist) {
		return WallClockRecord{}, ErrWallClockAbsent
	}
	if err != nil {
		return WallClockRecord{}, fmt.Errorf("%w: inspect wall clock directory: %v", ErrPruneAvailability, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return WallClockRecord{}, fmt.Errorf("%w: wall clock path is not a trusted directory", ErrPruneIntegrity)
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		return WallClockRecord{}, fmt.Errorf("%w: read wall clock directory: %v", ErrPruneAvailability, err)
	}
	if uint64(len(entries)) > maxWallClockFiles {
		return WallClockRecord{}, fmt.Errorf("%w: wall clock count exceeds limit", ErrPruneIntegrity)
	}
	var latest WallClockRecord
	for _, entry := range entries {
		info, infoErr := entry.Info()
		if infoErr != nil {
			return WallClockRecord{}, fmt.Errorf("%w: stat wall clock entry: %v", ErrPruneAvailability, infoErr)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return WallClockRecord{}, fmt.Errorf("%w: wall clock directory contains symlink", ErrPruneIntegrity)
		}
		if strings.HasPrefix(entry.Name(), ".wall-clock-") && strings.HasSuffix(entry.Name(), ".tmp") {
			continue
		}
		observed, ok := parseWallClockFilename(entry.Name())
		if !ok || !info.Mode().IsRegular() {
			return WallClockRecord{}, fmt.Errorf("%w: unknown wall clock entry", ErrPruneIntegrity)
		}
		data, readErr := os.ReadFile(filepath.Join(directory, entry.Name()))
		if readErr != nil {
			return WallClockRecord{}, fmt.Errorf("%w: read wall clock: %v", ErrPruneAvailability, readErr)
		}
		record, decodeErr := DecodeWallClock(data)
		if decodeErr != nil || record.ObservedWallTimeUnixMS != observed {
			return WallClockRecord{}, fmt.Errorf("%w: wall clock identity is invalid", ErrPruneIntegrity)
		}
		if record.ObservedWallTimeUnixMS > latest.ObservedWallTimeUnixMS {
			latest = record
		}
	}
	if latest.ObservedWallTimeUnixMS == 0 {
		return WallClockRecord{}, ErrWallClockAbsent
	}
	return latest, nil
}

func ensureWallClockDirectory(root string) error {
	directory := WallClockDirectory(root)
	info, err := os.Lstat(directory)
	if errors.Is(err, os.ErrNotExist) {
		if err := os.MkdirAll(directory, 0o700); err != nil {
			return fmt.Errorf("%w: create wall clock directory: %v", ErrPruneAvailability, err)
		}
		info, err = os.Lstat(directory)
	}
	if err != nil {
		return fmt.Errorf("%w: inspect wall clock directory: %v", ErrPruneAvailability, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("%w: wall clock path is not a trusted directory", ErrPruneIntegrity)
	}
	return nil
}

func wallClockFilename(observed uint64) string {
	return fmt.Sprintf("wall-clock-%020d.json", observed)
}

func parseWallClockFilename(name string) (uint64, bool) {
	if !strings.HasPrefix(name, "wall-clock-") || !strings.HasSuffix(name, ".json") {
		return 0, false
	}
	digits := strings.TrimSuffix(strings.TrimPrefix(name, "wall-clock-"), ".json")
	if len(digits) != 20 {
		return 0, false
	}
	value, err := strconv.ParseUint(digits, 10, 64)
	return value, err == nil && wallClockFilename(value) == name
}

func trustedDirectory(root string) error {
	info, err := os.Lstat(root)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("%w: trusted root is unavailable", ErrPruneIntegrity)
	}
	return nil
}
