package retention

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestWallClockIsAppendOnlyAndRejectsRegression(t *testing.T) {
	root := t.TempDir()
	if err := PublishWallClock(root, 100); err != nil {
		t.Fatal(err)
	}
	if err := PublishWallClock(root, 100); err != nil {
		t.Fatal(err)
	}
	if err := PublishWallClock(root, 99); err == nil || !errors.Is(err, ErrPruneIntegrity) {
		t.Fatalf("regression error = %v", err)
	}
	if err := PublishWallClock(root, 101); err != nil {
		t.Fatal(err)
	}
	latest, err := LoadLatestWallClock(root)
	if err != nil || latest.ObservedWallTimeUnixMS != 101 {
		t.Fatalf("latest = %+v err=%v", latest, err)
	}
}

func TestWallClockRejectsUnknownAndSymlinkEntries(t *testing.T) {
	root := t.TempDir()
	directory := WallClockDirectory(root)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "unexpected.json"), []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadLatestWallClock(root); err == nil || !errors.Is(err, ErrPruneIntegrity) {
		t.Fatalf("unknown entry error = %v", err)
	}
}

func TestWallClockRejectsNonDirectoryChildPath(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(WallClockDirectory(root), []byte("not a directory"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadLatestWallClock(root); !errors.Is(err, ErrPruneIntegrity) {
		t.Fatalf("non-directory wall clock path err=%v", err)
	}
	if err := PublishWallClock(root, 100); !errors.Is(err, ErrPruneIntegrity) {
		t.Fatalf("publish through non-directory wall clock path err=%v", err)
	}
}
