package r2

import (
	"errors"
	"path/filepath"
	"testing"
)

func TestPublicationLockRejectsSecondOwner(t *testing.T) {
	path := filepath.Join(t.TempDir(), "publication.lock")
	first, err := AcquirePublicationLock(path)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := AcquirePublicationLock(path)
	if !errors.Is(err, ErrPublicationLock) || second != nil {
		t.Fatalf("second lock = %v, owner=%v, want ErrPublicationLock", err, second)
	}
}
