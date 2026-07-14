package wal

import (
	"errors"
	"testing"

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
