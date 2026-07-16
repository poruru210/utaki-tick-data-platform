package r2

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"tick-data-platform/internal/archive"
)

func TestSaveVerificationReceiptIsNoClobberAndRetrySafe(t *testing.T) {
	path := filepath.Join(t.TempDir(), "receipt.json")
	receipt := testReceipt()
	if err := SaveVerificationReceipt(path, receipt); err != nil {
		t.Fatal(err)
	}
	want, err := receipt.CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(path); err != nil || !bytes.Equal(got, want) {
		t.Fatalf("saved receipt = %q, want %q, err=%v", got, want, err)
	}
	if err := SaveVerificationReceipt(path, receipt); err != nil {
		t.Fatalf("same-content retry: %v", err)
	}

	different := receipt
	different.ManifestKey = "different"
	if err := SaveVerificationReceipt(path, different); !errors.Is(err, archive.ErrIntegrity) {
		t.Fatalf("different-content retry error = %v, want ErrIntegrity", err)
	}
}

func TestSaveVerificationReceiptCrashBoundaryLeavesOnlyDisposableTemp(t *testing.T) {
	directory := t.TempDir()
	stale, err := os.CreateTemp(directory, ".verification-receipt-*.tmp")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stale.WriteString(`{"partial":`); err != nil {
		t.Fatal(err)
	}
	if err := stale.Sync(); err != nil {
		t.Fatal(err)
	}
	stalePath := stale.Name()
	if err := stale.Close(); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(directory, "receipt.json")
	if err := SaveVerificationReceipt(path, testReceipt()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("complete final receipt is missing: %v", err)
	}
	if _, err := os.Stat(stalePath); err != nil {
		t.Fatalf("crash leftover should remain disposable until cleanup: %v", err)
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.Name() == filepath.Base(path) {
			continue
		}
		if !bytes.HasPrefix([]byte(entry.Name()), []byte(".verification-receipt-")) {
			t.Fatalf("unexpected receipt directory entry %q", entry.Name())
		}
	}
}

func testReceipt() VerificationReceipt {
	return VerificationReceipt{
		ReceiptVersion:       "publication-verification-receipt-v1",
		ManifestKey:          "v1/snapshots/raw/test.json",
		VerificationComplete: true,
	}
}
