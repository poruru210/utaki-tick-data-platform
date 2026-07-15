package r2

import (
	"bytes"
	"errors"
	"os"
	"testing"

	"tick-data-platform/internal/archive"
)

func TestReplayReceiptBindsCanonicalBundleAndFinalObservation(t *testing.T) {
	fixture := newReplayPublicationFixture(t, true)
	receipt, err := fixture.publish(t)
	if err != nil {
		t.Fatal(err)
	}
	bundle := fixture.sealedBundle(t)
	if !bytes.Equal(receipt.BundleCanonical, bundle.CanonicalBytes) || receipt.BundleDigest != bundle.Digest {
		t.Fatal("receipt does not bind the complete canonical bundle")
	}
	canonical, err := receipt.CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range [][]byte{
		[]byte(bundle.LocalSources.ReceiptPath),
		[]byte(fixture.toolPath),
		[]byte(`"journal"`), []byte(`"stage"`), []byte(`"event"`),
		[]byte(`"retry"`), []byte(`"etag"`), []byte(`"credential"`), []byte(`"local_path"`),
	} {
		if len(forbidden) != 0 && bytes.Contains(canonical, forbidden) {
			t.Fatalf("receipt leaked runtime-only value %q", forbidden)
		}
	}
	tampered := receipt
	tampered.FinalObservationCanonical = append([]byte(nil), receipt.FinalObservationCanonical...)
	tampered.FinalObservationCanonical[len(tampered.FinalObservationCanonical)-1] ^= 1
	if _, err := tampered.CanonicalJSON(); err == nil {
		t.Fatal("receipt accepted a tampered terminal final observation")
	}
}

func TestReplayReceiptNoClobberSameContentAndConflict(t *testing.T) {
	fixture := newReplayPublicationFixture(t, true)
	receipt, err := fixture.publish(t)
	if err != nil {
		t.Fatal(err)
	}
	if err := SaveReplayVerificationReceipt(fixture.receipt, receipt); err != nil {
		t.Fatalf("same-content receipt retry: %v", err)
	}
	if err := os.WriteFile(fixture.receipt, []byte("conflicting-receipt"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := SaveReplayVerificationReceipt(fixture.receipt, receipt); !errors.Is(err, archive.ErrIntegrity) {
		t.Fatalf("receipt conflict = %v, want archive integrity error", err)
	}
}
