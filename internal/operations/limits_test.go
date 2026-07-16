package operations

import (
	"bytes"
	"testing"
)

func TestResourceLimitsCanonicalRoundTrip(t *testing.T) {
	canonical, err := DefaultResourceLimits.CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeResourceLimits(canonical)
	if err != nil {
		t.Fatal(err)
	}
	want, err := decoded.CanonicalJSON()
	if err != nil || !bytes.Equal(canonical, want) {
		t.Fatalf("round trip changed bytes: %q != %q; err=%v", canonical, want, err)
	}
}

func TestResourceLimitsRejectZeroAndOverflow(t *testing.T) {
	zero := DefaultResourceLimits
	zero.MaxProofBytes = 0
	if err := zero.Validate(); err == nil {
		t.Fatal("zero proof limit was accepted")
	}
	tooLarge := DefaultResourceLimits
	tooLarge.MaxAPIRequestBytes = maxAPIRequestBytes + 1
	if err := tooLarge.Validate(); err == nil {
		t.Fatal("unbounded request limit was accepted")
	}
	tooLarge = DefaultResourceLimits
	tooLarge.MaxProofBytes = tooLarge.MaxHandoverObservationBytes + 1
	if err := tooLarge.Validate(); err == nil {
		t.Fatal("inconsistent proof/observation relationship was accepted")
	}
}
