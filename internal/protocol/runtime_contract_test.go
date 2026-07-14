package protocol

import (
	"errors"
	"fmt"
	"testing"
)

func TestDecodeFrameRejectsShortHeaderWithoutPanic(t *testing.T) {
	for length := 0; length < 16; length++ {
		if _, err := DecodeFrame(make([]byte, length)); ErrorCodeOf(err) != ErrTruncatedFrame {
			t.Fatalf("length %d returned %v", length, err)
		}
	}
}

func TestBoundaryDigestDistinguishesOrderedMultiplicity(t *testing.T) {
	first := RawMqlTickV1{TimeMSC: 1000, BidBits: 1}
	second := RawMqlTickV1{TimeMSC: 1000, BidBits: 2}
	one := BoundaryDigest(1000, [32]byte{}, []RawMqlTickV1{first, second})
	reordered := BoundaryDigest(1000, [32]byte{}, []RawMqlTickV1{second, first})
	if one == reordered {
		t.Fatal("boundary digest must retain observation order")
	}
	withRepeat := BoundaryDigest(1000, [32]byte{}, []RawMqlTickV1{first, second, second})
	if one == withRepeat {
		t.Fatal("boundary digest must retain multiplicity")
	}
}

func TestErrorCodeOfPreservesWrappedProtocolErrorsAndRetriesUnknownErrors(t *testing.T) {
	wrappedProtocolError := fmt.Errorf("decode request: %w", &ProtocolError{Code: ErrCRCMismatch})
	if got := ErrorCodeOf(wrappedProtocolError); got != ErrCRCMismatch {
		t.Fatalf("wrapped protocol error code = %s, want %s", got, ErrCRCMismatch)
	}
	if got := ErrorCodeOf(errors.New("disk temporarily unavailable")); got != ErrInternalRetryable {
		t.Fatalf("unknown error code = %s, want %s", got, ErrInternalRetryable)
	}
}
