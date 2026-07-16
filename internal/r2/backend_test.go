package r2

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/smithy-go"
)

func TestClassifyRemoteErrorPreservesContextCancellation(t *testing.T) {
	for _, want := range []error{context.Canceled, context.DeadlineExceeded} {
		err := classifyRemoteError(want)
		if !errors.Is(err, want) {
			t.Fatalf("classified %v as %v without preserving cause", want, err)
		}
	}
}

func TestClassifyRemoteErrorMapsPermissionFailures(t *testing.T) {
	for _, code := range []string{"AccessDenied", "Unauthorized", "Forbidden", "InvalidAccessKeyId", "InvalidClientTokenId", "InvalidToken", "ExpiredToken", "MissingAuthenticationToken", "SignatureDoesNotMatch"} {
		err := classifyRemoteError(&smithy.GenericAPIError{Code: code})
		if !errors.Is(err, ErrRemotePermission) {
			t.Fatalf("classified %s as %v", code, err)
		}
	}
}
