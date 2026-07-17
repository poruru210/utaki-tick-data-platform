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

func TestUsePathStyleEndpointOnlyForLocalTestEndpoints(t *testing.T) {
	for _, endpoint := range []string{"http://127.0.0.1:9000", "http://[::1]:9000", "http://localhost:9000"} {
		if !usePathStyleEndpoint(endpoint) {
			t.Fatalf("local endpoint %q should use path-style requests", endpoint)
		}
	}
	for _, endpoint := range []string{
		"https://0123456789abcdef0123456789abcdef.r2.cloudflarestorage.com",
		"https://0123456789abcdef0123456789abcdef.eu.r2.cloudflarestorage.com",
	} {
		if usePathStyleEndpoint(endpoint) {
			t.Fatalf("R2 endpoint %q should use SDK default virtual-host resolution", endpoint)
		}
	}
}
