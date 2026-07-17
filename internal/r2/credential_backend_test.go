package r2

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"tick-data-platform/internal/credentials"
)

func TestCredentialBackendLoadsProviderOnceAndHasLifecycleBoundary(t *testing.T) {
	provider := &countingProvider{}
	backend, err := NewCredentialBackend(S3BackendConfig{
		Bucket: "tick-raw", Endpoint: "https://account.r2.cloudflarestorage.com", Region: "auto",
	}, provider)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := backend.Get(context.Background(), "key"); !errors.Is(err, ErrBackendNotReady) {
		t.Fatalf("pre-start error = %v", err)
	}
	if err := backend.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := atomic.LoadInt64(&provider.calls); got != 1 {
		t.Fatalf("provider calls = %d", got)
	}
	if err := backend.Start(context.Background()); err == nil {
		t.Fatal("second start unexpectedly succeeded")
	}
	if err := backend.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := backend.Get(context.Background(), "key"); !errors.Is(err, ErrBackendNotReady) {
		t.Fatalf("post-stop error = %v", err)
	}
	if err := backend.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := atomic.LoadInt64(&provider.calls); got != 1 {
		t.Fatalf("provider calls after restart = %d", got)
	}
}

type countingProvider struct {
	calls int64
}

func (p *countingProvider) Load(context.Context) (credentials.Credentials, error) {
	atomic.AddInt64(&p.calls, 1)
	return credentials.Credentials{AccessKeyID: "test-access", SecretAccessKey: "test-secret"}, nil
}
