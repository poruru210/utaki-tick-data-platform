package r2

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	appcredentials "tick-data-platform/internal/credentials"
)

var ErrBackendNotReady = errors.New("R2 backend is not ready")

// CredentialBackend binds one credential Provider to one S3 client. It is a
// lifecycle component: credentials are loaded once at Start and never watched
// or reloaded while the process is running.
type CredentialBackend struct {
	settings S3BackendConfig
	provider appcredentials.Provider

	loadOnce sync.Once
	loadErr  error
	backend  *S3Backend
	mu       sync.RWMutex
	started  bool
}

func NewCredentialBackend(settings S3BackendConfig, provider appcredentials.Provider) (*CredentialBackend, error) {
	if provider == nil {
		return nil, fmt.Errorf("credential provider is required")
	}
	if settings.Bucket == "" || settings.Endpoint == "" {
		return nil, fmt.Errorf("credential-bound S3 configuration is incomplete")
	}
	if settings.Region == "" {
		settings.Region = "auto"
	}
	if err := ValidateHTTPSHostEndpoint(settings.Endpoint); err != nil {
		return nil, err
	}
	return &CredentialBackend{settings: settings, provider: provider}, nil
}

func (b *CredentialBackend) Start(ctx context.Context) error {
	if b == nil {
		return ErrBackendNotReady
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	b.loadOnce.Do(func() {
		loaded, err := b.provider.Load(ctx)
		if err != nil {
			b.loadErr = err
			return
		}
		b.backend, b.loadErr = newS3BackendWithCredentials(ctx, b.settings, loaded.AccessKeyID, loaded.SecretAccessKey)
	})
	if b.loadErr != nil {
		return b.loadErr
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.started {
		return fmt.Errorf("R2 backend is already started")
	}
	b.started = true
	return nil
}

func (b *CredentialBackend) Stop(ctx context.Context) error {
	if b == nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	b.mu.Lock()
	b.started = false
	b.mu.Unlock()
	return nil
}

func (b *CredentialBackend) ready() (*S3Backend, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if !b.started || b.backend == nil {
		return nil, ErrBackendNotReady
	}
	return b.backend, nil
}

func (b *CredentialBackend) PutIfAbsent(ctx context.Context, key string, body []byte) error {
	backend, err := b.ready()
	if err != nil {
		return err
	}
	return backend.PutIfAbsent(ctx, key, body)
}

func (b *CredentialBackend) Get(ctx context.Context, key string) ([]byte, error) {
	backend, err := b.ready()
	if err != nil {
		return nil, err
	}
	return backend.Get(ctx, key)
}

func (b *CredentialBackend) Open(ctx context.Context, key string) (io.ReadCloser, int64, error) {
	backend, err := b.ready()
	if err != nil {
		return nil, 0, err
	}
	return backend.Open(ctx, key)
}

func (b *CredentialBackend) List(ctx context.Context, prefix string) ([]RemoteObject, error) {
	backend, err := b.ready()
	if err != nil {
		return nil, err
	}
	return backend.List(ctx, prefix)
}

func (b *CredentialBackend) GetLimited(ctx context.Context, key string, maxBytes uint64) ([]byte, error) {
	backend, err := b.ready()
	if err != nil {
		return nil, err
	}
	return backend.GetLimited(ctx, key, maxBytes)
}

func (b *CredentialBackend) ListLimited(ctx context.Context, prefix string, maxObjects uint64) ([]RemoteObject, error) {
	backend, err := b.ready()
	if err != nil {
		return nil, err
	}
	return backend.ListLimited(ctx, prefix, maxObjects)
}

func (b *CredentialBackend) PutFileIfAbsent(ctx context.Context, key, path string, expectedSHA256 [32]byte, expectedBytes uint64) (RemoteObjectCommit, error) {
	backend, err := b.ready()
	if err != nil {
		return RemoteObjectCommit{}, err
	}
	return backend.PutFileIfAbsent(ctx, key, path, expectedSHA256, expectedBytes)
}

func (b *CredentialBackend) VerifyFile(ctx context.Context, key, path string, expectedSHA256 [32]byte, expectedBytes uint64) (RemoteObjectVerification, error) {
	backend, err := b.ready()
	if err != nil {
		return RemoteObjectVerification{}, err
	}
	return backend.VerifyFile(ctx, key, path, expectedSHA256, expectedBytes)
}

var _ WriteBackend = (*CredentialBackend)(nil)
var _ BoundedObjectBackend = (*CredentialBackend)(nil)
