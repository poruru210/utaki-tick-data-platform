package app

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
	"testing"
	"time"

	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"

	"tick-data-platform/internal/protocol"
	"tick-data-platform/internal/publication"
	"tick-data-platform/internal/r2"
	"tick-data-platform/internal/wal"
)

func TestFakeR2PublicationThroughFxApplication(t *testing.T) {
	config := testConfig(t)
	config.Publication.SealMaxBytes = 1
	config.Publication.ScanIntervalMS = 10
	config.Publication.RetryMinMS = 10
	config.Publication.RetryMaxMS = 100
	backend := newAppFakeBackend()
	var store *wal.Store
	var catalog *publication.Catalog
	application := fxtest.New(t,
		TestOptionsWithRemoteBackend(config, &staticProvider{}, backend),
		fx.StartTimeout(30*time.Second),
		fx.StopTimeout(30*time.Second),
		fx.Populate(&store, &catalog),
	)
	application.RequireStart()
	t.Cleanup(func() {
		application.RequireStop()
		removeEventually(t, config.Publication.CatalogPath)
	})

	frame, err := protocol.EncodeMessage(protocol.BatchFrameV1{
		RequestedFromMSC: time.Date(2024, 3, 9, 0, 0, 0, 0, time.UTC).UnixMilli(),
		SourceSchemaID:   protocol.SourceSchemaMT5,
		ReturnedCount:    1,
		Records: []protocol.RawMqlTickV1{{
			TimeMSC:         time.Date(2024, 3, 9, 0, 0, 1, 0, time.UTC).UnixMilli(),
			CaptureSequence: 1,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(frame, 1710000000, 1); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		record, found, err := catalog.LatestManifest(context.Background(), "2024-03-09")
		if err != nil {
			t.Fatal(err)
		}
		if found && record.State == publication.ManifestStatePublished {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	record, found, err := catalog.LatestManifest(context.Background(), "2024-03-09")
	if err != nil || !found || record.State != publication.ManifestStatePublished {
		t.Fatalf("Fx publication did not reach local published state: record=%+v found=%v err=%v", record, found, err)
	}
	if backend.count() < 4 {
		t.Fatalf("fake R2 object count = %d, want claim, descriptor, raw object, manifest", backend.count())
	}
}

func removeEventually(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		err := os.Remove(path)
		if err == nil || os.IsNotExist(err) {
			return
		}
		if time.Now().After(deadline) {
			t.Errorf("remove %s after application stop: %v", path, err)
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func TestFxApplicationShutsDownOnPublicationWorkerError(t *testing.T) {
	config := testConfig(t)
	config.Publication.SealMaxBytes = 1
	config.Publication.ScanIntervalMS = 10
	config.Publication.RetryMinMS = 10
	config.Publication.RetryMaxMS = 100
	backend := &failingAppBackend{appFakeBackend: newAppFakeBackend()}
	var store *wal.Store
	application := fxtest.New(t,
		TestOptionsWithRemoteBackend(config, &staticProvider{}, backend),
		fx.StartTimeout(30*time.Second),
		fx.StopTimeout(30*time.Second),
		fx.Populate(&store),
	)
	application.RequireStart()
	t.Cleanup(func() { application.RequireStop() })

	frame, err := protocol.EncodeMessage(protocol.BatchFrameV1{
		RequestedFromMSC: time.Date(2024, 3, 9, 0, 0, 0, 0, time.UTC).UnixMilli(),
		SourceSchemaID:   protocol.SourceSchemaMT5,
		ReturnedCount:    1,
		Records: []protocol.RawMqlTickV1{{
			TimeMSC:         time.Date(2024, 3, 9, 0, 0, 1, 0, time.UTC).UnixMilli(),
			CaptureSequence: 1,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(frame, 1710000000, 1); err != nil {
		t.Fatal(err)
	}
	select {
	case <-application.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("Fx application did not shut down after publication worker error")
	}
	application.RequireStop()
}

type appFakeBackend struct {
	mu      sync.Mutex
	objects map[string][]byte
}

func newAppFakeBackend() *appFakeBackend {
	return &appFakeBackend{objects: make(map[string][]byte)}
}

func (b *appFakeBackend) PutIfAbsent(_ context.Context, key string, body []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, found := b.objects[key]; found {
		return r2.ErrObjectExists
	}
	b.objects[key] = append([]byte(nil), body...)
	return nil
}

func (b *appFakeBackend) Get(_ context.Context, key string) ([]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	body, found := b.objects[key]
	if !found {
		return nil, r2.ErrObjectNotFound
	}
	return append([]byte(nil), body...), nil
}

func (b *appFakeBackend) Open(_ context.Context, key string) (io.ReadCloser, int64, error) {
	body, err := b.Get(context.Background(), key)
	if err != nil {
		return nil, 0, err
	}
	return io.NopCloser(bytes.NewReader(body)), int64(len(body)), nil
}

func (b *appFakeBackend) List(_ context.Context, prefix string) ([]r2.RemoteObject, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	keys := make([]string, 0)
	for key := range b.objects {
		if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	objects := make([]r2.RemoteObject, 0, len(keys))
	for _, key := range keys {
		objects = append(objects, r2.RemoteObject{Key: key, Size: int64(len(b.objects[key]))})
	}
	return objects, nil
}

func (b *appFakeBackend) PutFileIfAbsent(ctx context.Context, key, path string, expectedSHA256 [32]byte, expectedBytes uint64) (r2.RemoteObjectCommit, error) {
	body, err := os.ReadFile(path)
	if err != nil {
		return r2.RemoteObjectCommit{}, err
	}
	if uint64(len(body)) != expectedBytes || sha256.Sum256(body) != expectedSHA256 {
		return r2.RemoteObjectCommit{}, r2.ErrLocalObjectChanged
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if existing, found := b.objects[key]; found {
		if !bytes.Equal(existing, body) {
			return r2.RemoteObjectCommit{}, r2.ErrImmutableCollision
		}
		return r2.RemoteObjectCommit{ETag: fmt.Sprintf("etag-%x", expectedSHA256[:4])}, nil
	}
	b.objects[key] = append([]byte(nil), body...)
	return r2.RemoteObjectCommit{ETag: fmt.Sprintf("etag-%x", expectedSHA256[:4])}, nil
}

func (b *appFakeBackend) VerifyFile(_ context.Context, key, path string, expectedSHA256 [32]byte, expectedBytes uint64) (r2.RemoteObjectVerification, error) {
	body, err := os.ReadFile(path)
	if err != nil {
		return r2.RemoteObjectVerification{}, err
	}
	if uint64(len(body)) != expectedBytes || sha256.Sum256(body) != expectedSHA256 {
		return r2.RemoteObjectVerification{}, r2.ErrLocalObjectChanged
	}
	remote, err := b.Get(context.Background(), key)
	if err != nil {
		return r2.RemoteObjectVerification{}, err
	}
	if !bytes.Equal(remote, body) {
		return r2.RemoteObjectVerification{}, r2.ErrRemoteCheckMismatch
	}
	return r2.RemoteObjectVerification{ETag: fmt.Sprintf("etag-%x", expectedSHA256[:4])}, nil
}

func (b *appFakeBackend) count() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.objects)
}

var _ r2.WriteBackend = (*appFakeBackend)(nil)

type failingAppBackend struct {
	*appFakeBackend
}

func (*failingAppBackend) PutFileIfAbsent(context.Context, string, string, [32]byte, uint64) (r2.RemoteObjectCommit, error) {
	return r2.RemoteObjectCommit{}, r2.ErrImmutableCollision
}

func (*failingAppBackend) VerifyFile(context.Context, string, string, [32]byte, uint64) (r2.RemoteObjectVerification, error) {
	return r2.RemoteObjectVerification{}, r2.ErrImmutableCollision
}

var _ r2.WriteBackend = (*failingAppBackend)(nil)
