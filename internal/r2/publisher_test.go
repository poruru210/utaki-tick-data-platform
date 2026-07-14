package r2

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"tick-data-platform/internal/archive"
	"tick-data-platform/internal/protocol"
	"tick-data-platform/internal/wal"
)

type fakeBackend struct {
	mu      sync.Mutex
	objects map[string][]byte
	puts    []string
}

func newFakeBackend() *fakeBackend {
	return &fakeBackend{objects: make(map[string][]byte)}
}

func (b *fakeBackend) PutIfAbsent(_ context.Context, key string, body []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.objects[key]; ok {
		return ErrObjectExists
	}
	b.objects[key] = append([]byte(nil), body...)
	b.puts = append(b.puts, key)
	return nil
}

func (b *fakeBackend) Get(_ context.Context, key string) ([]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	body, ok := b.objects[key]
	if !ok {
		return nil, ErrObjectNotFound
	}
	return append([]byte(nil), body...), nil
}

func (b *fakeBackend) List(_ context.Context, prefix string) ([]RemoteObject, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	keys := make([]string, 0)
	for key := range b.objects {
		if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	result := make([]RemoteObject, 0, len(keys))
	for _, key := range keys {
		result = append(result, RemoteObject{Key: key, Size: int64(len(b.objects[key]))})
	}
	return result, nil
}

func (b *fakeBackend) force(key string, body []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.objects[key] = append([]byte(nil), body...)
}

type fakeRcloneExecutor struct {
	backend             *fakeBackend
	calls               [][]string
	mu                  sync.Mutex
	failOn              string
	mutateOnObjectCheck bool
	objectCheckCount    int
}

func (e *fakeRcloneExecutor) run(_ context.Context, executable string, args ...string) (string, error) {
	e.mu.Lock()
	e.calls = append(e.calls, append([]string{executable}, args...))
	failOn := e.failOn
	e.mu.Unlock()
	if len(args) == 1 && args[0] == "version" {
		return "rclone v1.74.4\n", nil
	}
	if len(args) != 4 && len(args) != 3 {
		return "", errors.New("unexpected fake rclone argv")
	}
	operation := args[0]
	if operation == failOn {
		return "", errors.New("injected rclone failure")
	}
	if operation == "copyto" {
		body, err := os.ReadFile(args[2])
		if err != nil {
			return "", err
		}
		if existing, err := e.backend.Get(context.Background(), args[3]); err == nil {
			if !bytes.Equal(existing, body) {
				return "", errors.New("immutable content collision")
			}
			return "", nil
		}
		e.backend.force(args[3], body)
		return "", nil
	}
	if operation == "check" {
		local, err := os.ReadFile(args[2])
		if err != nil {
			return "", err
		}
		if e.mutateOnObjectCheck && bytes.Contains([]byte(args[3]), []byte("objects/raw")) {
			e.mu.Lock()
			e.objectCheckCount++
			mutate := e.objectCheckCount == 3
			e.mu.Unlock()
			if mutate {
				e.backend.force(args[3], []byte("tampered-remote-object"))
			}
		}
		remote, err := e.backend.Get(context.Background(), args[3])
		if err != nil || !bytes.Equal(local, remote) {
			return "", errors.New("fake rclone check mismatch")
		}
		return "", nil
	}
	return "", errors.New("forbidden fake operation")
}

func TestPublisherFirstAndIdempotentRetry(t *testing.T) {
	fixture := newPublicationFixture(t)
	publisher := fixture.publisher(t, nil)
	if _, err := publisher.Publish(context.Background(), fixture.input); err != nil {
		t.Fatal(err)
	}
	if _, err := publisher.Publish(context.Background(), fixture.input); err != nil {
		t.Fatalf("same-content retry: %v", err)
	}
	key, err := fixture.layout.ManifestKey(fixture.manifest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.backend.Get(context.Background(), key); err != nil {
		t.Fatalf("manifest was not published: %v", err)
	}
	claimKey, err := fixture.layout.ClaimKey(fixture.scope.PublisherEpoch)
	if err != nil {
		t.Fatal(err)
	}
	claimBytes, err := fixture.backend.Get(context.Background(), claimKey)
	if err != nil || len(claimBytes) == 0 {
		t.Fatalf("publisher claim missing: %v", err)
	}
	descriptorKey, err := fixture.layout.ScopeDescriptorKey()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.backend.Get(context.Background(), descriptorKey); err != nil {
		t.Fatalf("scope descriptor was not transferred through rclone: %v", err)
	}
	claimKey, err = fixture.layout.ClaimKey(fixture.scope.PublisherEpoch)
	if err != nil {
		t.Fatal(err)
	}
	fixture.backend.mu.Lock()
	puts := append([]string(nil), fixture.backend.puts...)
	fixture.backend.mu.Unlock()
	if len(puts) != 1 || puts[0] != claimKey {
		t.Fatalf("conditional backend writes = %v, want publisher claim only %q", puts, claimKey)
	}
}

func TestPublisherRejectsPublisherConflictAndLockConflict(t *testing.T) {
	fixture := newPublicationFixture(t)
	claimKey, err := fixture.layout.ClaimKey(fixture.scope.PublisherEpoch)
	if err != nil {
		t.Fatal(err)
	}
	fixture.backend.force(claimKey, []byte("different-publisher"))
	if _, err := fixture.publisher(t, nil).Publish(context.Background(), fixture.input); !errors.Is(err, ErrPublisherConflict) {
		t.Fatalf("publisher conflict error = %v, want ErrPublisherConflict", err)
	}

	second := newPublicationFixture(t)
	lockPath := filepath.Join(filepath.Dir(second.journal.Path()), "campaign.lock")
	owner, err := AcquirePublicationLock(lockPath)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()
	if _, err := second.publisher(t, nil).Publish(context.Background(), second.input); !errors.Is(err, ErrPublicationLock) {
		t.Fatalf("publisher lock conflict error = %v, want ErrPublicationLock", err)
	}
}

func TestPublisherCheckFailureStopsBeforeManifest(t *testing.T) {
	fixture := newPublicationFixture(t)
	fixture.executor.failOn = "check"
	if _, err := fixture.publisher(t, nil).Publish(context.Background(), fixture.input); err == nil {
		t.Fatal("check/download failure was not returned")
	}
	manifestKey, err := fixture.layout.ManifestKey(fixture.manifest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.backend.Get(context.Background(), manifestKey); !errors.Is(err, ErrObjectNotFound) {
		t.Fatalf("manifest exists after check failure: %v", err)
	}
}

func TestPublisherRemoteRawMutationStopsBeforeManifest(t *testing.T) {
	fixture := newPublicationFixture(t)
	fixture.executor.mutateOnObjectCheck = true
	if _, err := fixture.publisher(t, nil).Publish(context.Background(), fixture.input); err == nil {
		t.Fatal("remote raw mutation was not detected")
	}
	manifestKey, err := fixture.layout.ManifestKey(fixture.manifest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.backend.Get(context.Background(), manifestKey); !errors.Is(err, ErrObjectNotFound) {
		t.Fatalf("manifest exists after remote raw mutation: %v", err)
	}
}

func TestPublisherRestartsAfterEveryDurableStage(t *testing.T) {
	stages := []string{StageClaimed, StageObjectsCopied, StageObjectsVerified, StageManifestCopied, StageManifestVerified, StageReceiptSaved}
	for _, target := range stages {
		t.Run(target, func(t *testing.T) {
			fixture := newPublicationFixture(t)
			crash := errors.New("simulated process crash")
			first := fixture.publisher(t, func(stage string) error {
				if stage == target {
					return crash
				}
				return nil
			})
			if _, err := first.Publish(context.Background(), fixture.input); !errors.Is(err, crash) {
				t.Fatalf("first publish error = %v, want simulated crash", err)
			}
			second := fixture.publisher(t, nil)
			if _, err := second.Publish(context.Background(), fixture.input); err != nil {
				t.Fatalf("restart after %s: %v", target, err)
			}
		})
	}
}

func TestPublisherFailureBeforeManifestLeavesDataWithoutManifest(t *testing.T) {
	fixture := newPublicationFixture(t)
	// The descriptor is intentionally allowed through, while the manifest is
	// failed after objects have been copied and checked by the fake below.
	fixture.executor.failManifest = true
	publisher := fixture.publisher(t, nil)
	if _, err := publisher.Publish(context.Background(), fixture.input); err == nil {
		t.Fatal("manifest copy failure was not returned")
	}
	manifestKey, err := fixture.layout.ManifestKey(fixture.manifest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.backend.Get(context.Background(), manifestKey); !errors.Is(err, ErrObjectNotFound) {
		t.Fatalf("manifest exists after failed publication: %v", err)
	}
}

type publicationFixture struct {
	scope    archive.ScopeConfig
	layout   Layout
	manifest archive.RawDayManifest
	input    PublicationInput
	backend  *fakeBackend
	executor *testRcloneExecutor
	journal  *PublicationJournal
	toolPath string
}

type testRcloneExecutor struct {
	fakeRcloneExecutor
	failManifest bool
}

func (e *testRcloneExecutor) run(ctx context.Context, executable string, args ...string) (string, error) {
	if e.failManifest && len(args) > 0 && args[0] == "copyto" && len(args) > 3 && bytes.Contains([]byte(args[3]), []byte("snapshots/raw")) {
		return "", errors.New("injected manifest copy failure")
	}
	return e.fakeRcloneExecutor.run(ctx, executable, args...)
}

func newPublicationFixture(t *testing.T) *publicationFixture {
	t.Helper()
	scope := layoutTestScope()
	layout, err := NewLayout("v1", "v1", scope)
	if err != nil {
		t.Fatal(err)
	}
	object := publicationObject(t)
	manifest, err := archive.BuildRawDayManifest(archive.RawDayManifestInput{
		Scope:              scope,
		Date:               "2024-03-09",
		RawObjects:         []archive.RawObject{object},
		TerminalSyncStatus: "complete",
		CompletenessStatus: "settled_snapshot",
		LogicalCloseTimeS:  1710028800,
	})
	if err != nil {
		t.Fatal(err)
	}
	manifestBytes, err := archive.ManifestCanonicalJSON(manifest)
	if err != nil {
		t.Fatal(err)
	}
	backend := newFakeBackend()
	executor := &testRcloneExecutor{fakeRcloneExecutor: fakeRcloneExecutor{backend: backend}}
	data := []byte("fake-rclone-binary")
	toolPath := filepath.Join(t.TempDir(), "rclone.exe")
	if err := os.WriteFile(toolPath, data, 0o700); err != nil {
		t.Fatal(err)
	}
	journal, err := OpenPublicationJournal(filepath.Join(t.TempDir(), "publication.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = journal.Close() })
	return &publicationFixture{
		scope: scope, layout: layout, manifest: manifest,
		input:   PublicationInput{Manifest: manifest, ManifestBytes: manifestBytes, ObjectPaths: map[string]string{object.Key: object.Path}},
		backend: backend, executor: executor, journal: journal, toolPath: toolPath,
	}
}

func (f *publicationFixture) publisher(t *testing.T, afterStage func(string) error) *Publisher {
	t.Helper()
	publisher, err := NewPublisher(f.layout, f.backend, &RcloneRunner{
		binaryPath: f.toolPath,
		tool:       RcloneTool{GOOS: "windows", GOARCH: "amd64", BinaryBytes: uint64(len("fake-rclone-binary")), BinarySHA256: fmt.Sprintf("%x", sha256.Sum256([]byte("fake-rclone-binary")))},
		executor:   f.executor,
	}, f.journal, filepath.Join(filepath.Dir(f.journal.Path()), "campaign.lock"))
	if err != nil {
		t.Fatal(err)
	}
	publisher.hooks.afterStage = afterStage
	return publisher
}

func publicationObject(t *testing.T) archive.RawObject {
	t.Helper()
	root := t.TempDir()
	store, err := wal.Open(root, "gateway-test-01")
	if err != nil {
		t.Fatal(err)
	}
	frame, err := protocol.EncodeMessage(protocol.BatchFrameV1{
		RequestedFromMSC: time.Date(2024, 3, 9, 0, 0, 0, 0, time.UTC).UnixMilli(),
		ReturnedCount:    1,
		SourceSchemaID:   protocol.SourceSchemaMT5,
		Records: []protocol.RawMqlTickV1{{
			TimeMSC:         time.Date(2024, 3, 9, 0, 0, 1, 0, time.UTC).UnixMilli(),
			CaptureSequence: 1,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(frame, 1710000000, 42); err != nil {
		t.Fatal(err)
	}
	sealed, err := store.Seal()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	object, err := archive.PromoteSealedSegment(t.TempDir(), sealed.Path)
	if err != nil {
		t.Fatal(err)
	}
	return object
}
