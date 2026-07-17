package r2

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"tick-data-platform/internal/archive"
	"tick-data-platform/internal/protocol"
	"tick-data-platform/internal/wal"
)

type fakeBackend struct {
	mu                       sync.Mutex
	objects                  map[string][]byte
	puts                     []string
	listExtras               []RemoteObject
	listCount                int
	listStarted              chan struct{}
	filePuts                 []string
	fileMutations            map[string]int
	fileVerifies             []string
	failVerify               bool
	failManifest             bool
	mutateOnObjectVerify     bool
	objectVerifyCount        int
	timeoutNext              bool
	mutateOnDescriptorVerify bool
	descriptorVerifyCount    int
}

func newFakeBackend() *fakeBackend {
	return &fakeBackend{objects: make(map[string][]byte), fileMutations: make(map[string]int)}
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

func (b *fakeBackend) GetLimited(_ context.Context, key string, maxBytes uint64) ([]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	body, ok := b.objects[key]
	if !ok {
		return nil, ErrObjectNotFound
	}
	if uint64(len(body)) > maxBytes {
		return nil, fmt.Errorf("%w: fake metadata object is oversized", ErrResourceLimit)
	}
	return append([]byte(nil), body...), nil
}

func (b *fakeBackend) Open(ctx context.Context, key string) (io.ReadCloser, int64, error) {
	b.mu.Lock()
	body, ok := b.objects[key]
	if !ok {
		b.mu.Unlock()
		return nil, 0, ErrObjectNotFound
	}
	body = append([]byte(nil), body...)
	b.mu.Unlock()
	return io.NopCloser(bytes.NewReader(body)), int64(len(body)), nil
}

func (b *fakeBackend) List(_ context.Context, prefix string) ([]RemoteObject, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.listCount++
	if b.listStarted != nil {
		select {
		case b.listStarted <- struct{}{}:
		default:
		}
	}
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
	for _, extra := range b.listExtras {
		if strings.HasPrefix(extra.Key, prefix) {
			result = append(result, extra)
		}
	}
	return result, nil
}

func (b *fakeBackend) ListLimited(_ context.Context, prefix string, maxObjects uint64) ([]RemoteObject, error) {
	if maxObjects == 0 {
		return nil, fmt.Errorf("%w: fake object count limit is zero", ErrResourceLimit)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.listCount++
	if b.listStarted != nil {
		select {
		case b.listStarted <- struct{}{}:
		default:
		}
	}
	result := make([]RemoteObject, 0)
	appendObject := func(remote RemoteObject) error {
		if uint64(len(result)) >= maxObjects {
			return fmt.Errorf("%w: fake derivative object count exceeds limit", ErrResourceLimit)
		}
		result = append(result, remote)
		return nil
	}
	for key, body := range b.objects {
		if strings.HasPrefix(key, prefix) {
			if err := appendObject(RemoteObject{Key: key, Size: int64(len(body))}); err != nil {
				return nil, err
			}
		}
	}
	for _, extra := range b.listExtras {
		if strings.HasPrefix(extra.Key, prefix) {
			if err := appendObject(extra); err != nil {
				return nil, err
			}
		}
	}
	return result, nil
}

func (b *fakeBackend) force(key string, body []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.objects[key] = append([]byte(nil), body...)
}

func (b *fakeBackend) remove(key string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.objects, key)
}

func (b *fakeBackend) currentListCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.listCount
}

func (b *fakeBackend) PutFileIfAbsent(_ context.Context, key, path string, expectedSHA256 [32]byte, expectedBytes uint64) (RemoteObjectCommit, error) {
	body, err := readVerifiedFakeFile(path, expectedSHA256, expectedBytes)
	if err != nil {
		return RemoteObjectCommit{}, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.timeoutNext {
		b.timeoutNext = false
		return RemoteObjectCommit{}, context.DeadlineExceeded
	}
	b.filePuts = append(b.filePuts, key)
	if b.failManifest && strings.Contains(key, "snapshots/raw") {
		return RemoteObjectCommit{}, errors.New("injected manifest copy failure")
	}
	if existing, ok := b.objects[key]; ok {
		if bytes.Equal(existing, body) {
			return RemoteObjectCommit{ETag: "fake-etag-" + key}, nil
		}
		return RemoteObjectCommit{}, ErrImmutableCollision
	}
	b.objects[key] = append([]byte(nil), body...)
	b.fileMutations[key]++
	return RemoteObjectCommit{ETag: "fake-etag-" + key}, nil
}

func (b *fakeBackend) mutationCount(key string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.fileMutations[key]
}

func (b *fakeBackend) VerifyFile(_ context.Context, key, path string, expectedSHA256 [32]byte, expectedBytes uint64) (RemoteObjectVerification, error) {
	local, err := readVerifiedFakeFile(path, expectedSHA256, expectedBytes)
	if err != nil {
		return RemoteObjectVerification{}, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.timeoutNext {
		b.timeoutNext = false
		return RemoteObjectVerification{}, context.DeadlineExceeded
	}
	b.fileVerifies = append(b.fileVerifies, key)
	if b.failVerify {
		return RemoteObjectVerification{}, errors.New("injected remote verification failure")
	}
	if b.mutateOnObjectVerify && strings.Contains(key, "objects/raw") {
		b.objectVerifyCount++
		if b.objectVerifyCount == 3 {
			b.objects[key] = []byte("tampered-remote-object")
		}
	}
	if b.mutateOnDescriptorVerify && strings.Contains(key, "scope-descriptor-v1.json") {
		b.descriptorVerifyCount++
		if b.descriptorVerifyCount == 2 {
			b.objects[key] = []byte("tampered-remote-scope-descriptor")
		}
	}
	remote, ok := b.objects[key]
	if !ok || !bytes.Equal(local, remote) {
		return RemoteObjectVerification{}, ErrRemoteCheckMismatch
	}
	return RemoteObjectVerification{ETag: "fake-etag-" + key}, nil
}

func readVerifiedFakeFile(path string, expectedSHA256 [32]byte, expectedBytes uint64) ([]byte, error) {
	body, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if uint64(len(body)) != expectedBytes || sha256.Sum256(body) != expectedSHA256 {
		return nil, ErrLocalObjectChanged
	}
	return body, nil
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
		t.Fatalf("scope descriptor was not published through R2 API: %v", err)
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
	rawKey, err := fixture.layout.RawObjectKey(fixture.manifest.ChainObjects[0])
	if err != nil {
		t.Fatal(err)
	}
	descriptorKey, err = fixture.layout.ScopeDescriptorKey()
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{rawKey, descriptorKey} {
		if got := fixture.backend.mutationCount(key); got != 1 {
			t.Fatalf("duplicate publication mutated %q %d times, want once", key, got)
		}
	}
	states, err := fixture.journal.ObjectStateRecords(key)
	if err != nil {
		t.Fatal(err)
	}
	wantStates := []string{ObjectStateSealedLocal, ObjectStateUploading, ObjectStateRemoteCommitted, ObjectStateRemoteVerified}
	if len(states) != len(wantStates) {
		t.Fatalf("object state count = %d, want %d: %+v", len(states), len(wantStates), states)
	}
	raw := fixture.manifest.ChainObjects[0]
	rawRemoteKey, err := fixture.layout.RemoteKey(raw.Key)
	if err != nil {
		t.Fatal(err)
	}
	for index, state := range states {
		if state.State != wantStates[index] || state.Identity != key || state.ObjectKey != rawRemoteKey || state.LocalPath != fixture.input.ObjectPaths[raw.Key] || state.Size != raw.Bytes || state.SHA256 != raw.SHA256 || state.MD5 == ([16]byte{}) || state.UploadMethod != UploadMethodS3PutObjectV1 {
			t.Fatalf("object state[%d] = %+v", index, state)
		}
	}
	if states[2].RemoteETag == "" || states[3].RemoteETag == "" || states[3].RemoteVerifiedAt.IsZero() {
		t.Fatalf("remote commit/verification metadata missing: %+v", states)
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
	lockPath := filepath.Join(filepath.Dir(second.journal.Path()), "publication.lock")
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
	fixture.backend.failVerify = true
	if _, err := fixture.publisher(t, nil).Publish(context.Background(), fixture.input); err == nil {
		t.Fatal("remote verification failure was not returned")
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
	fixture.backend.mutateOnObjectVerify = true
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

func TestPublisherScopeDescriptorMutationStopsBeforeManifest(t *testing.T) {
	fixture := newPublicationFixture(t)
	fixture.backend.mutateOnDescriptorVerify = true
	if _, err := fixture.publisher(t, nil).Publish(context.Background(), fixture.input); err == nil {
		t.Fatal("remote scope descriptor mutation was not detected")
	}
	descriptorKey, err := fixture.layout.ScopeDescriptorKey()
	if err != nil {
		t.Fatal(err)
	}
	descriptor, err := fixture.backend.Get(context.Background(), descriptorKey)
	if err != nil || !bytes.Equal(descriptor, []byte("tampered-remote-scope-descriptor")) {
		t.Fatalf("scope descriptor mutation = %q, want tampered bytes, err=%v", descriptor, err)
	}
	manifestKey, err := fixture.layout.ManifestKey(fixture.manifest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.backend.Get(context.Background(), manifestKey); !errors.Is(err, ErrObjectNotFound) {
		t.Fatalf("manifest exists after scope descriptor mutation: %v", err)
	}
}

func TestPublisherRawImmutableCollisionPreservesOriginalAndOmitsManifest(t *testing.T) {
	fixture := newPublicationFixture(t)
	rawKey, err := fixture.layout.RawObjectKey(fixture.manifest.ChainObjects[0])
	if err != nil {
		t.Fatal(err)
	}
	original := []byte("preexisting immutable raw bytes")
	fixture.backend.force(rawKey, original)
	if _, err := fixture.publisher(t, nil).Publish(context.Background(), fixture.input); err == nil {
		t.Fatal("raw immutable collision was accepted")
	}
	fixture.backend.mu.Lock()
	copyAttempted := false
	for _, key := range fixture.backend.filePuts {
		if key == rawKey {
			copyAttempted = true
		}
	}
	fixture.backend.mu.Unlock()
	if !copyAttempted {
		t.Fatalf("immutable raw PutObject was not attempted for %q", rawKey)
	}
	got, err := fixture.backend.Get(context.Background(), rawKey)
	if err != nil || !bytes.Equal(got, original) {
		t.Fatalf("preexisting raw bytes = %q, want %q, err=%v", got, original, err)
	}
	manifestKey, err := fixture.layout.ManifestKey(fixture.manifest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.backend.Get(context.Background(), manifestKey); !errors.Is(err, ErrObjectNotFound) {
		t.Fatalf("manifest exists after raw immutable collision: %v", err)
	}
}

func TestPublisherDoesNotExposeEnvironmentSecret(t *testing.T) {
	secret := "sentinel-r2-secret-7f5c"
	t.Setenv("R2_TEST_SECRET", secret)
	fixture := newPublicationFixture(t)
	receiptPath := filepath.Join(t.TempDir(), "receipt.json")
	fixture.input.ReceiptPath = receiptPath
	if _, err := fixture.publisher(t, nil).Publish(context.Background(), fixture.input); err != nil {
		t.Fatal(err)
	}
	manifestKey, err := fixture.layout.ManifestKey(fixture.manifest)
	if err != nil {
		t.Fatal(err)
	}
	record, found, err := fixture.journal.Record(manifestKey)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("publication journal record is missing")
	}
	if bytes.Contains(record.IntentBytes, []byte(secret)) {
		t.Fatal("environment secret appeared in journal intent bytes")
	}
	databaseBytes, err := os.ReadFile(fixture.journal.Path())
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(databaseBytes, []byte(secret)) {
		t.Fatal("environment secret appeared in SQLite bytes")
	}
	receiptBytes, err := os.ReadFile(receiptPath)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(receiptBytes, []byte(secret)) {
		t.Fatal("environment secret appeared in receipt bytes")
	}

	fixture.backend.failVerify = true
	if _, err := fixture.publisher(t, nil).Publish(context.Background(), fixture.input); err == nil || bytes.Contains([]byte(err.Error()), []byte(secret)) {
		t.Fatalf("returned error exposed secret or was absent: %v", err)
	}
}

func TestPublisherRetriesAfterContextDeadline(t *testing.T) {
	fixture := newPublicationFixture(t)
	fixture.backend.timeoutNext = true
	if _, err := fixture.publisher(t, nil).Publish(context.Background(), fixture.input); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("timeout error = %v, want context deadline", err)
	}
	if _, err := fixture.publisher(t, nil).Publish(context.Background(), fixture.input); err != nil {
		t.Fatalf("reconcile after timeout: %v", err)
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
	fixture.backend.failManifest = true
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
	journal  *PublicationJournal
}

func newPublicationFixture(t *testing.T) *publicationFixture {
	t.Helper()
	scope := layoutTestScope()
	layout, err := NewLayout("v1", scope)
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
	journal := newStartedPublicationJournal(t, filepath.Join(t.TempDir(), "publication.sqlite"))
	return &publicationFixture{
		scope: scope, layout: layout, manifest: manifest,
		input:   PublicationInput{Manifest: manifest, ManifestBytes: manifestBytes, ObjectPaths: map[string]string{object.Key: object.Path}},
		backend: backend, journal: journal,
	}
}

func (f *publicationFixture) publisher(t *testing.T, afterStage func(string) error) *Publisher {
	t.Helper()
	publisher, err := NewPublisher(f.layout, f.backend, f.journal, filepath.Join(filepath.Dir(f.journal.Path()), "publication.lock"), time.Now)
	if err != nil {
		t.Fatal(err)
	}
	publisher.hooks.afterStage = afterStage
	return publisher
}

func publicationObject(t *testing.T) archive.RawObject {
	t.Helper()
	root := t.TempDir()
	store, err := newStartedPublisherWAL(root, "gateway-test-01")
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
	if err := store.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	object, err := archive.PromoteSealedSegment(t.TempDir(), sealed.Path)
	if err != nil {
		t.Fatal(err)
	}
	return object
}

func newStartedPublisherWAL(root, gatewayID string) (*wal.Store, error) {
	store, err := wal.NewStore(root, gatewayID, nil)
	if err != nil {
		return nil, err
	}
	if err := store.Start(context.Background()); err != nil {
		return nil, err
	}
	return store, nil
}
