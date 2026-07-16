package delivery

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"tick-data-platform/internal/archive"
	"tick-data-platform/internal/r2"
	"tick-data-platform/internal/wal"
)

type m2E2EBackend struct {
	mu      sync.Mutex
	objects map[string][]byte
	puts    []string
}

func (b *m2E2EBackend) PutIfAbsent(_ context.Context, key string, body []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.objects[key]; ok {
		return r2.ErrObjectExists
	}
	b.objects[key] = append([]byte(nil), body...)
	b.puts = append(b.puts, key)
	return nil
}

func (b *m2E2EBackend) List(_ context.Context, prefix string) ([]r2.RemoteObject, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	keys := make([]string, 0)
	for key := range b.objects {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	result := make([]r2.RemoteObject, 0, len(keys))
	for _, key := range keys {
		result = append(result, r2.RemoteObject{Key: key, Size: int64(len(b.objects[key]))})
	}
	return result, nil
}

func (b *m2E2EBackend) ListLimited(ctx context.Context, prefix string, maxObjects uint64) ([]r2.RemoteObject, error) {
	if maxObjects == 0 {
		return nil, r2.ErrResourceLimit
	}
	result, err := b.List(ctx, prefix)
	if err != nil {
		return nil, err
	}
	if uint64(len(result)) > maxObjects {
		return nil, r2.ErrResourceLimit
	}
	return result, nil
}

func (b *m2E2EBackend) Get(_ context.Context, key string) ([]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	body, ok := b.objects[key]
	if !ok {
		return nil, r2.ErrObjectNotFound
	}
	return append([]byte(nil), body...), nil
}

func (b *m2E2EBackend) GetLimited(ctx context.Context, key string, maxBytes uint64) ([]byte, error) {
	if maxBytes == 0 {
		return nil, r2.ErrResourceLimit
	}
	body, err := b.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if uint64(len(body)) > maxBytes {
		return nil, r2.ErrResourceLimit
	}
	return body, nil
}

func (b *m2E2EBackend) Open(ctx context.Context, key string) (io.ReadCloser, int64, error) {
	body, err := b.Get(ctx, key)
	if err != nil {
		return nil, 0, err
	}
	return io.NopCloser(bytes.NewReader(body)), int64(len(body)), nil
}

func (b *m2E2EBackend) PutFileIfAbsent(_ context.Context, key, path string, expectedSHA256 [32]byte, expectedBytes uint64) (r2.RemoteObjectCommit, error) {
	body, err := m2E2EReadVerifiedFile(path, expectedSHA256, expectedBytes)
	if err != nil {
		return r2.RemoteObjectCommit{}, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if existing, ok := b.objects[key]; ok {
		if bytes.Equal(existing, body) {
			return r2.RemoteObjectCommit{ETag: "m2-e2e-etag-" + key}, nil
		}
		return r2.RemoteObjectCommit{}, r2.ErrImmutableCollision
	}
	b.objects[key] = append([]byte(nil), body...)
	return r2.RemoteObjectCommit{ETag: "m2-e2e-etag-" + key}, nil
}

func (b *m2E2EBackend) VerifyFile(_ context.Context, key, path string, expectedSHA256 [32]byte, expectedBytes uint64) (r2.RemoteObjectVerification, error) {
	local, err := m2E2EReadVerifiedFile(path, expectedSHA256, expectedBytes)
	if err != nil {
		return r2.RemoteObjectVerification{}, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	remote, ok := b.objects[key]
	if !ok || !bytes.Equal(local, remote) {
		return r2.RemoteObjectVerification{}, r2.ErrRemoteCheckMismatch
	}
	return r2.RemoteObjectVerification{ETag: "m2-e2e-etag-" + key}, nil
}

func m2E2EReadVerifiedFile(path string, expectedSHA256 [32]byte, expectedBytes uint64) ([]byte, error) {
	body, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if uint64(len(body)) != expectedBytes || sha256.Sum256(body) != expectedSHA256 {
		return nil, r2.ErrLocalObjectChanged
	}
	return body, nil
}

var _ r2.ObjectBackend = (*m2E2EBackend)(nil)
var _ r2.ReadBackend = (*m2E2EBackend)(nil)
var _ r2.WriteBackend = (*m2E2EBackend)(nil)

func TestM2RawOffhostDeliveryEndToEndFake(t *testing.T) {
	scope := m2E2EScope()
	layout, err := r2.NewLayout("v1", scope)
	if err != nil {
		t.Fatal(err)
	}
	object, frames := m2E2ESealedObject(t)
	manifest, err := archive.BuildRawDayManifest(archive.RawDayManifestInput{
		Scope:              scope,
		Date:               "2024-03-09",
		RawObjects:         []archive.RawObject{object},
		TerminalSyncStatus: "complete",
		CompletenessStatus: "settled_snapshot",
		LogicalCloseTimeS:  time.Date(2024, 3, 9, 0, 1, 0, 0, time.UTC).Unix(),
	})
	if err != nil {
		t.Fatal(err)
	}
	manifestBytes, err := archive.ManifestCanonicalJSON(manifest)
	if err != nil {
		t.Fatal(err)
	}
	backend := &m2E2EBackend{objects: make(map[string][]byte)}
	journal, err := r2.OpenPublicationJournal(filepath.Join(t.TempDir(), "publication.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = journal.Close() })
	publisher, err := r2.NewPublisher(layout, backend, journal, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := publisher.Publish(context.Background(), r2.PublicationInput{
		Manifest: manifest, ManifestBytes: manifestBytes,
		ObjectPaths: map[string]string{object.Key: object.Path},
	}); err != nil {
		t.Fatal(err)
	}
	manifestKey, err := layout.ManifestKey(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := backend.Get(context.Background(), manifestKey); err != nil {
		t.Fatalf("published manifest is missing: %v", err)
	}
	claimKey, err := layout.ClaimKey(scope.PublisherEpoch)
	if err != nil {
		t.Fatal(err)
	}
	backend.mu.Lock()
	puts := append([]string(nil), backend.puts...)
	backend.mu.Unlock()
	if len(puts) != 1 || puts[0] != claimKey {
		t.Fatalf("conditional claim writes = %v, want only %q", puts, claimKey)
	}

	cacheRoot := t.TempDir()
	reader, err := NewArchiveReaderV1WithBackend(ReaderConfig{
		Version: ReaderConfigVersion, Endpoint: "https://0123456789abcdef0123456789abcdef.r2.cloudflarestorage.com",
		Bucket: "m2-e2e-bucket", CredentialsPath: filepath.Join(t.TempDir(), "credentials.json"),
		Region: "auto", ImmutableRoot: "v1", CacheRoot: cacheRoot,
		MaxMetadataBytes: 1 << 20, MaxRawObjectBytes: 1 << 30,
	}, backend)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := reader.ResolveSnapshot(context.Background(), SnapshotSelector{Manifest: manifestKey})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := reader.BuildFetchPlan(context.Background(), resolved)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(plan.ManifestCachePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("manifest cache was not empty: %v", err)
	}
	result, err := reader.Fetch(context.Background(), plan, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Entries) != 2 || !bytes.Equal(result.Entries[0].Frame, frames[0]) || !bytes.Equal(result.Entries[1].Frame, frames[1]) {
		t.Fatal("empty-cache fetch did not restore exact BatchFrameV1 bytes")
	}
	if result.Entries[1].Batch.CopyTicksError == 0 || len(result.Entries[1].Batch.Records) != 0 {
		t.Fatalf("zero-record error batch was not restored: %+v", result.Entries[1].Batch)
	}
	dayReport, err := reader.VerifyDay(context.Background(), SnapshotSelector{Manifest: manifestKey})
	if err != nil {
		t.Fatal(err)
	}
	if dayReport.GenesisVerified || dayReport.VerificationScope != VerificationScopeAnchoredDay {
		t.Fatalf("day report = %+v", dayReport)
	}
	campaignReport, err := reader.VerifyCampaign(context.Background(), scope.DatasetID, scope.CampaignID, hex.EncodeToString(object.Segment.ChainRoot[:]))
	if err != nil {
		t.Fatal(err)
	}
	if !campaignReport.GenesisVerified || campaignReport.VerificationScope != VerificationScopeCampaign || campaignReport.VerifiedThrough != 2 {
		t.Fatalf("campaign report = %+v", campaignReport)
	}
}

func m2E2EScope() archive.ScopeConfig {
	return archive.ScopeConfig{
		DatasetID: "m2-e2e-dataset", CampaignID: "m2-e2e-campaign", ProviderID: "m2-e2e-provider",
		StableFeedID: "m2-e2e-feed", ExactSourceSymbol: "EURUSD.raw", BrokerServerFingerprint: "m2-e2e-server",
		GatewayBuildIdentity: "m2-e2e-gateway", ProducerBuildIdentity: "m2-e2e-producer", DayDefinitionID: "utc-day-v1",
		SettlePolicy: "manual-v1", PublisherID: "m2-e2e-publisher", PublisherEpoch: 1,
	}
}

func m2E2ESealedObject(t *testing.T) (archive.RawObject, [][]byte) {
	t.Helper()
	frames := [][]byte{readerTestFrame(t, time.Date(2024, 3, 9, 0, 0, 0, 100000000, time.UTC).UnixMilli(), 1, false), readerTestFrame(t, time.Date(2024, 3, 9, 0, 0, 0, 0, time.UTC).UnixMilli(), 2, true)}
	root := t.TempDir()
	store, err := wal.Open(root, "m2-e2e-gateway")
	if err != nil {
		t.Fatal(err)
	}
	for index, frame := range frames {
		if _, err := store.Append(frame, 1710000000+int64(index), uint64(index+1)); err != nil {
			_ = store.Close()
			t.Fatal(err)
		}
	}
	sealed, err := store.Seal()
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	object, err := archive.PromoteSealedSegment(t.TempDir(), sealed.Path)
	if err != nil {
		t.Fatal(err)
	}
	return object, frames
}
