package delivery

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"tick-data-platform/internal/archive"
	"tick-data-platform/internal/protocol"
	"tick-data-platform/internal/r2"
	"tick-data-platform/internal/wal"
	"tick-data-platform/producers/fake"
)

type memoryReadBackend struct {
	objects          map[string][]byte
	openError        map[string]error
	closeError       map[string]error
	duplicateListKey string
}

type memoryReadCloser struct {
	io.Reader
	closeErr error
}

func (r memoryReadCloser) Close() error { return r.closeErr }

var _ r2.ReadBackend = (*memoryReadBackend)(nil)

func (b *memoryReadBackend) List(_ context.Context, prefix string) ([]r2.RemoteObject, error) {
	result := make([]r2.RemoteObject, 0)
	for key, body := range b.objects {
		if strings.HasPrefix(key, prefix) {
			result = append(result, r2.RemoteObject{Key: key, Size: int64(len(body))})
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Key < result[j].Key })
	if b.duplicateListKey != "" && strings.HasPrefix(b.duplicateListKey, prefix) {
		result = append(result, r2.RemoteObject{Key: b.duplicateListKey, Size: int64(len(b.objects[b.duplicateListKey]))})
	}
	return result, nil
}

func (b *memoryReadBackend) ListLimited(ctx context.Context, prefix string, maxObjects uint64) ([]r2.RemoteObject, error) {
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

func (b *memoryReadBackend) Get(_ context.Context, key string) ([]byte, error) {
	body, ok := b.objects[key]
	if !ok {
		return nil, r2.ErrObjectNotFound
	}
	return append([]byte(nil), body...), nil
}

func (b *memoryReadBackend) GetLimited(ctx context.Context, key string, maxBytes uint64) ([]byte, error) {
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

func (b *memoryReadBackend) Open(_ context.Context, key string) (io.ReadCloser, int64, error) {
	if err, ok := b.openError[key]; ok {
		return nil, 0, err
	}
	body, ok := b.objects[key]
	if !ok {
		return nil, 0, r2.ErrObjectNotFound
	}
	if closeErr := b.closeError[key]; closeErr != nil {
		return memoryReadCloser{Reader: bytes.NewReader(append([]byte(nil), body...)), closeErr: closeErr}, int64(len(body)), nil
	}
	return io.NopCloser(bytes.NewReader(append([]byte(nil), body...))), int64(len(body)), nil
}

type deliveryFixture struct {
	reader    ArchiveReaderV1
	backend   *memoryReadBackend
	scope     archive.ScopeConfig
	layout    r2.Layout
	objects   []archive.RawObject
	manifestA archive.RawDayManifest
	manifestB archive.RawDayManifest
}

func newDeliveryFixture(t *testing.T) deliveryFixture {
	t.Helper()
	scope := archive.ScopeConfig{
		DatasetID:               "dataset-reader",
		CampaignID:              "campaign-reader",
		ProviderID:              "provider-reader",
		StableFeedID:            "feed-reader",
		ExactSourceSymbol:       "EURUSD.raw",
		BrokerServerFingerprint: "server-reader",
		GatewayBuildIdentity:    "gateway-reader",
		ProducerBuildIdentity:   "producer-reader",
		DayDefinitionID:         "utc-day-v1",
		SettlePolicy:            "manual-v1",
		PublisherID:             "publisher-reader",
		PublisherEpoch:          1,
	}
	layout, err := r2.NewLayout("v1", "", scope)
	if err != nil {
		t.Fatal(err)
	}
	objects := make([]archive.RawObject, 0, 3)
	root := t.TempDir()
	outbox := t.TempDir()
	store, err := wal.Open(root, "gateway-reader")
	if err != nil {
		t.Fatal(err)
	}
	dayA := time.Date(2024, 3, 9, 0, 0, 0, 0, time.UTC).UnixMilli()
	dayB := time.Date(2024, 3, 10, 0, 0, 0, 0, time.UTC).UnixMilli()
	frames := []struct {
		timeMSC int64
		zero    bool
	}{
		{timeMSC: dayA + 100},
		{timeMSC: dayB, zero: true},
		{timeMSC: dayA + 200},
	}
	for index, item := range frames {
		frame := readerTestFrame(t, item.timeMSC, uint64(index+1), item.zero)
		if _, err := store.Append(frame, 1710000000+int64(index), uint64(100+index)); err != nil {
			_ = store.Close()
			t.Fatal(err)
		}
		sealed, err := store.Seal()
		if err != nil {
			_ = store.Close()
			t.Fatal(err)
		}
		object, err := archive.PromoteSealedSegment(outbox, sealed.Path)
		if err != nil {
			_ = store.Close()
			t.Fatal(err)
		}
		objects = append(objects, object)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	base := archive.RawDayManifestInput{
		Scope: scope, RawObjects: objects, TerminalSyncStatus: "complete",
		CompletenessStatus: "settled_snapshot", LogicalCloseTimeS: 1710100000,
	}
	manifestA, err := archive.BuildRawDayManifest(archive.RawDayManifestInput{Scope: scope, Date: "2024-03-09", RawObjects: objects, TerminalSyncStatus: base.TerminalSyncStatus, CompletenessStatus: base.CompletenessStatus, LogicalCloseTimeS: base.LogicalCloseTimeS})
	if err != nil {
		t.Fatal(err)
	}
	manifestB, err := archive.BuildRawDayManifest(archive.RawDayManifestInput{Scope: scope, Date: "2024-03-10", RawObjects: objects, TerminalSyncStatus: base.TerminalSyncStatus, CompletenessStatus: base.CompletenessStatus, LogicalCloseTimeS: base.LogicalCloseTimeS})
	if err != nil {
		t.Fatal(err)
	}
	backend := &memoryReadBackend{objects: make(map[string][]byte), openError: make(map[string]error), closeError: make(map[string]error)}
	scopeDescriptor, err := scope.CanonicalConfigJSON()
	if err != nil {
		t.Fatal(err)
	}
	scopeKey, err := layout.ScopeDescriptorKey()
	if err != nil {
		t.Fatal(err)
	}
	backend.objects[scopeKey] = scopeDescriptor
	for _, manifest := range []archive.RawDayManifest{manifestA, manifestB} {
		body, err := archive.ManifestCanonicalJSON(manifest)
		if err != nil {
			t.Fatal(err)
		}
		key, err := layout.ManifestKey(manifest)
		if err != nil {
			t.Fatal(err)
		}
		backend.objects[key] = body
	}
	for _, object := range objects {
		body, err := os.ReadFile(object.Path)
		if err != nil {
			t.Fatal(err)
		}
		key, err := layout.RemoteKey(object.Key)
		if err != nil {
			t.Fatal(err)
		}
		backend.objects[key] = body
	}
	config := testReaderConfig(t.TempDir())
	reader, err := NewArchiveReaderV1WithBackend(config, backend)
	if err != nil {
		t.Fatal(err)
	}
	return deliveryFixture{reader: reader, backend: backend, scope: scope, layout: layout, objects: objects, manifestA: manifestA, manifestB: manifestB}
}

func testReaderConfig(cacheRoot string) ReaderConfig {
	return ReaderConfig{
		Version: ReaderConfigVersion, Endpoint: "https://reader.invalid",
		BucketEnv: "TICK_READER_TEST_BUCKET", AccessKeyEnv: "TICK_READER_TEST_ACCESS",
		SecretKeyEnv: "TICK_READER_TEST_SECRET", Region: "auto", ImmutableRoot: "v1",
		CacheRoot: cacheRoot, MaxMetadataBytes: 1 << 20, MaxRawObjectBytes: 1 << 30,
	}
}

func TestArchiveReaderUsesBoundedRemoteListing(t *testing.T) {
	fixture := newDeliveryFixture(t)
	config := testReaderConfig(t.TempDir())
	config.MaxRemoteObjects = 1
	reader, err := NewArchiveReaderV1WithBackend(config, fixture.backend)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reader.ListDatasets(context.Background()); !errors.Is(err, r2.ErrResourceLimit) {
		t.Fatalf("unbounded inventory result = %v, want ErrResourceLimit", err)
	}
}

func readerTestFrame(t *testing.T, timeMSC int64, sequence uint64, zero bool) []byte {
	t.Helper()
	fixture, err := fake.BatchFixture()
	if err != nil {
		t.Fatal(err)
	}
	frame, err := protocol.DecodeFrame(fixture.Frame)
	if err != nil {
		t.Fatal(err)
	}
	message, err := protocol.DecodeMessage(frame)
	if err != nil {
		t.Fatal(err)
	}
	batch := message.(protocol.BatchFrameV1)
	batch.BatchSequence = sequence
	batch.RequestedFromMSC = timeMSC
	if zero {
		batch.Records = nil
		batch.ReturnedCount = -1
		batch.CopyTicksError = 7
	} else {
		batch.Records[0].TimeMSC = timeMSC
		batch.Records[0].CaptureSequence = 100 + sequence
	}
	encoded, err := protocol.EncodeMessage(batch)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func TestArchiveReaderDiscoveryFetchAndDayRestoration(t *testing.T) {
	fixture := newDeliveryFixture(t)
	ctx := context.Background()
	datasets, err := fixture.reader.ListDatasets(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(datasets) != 1 || datasets[0].DatasetID != fixture.scope.DatasetID {
		t.Fatalf("datasets = %+v", datasets)
	}
	campaigns, err := fixture.reader.ListCampaigns(ctx, fixture.scope.DatasetID)
	if err != nil {
		t.Fatal(err)
	}
	if len(campaigns) != 1 || campaigns[0].CampaignID != fixture.scope.CampaignID {
		t.Fatalf("campaigns = %+v", campaigns)
	}
	snapshots, err := fixture.reader.ListRawSnapshots(ctx, RawDayScope{DatasetID: fixture.scope.DatasetID, CampaignID: fixture.scope.CampaignID, Date: "2024-03-09"})
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshots) != 1 || snapshots[0].Revision != 1 {
		t.Fatalf("snapshots = %+v", snapshots)
	}
	manifestKey, err := fixture.layout.ManifestKey(fixture.manifestA)
	if err != nil {
		t.Fatal(err)
	}
	resolvedByKey, err := fixture.reader.ResolveSnapshot(ctx, SnapshotSelector{Manifest: manifestKey})
	if err != nil {
		t.Fatal(err)
	}
	resolvedByDigest, err := fixture.reader.ResolveSnapshot(ctx, SnapshotSelector{Manifest: hexDigest(fixture.manifestA.ManifestSHA256)})
	if err != nil {
		t.Fatal(err)
	}
	if resolvedByKey.ManifestKey != resolvedByDigest.ManifestKey {
		t.Fatal("key and digest selectors resolved different manifests")
	}
	plan, err := fixture.reader.BuildFetchPlan(ctx, resolvedByKey)
	if err != nil {
		t.Fatal(err)
	}
	result, err := fixture.reader.Fetch(ctx, plan, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Entries) != 3 {
		t.Fatalf("restored entries = %d, want 3 including cross-day zero batch", len(result.Entries))
	}
	if result.Entries[1].Batch.CopyTicksError == 0 || len(result.Entries[1].Batch.Records) != 0 {
		t.Fatalf("zero-record error batch was not restored: %+v", result.Entries[1].Batch)
	}
	report, err := fixture.reader.VerifyDay(ctx, SnapshotSelector{Manifest: hexDigest(fixture.manifestA.ManifestSHA256)})
	if err != nil {
		t.Fatal(err)
	}
	if report.GenesisVerified || report.VerificationScope != VerificationScopeAnchoredDay {
		t.Fatalf("day report overclaims verification: %+v", report)
	}
}

func TestArchiveReaderRejectsRemoteMutationAndCorruptCache(t *testing.T) {
	fixture := newDeliveryFixture(t)
	ctx := context.Background()
	key, err := fixture.layout.ManifestKey(fixture.manifestA)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := fixture.reader.ResolveSnapshot(ctx, SnapshotSelector{Manifest: key})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := fixture.reader.BuildFetchPlan(ctx, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.reader.Fetch(ctx, plan, ""); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(plan.Objects[0].CachePath, []byte("corrupt"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.reader.Fetch(ctx, plan, ""); !errors.Is(err, archive.ErrIntegrity) {
		t.Fatalf("corrupt cache error = %v, want ErrIntegrity", err)
	}
	fixture.backend.objects[plan.Objects[0].RemoteKey][0] ^= 0xff
	if _, err := fixture.reader.Fetch(ctx, plan, ""); !errors.Is(err, archive.ErrIntegrity) {
		t.Fatalf("remote raw mutation error = %v, want ErrIntegrity", err)
	}
	fixture.backend.objects[plan.Objects[0].RemoteKey][0] ^= 0xff
	fixture.backend.objects[plan.ManifestKey] = []byte("{}")
	if _, err := fixture.reader.Fetch(ctx, plan, ""); !errors.Is(err, archive.ErrIntegrity) {
		t.Fatalf("remote manifest mutation error = %v, want ErrIntegrity", err)
	}
}

func TestArchiveReaderStreamingFailureLeavesNoRawFinalOrTemporary(t *testing.T) {
	fixture := newDeliveryFixture(t)
	ctx := context.Background()
	key, err := fixture.layout.ManifestKey(fixture.manifestA)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := fixture.reader.ResolveSnapshot(ctx, SnapshotSelector{Manifest: key})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := fixture.reader.BuildFetchPlan(ctx, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	fixture.backend.openError[plan.Objects[0].RemoteKey] = errors.New("injected stream failure")
	if _, err := fixture.reader.Fetch(ctx, plan, ""); err == nil {
		t.Fatal("streaming failure was accepted")
	}
	if _, err := os.Stat(plan.Objects[0].CachePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("raw final after streaming failure: %v", err)
	}
	entries, err := os.ReadDir(filepath.Dir(plan.Objects[0].CachePath))
	if errors.Is(err, os.ErrNotExist) {
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".tmp") {
			t.Fatalf("temporary raw file survived streaming failure: %s", entry.Name())
		}
	}
}

func TestArchiveReaderDoesNotPersistCredentialValues(t *testing.T) {
	secret := "reader-secret-sentinel"
	t.Setenv("TICK_READER_TEST_SECRET", secret)
	fixture := newDeliveryFixture(t)
	ctx := context.Background()
	key, err := fixture.layout.ManifestKey(fixture.manifestA)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := fixture.reader.ResolveSnapshot(ctx, SnapshotSelector{Manifest: key})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := fixture.reader.BuildFetchPlan(ctx, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.reader.Fetch(ctx, plan, ""); err != nil {
		t.Fatal(err)
	}
	var foundSecret bool
	cacheRoot := filepath.Dir(filepath.Dir(plan.ManifestCachePath))
	if err := filepath.Walk(cacheRoot, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		body, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		foundSecret = foundSecret || bytes.Contains(body, []byte(secret))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if foundSecret {
		t.Fatal("credential value was persisted in reader cache")
	}
}

func TestArchiveReaderCampaignProvesGenesisToRootAndRejectsMissingRoot(t *testing.T) {
	fixture := newDeliveryFixture(t)
	ctx := context.Background()
	throughRoot := fixture.objects[2].Segment.ChainRoot
	report, err := fixture.reader.VerifyCampaign(ctx, fixture.scope.DatasetID, fixture.scope.CampaignID, hexDigest(throughRoot))
	if err != nil {
		t.Fatal(err)
	}
	if !report.GenesisVerified || report.VerificationScope != VerificationScopeCampaign || report.VerifiedThrough != 3 {
		t.Fatalf("campaign report = %+v", report)
	}
	if _, err := fixture.reader.VerifyCampaign(ctx, fixture.scope.DatasetID, fixture.scope.CampaignID, strings.Repeat("f", 64)); !errors.Is(err, archive.ErrIntegrity) {
		t.Fatalf("missing root error = %v, want ErrIntegrity", err)
	}
}

func TestArchiveReaderCampaignRejectsCanonicalButSemanticallyMutatedManifest(t *testing.T) {
	fixture := newDeliveryFixture(t)
	mutated := fixture.manifestA
	mutated.Objects = append([]archive.RawObjectRange(nil), mutated.Objects...)
	mutated.Objects[0].LastRecordOrdinal++
	var err error
	mutated.RawSetRoot, err = archive.RawSetRoot(mutated.Objects)
	if err != nil {
		t.Fatal(err)
	}
	mutated.ManifestSHA256 = [32]byte{}
	mutated.ManifestSHA256, err = archive.ManifestDigest(mutated)
	if err != nil {
		t.Fatal(err)
	}
	body, err := archive.ManifestCanonicalJSON(mutated)
	if err != nil {
		t.Fatal(err)
	}
	oldKey, err := fixture.layout.ManifestKey(fixture.manifestA)
	if err != nil {
		t.Fatal(err)
	}
	newKey, err := fixture.layout.ManifestKey(mutated)
	if err != nil {
		t.Fatal(err)
	}
	if oldKey == newKey {
		t.Fatal("semantic mutation did not produce a new manifest digest key")
	}
	delete(fixture.backend.objects, oldKey)
	fixture.backend.objects[newKey] = body
	throughRoot := fixture.objects[2].Segment.ChainRoot
	if _, err := fixture.reader.VerifyCampaign(context.Background(), fixture.scope.DatasetID, fixture.scope.CampaignID, hexDigest(throughRoot)); !errors.Is(err, archive.ErrIntegrity) {
		t.Fatalf("semantic manifest mutation error = %v, want ErrIntegrity", err)
	}
}

func TestArchiveReaderRejectsMalformedSelectorAndRevisionBranch(t *testing.T) {
	fixture := newDeliveryFixture(t)
	ctx := context.Background()
	if _, err := fixture.reader.ResolveSnapshot(ctx, SnapshotSelector{Manifest: "v1/../manifest.json"}); !errors.Is(err, ErrSelectorInvalid) {
		t.Fatalf("traversal selector error = %v, want ErrSelectorInvalid", err)
	}
	revisionTwo, err := archive.BuildRawDayManifest(archive.RawDayManifestInput{
		Scope: fixture.scope, Date: fixture.manifestA.Date, RawObjects: fixture.objects,
		Revision: 2, Previous: &fixture.manifestA, TerminalSyncStatus: "complete",
		CompletenessStatus: "settled_snapshot", LogicalCloseTimeS: 1710100001,
	})
	if err != nil {
		t.Fatal(err)
	}
	branch, err := archive.BuildRawDayManifest(archive.RawDayManifestInput{
		Scope: fixture.scope, Date: fixture.manifestA.Date, RawObjects: fixture.objects,
		Revision: 2, Previous: &fixture.manifestA, TerminalSyncStatus: "complete",
		CompletenessStatus: "settled_snapshot", LogicalCloseTimeS: 1710100002,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, manifest := range []archive.RawDayManifest{revisionTwo, branch} {
		body, err := archive.ManifestCanonicalJSON(manifest)
		if err != nil {
			t.Fatal(err)
		}
		key, err := fixture.layout.ManifestKey(manifest)
		if err != nil {
			t.Fatal(err)
		}
		fixture.backend.objects[key] = body
	}
	if _, err := fixture.reader.ListRawSnapshots(ctx, RawDayScope{DatasetID: fixture.scope.DatasetID, CampaignID: fixture.scope.CampaignID}); !errors.Is(err, archive.ErrIntegrity) {
		t.Fatalf("revision branch error = %v, want ErrIntegrity", err)
	}
}

func hexDigest(value [32]byte) string {
	const hexAlphabet = "0123456789abcdef"
	result := make([]byte, 64)
	for index, value := range value {
		result[index*2] = hexAlphabet[value>>4]
		result[index*2+1] = hexAlphabet[value&0xf]
	}
	return string(result)
}
