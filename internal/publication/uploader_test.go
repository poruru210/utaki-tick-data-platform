package publication

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"tick-data-platform/internal/archive"
	"tick-data-platform/internal/protocol"
	"tick-data-platform/internal/r2"
	"tick-data-platform/internal/testsupport"
)

func TestUploaderPublishesDueManifestAndRecordsLocalCompletion(t *testing.T) {
	root, catalog, record, cleanup := spoolUploaderFixture(t)
	defer cleanup()
	fake := &fakeRemotePublisher{}
	uploader, err := NewUploader(UploaderConfig{
		Catalog: catalog, Publisher: fake, ReceiptRoot: filepath.Join(root, "receipts"),
		ScanInterval: time.Hour, RetryMin: time.Second, RetryMax: time.Minute, Clock: time.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := uploader.PublishDue(context.Background()); err != nil {
		t.Fatal(err)
	}
	updated, found, err := catalog.LatestManifest(context.Background(), record.Date)
	if err != nil || !found {
		t.Fatalf("latest manifest found=%v err=%v", found, err)
	}
	if updated.State != ManifestStatePublished || len(fake.inputs) != 1 {
		t.Fatalf("manifest state=%q calls=%d", updated.State, len(fake.inputs))
	}
	if fake.inputs[0].ManifestPath != record.Path || len(fake.inputs[0].ObjectPaths) != 1 {
		t.Fatalf("publisher input = %+v", fake.inputs[0])
	}
}

func TestUploaderResumesUnfinishedRemoteIntent(t *testing.T) {
	root, catalog, record, cleanup := spoolUploaderFixture(t)
	defer cleanup()
	fake := &fakeRemotePublisher{}
	uploader, err := NewUploader(UploaderConfig{
		Catalog: catalog, Publisher: fake, RemoteIntents: &fakeRemoteIntentSource{}, ReceiptRoot: filepath.Join(root, "receipts"),
		ScanInterval: time.Hour, RetryMin: time.Second, RetryMax: time.Minute, Clock: time.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	input, err := uploader.inputFor(context.Background(), record)
	if err != nil {
		t.Fatal(err)
	}
	source := uploader.remoteIntents.(*fakeRemoteIntentSource)
	source.pending = []r2.UnfinishedPublication{{Identity: "remote-intent", Stage: r2.StageIntent, Input: input}}
	if err := uploader.PublishDue(context.Background()); err != nil {
		t.Fatal(err)
	}
	updated, found, err := catalog.LatestManifest(context.Background(), record.Date)
	if err != nil || !found || updated.State != ManifestStatePublished || len(fake.inputs) != 1 {
		t.Fatalf("recovered publication = %+v found=%v calls=%d err=%v", updated, found, len(fake.inputs), err)
	}
}

func TestUploaderResumesDurableJournalIntentAfterRestart(t *testing.T) {
	root, catalog, record, cleanup := spoolUploaderFixture(t)
	defer cleanup()

	clock := func() time.Time { return time.Date(2024, 3, 9, 12, 0, 0, 0, time.UTC) }
	inputBuilder, err := NewUploader(UploaderConfig{
		Catalog: catalog, Publisher: &fakeRemotePublisher{}, ReceiptRoot: filepath.Join(root, "receipts"),
		ScanInterval: time.Hour, RetryMin: time.Second, RetryMax: time.Minute, Clock: clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	input, err := inputBuilder.inputFor(context.Background(), record)
	if err != nil {
		t.Fatal(err)
	}
	layout, err := r2.NewLayout("v1", testScope())
	if err != nil {
		t.Fatal(err)
	}
	journalPath := filepath.Join(root, "remote-journal.sqlite")
	journal, err := testsupport.NewStartedPublicationJournal(journalPath)
	if err != nil {
		t.Fatal(err)
	}
	backend := newUploaderRestartBackend()
	backend.failClaimOnce = true
	firstPublisher, err := r2.NewPublisher(layout, backend, journal, filepath.Join(root, "publication.lock"), time.Now)
	if err != nil {
		_ = journal.Stop(context.Background())
		t.Fatal(err)
	}
	if _, err := firstPublisher.Publish(context.Background(), input); err == nil {
		_ = journal.Stop(context.Background())
		t.Fatal("first publication unexpectedly completed")
	}
	unfinished, err := journal.ListUnfinished(context.Background())
	if err != nil || len(unfinished) != 1 || unfinished[0].Stage != r2.StageIntent {
		_ = journal.Stop(context.Background())
		t.Fatalf("durable unfinished intent = %+v err=%v", unfinished, err)
	}
	if err := journal.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}

	journal, err = testsupport.NewStartedPublicationJournal(journalPath)
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Stop(context.Background())
	secondPublisher, err := r2.NewPublisher(layout, backend, journal, filepath.Join(root, "publication.lock"), time.Now)
	if err != nil {
		t.Fatal(err)
	}
	uploader, err := NewUploader(UploaderConfig{
		Catalog: catalog, Publisher: secondPublisher, RemoteIntents: journal,
		ReceiptRoot: filepath.Join(root, "receipts"), ScanInterval: time.Hour,
		RetryMin: time.Second, RetryMax: time.Minute, Clock: clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := uploader.PublishDue(context.Background()); err != nil {
		t.Fatal(err)
	}
	updated, found, err := catalog.LatestManifest(context.Background(), record.Date)
	if err != nil || !found || updated.State != ManifestStatePublished {
		t.Fatalf("local completion after restart = %+v found=%v err=%v", updated, found, err)
	}
	manifestBytes, err := os.ReadFile(record.Path)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := archive.VerifyRawDayManifest(manifestBytes)
	if err != nil {
		t.Fatal(err)
	}
	layout, err = r2.NewLayout("v1", testScope())
	if err != nil {
		t.Fatal(err)
	}
	manifestKey, err := layout.ManifestKey(manifest)
	if err != nil {
		t.Fatal(err)
	}
	remoteRecord, found, err := journal.Record(manifestKey)
	if err != nil || !found || remoteRecord.Stage != r2.StageReceiptSaved {
		t.Fatalf("remote completion after restart = %+v found=%v err=%v", remoteRecord, found, err)
	}
	states, err := journal.ObjectStateRecords(manifestKey)
	if err != nil {
		t.Fatal(err)
	}
	verified := 0
	for _, state := range states {
		if state.State == r2.ObjectStateRemoteVerified {
			verified++
		}
	}
	if verified != len(manifest.ChainObjects) {
		t.Fatalf("remote_verified object states = %d, want %d: %+v", verified, len(manifest.ChainObjects), states)
	}
}

func TestUploaderPersistsTransientBackoffAndRetriesWhenDue(t *testing.T) {
	root, catalog, record, cleanup := spoolUploaderFixture(t)
	defer cleanup()
	now := time.Date(2024, 3, 9, 12, 0, 0, 0, time.UTC)
	fake := &fakeRemotePublisher{err: context.DeadlineExceeded}
	uploader, err := NewUploader(UploaderConfig{
		Catalog: catalog, Publisher: fake, ReceiptRoot: filepath.Join(root, "receipts"),
		ScanInterval: time.Hour, RetryMin: time.Minute, RetryMax: 5 * time.Minute, Clock: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := uploader.PublishDue(context.Background()); err != nil {
		t.Fatal(err)
	}
	retry, found, err := catalog.LatestManifest(context.Background(), record.Date)
	if err != nil || !found {
		t.Fatalf("retry manifest found=%v err=%v", found, err)
	}
	if retry.State != ManifestStateRetryWait || retry.Attempts != 1 || !retry.NextRetryAt.Equal(now.Add(time.Minute)) {
		t.Fatalf("retry record = %+v", retry)
	}
	if uploader.LastErrorClass() != "remote_timeout" {
		t.Fatalf("transient publication error class = %q", uploader.LastErrorClass())
	}
	fake.err = nil
	if err := uploader.PublishDue(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(fake.inputs) != 1 {
		t.Fatal("manifest was retried before backoff elapsed")
	}
	now = now.Add(time.Minute)
	if err := uploader.PublishDue(context.Background()); err != nil {
		t.Fatal(err)
	}
	updated, found, err := catalog.LatestManifest(context.Background(), record.Date)
	if err != nil || !found || updated.State != ManifestStatePublished || len(fake.inputs) != 2 {
		t.Fatalf("retry did not complete: record=%+v found=%v calls=%d err=%v", updated, found, len(fake.inputs), err)
	}
	if uploader.LastErrorClass() != "" {
		t.Fatalf("recovered publication retained error class %q", uploader.LastErrorClass())
	}
}

func TestUploaderPersistsBackoffAcrossRestart(t *testing.T) {
	root, catalog, record, cleanup := spoolUploaderFixture(t)
	defer cleanup()
	catalogPath := catalog.Path()
	now := time.Date(2024, 3, 9, 12, 0, 0, 0, time.UTC)
	firstPublisher := &fakeRemotePublisher{err: context.DeadlineExceeded}
	first, err := NewUploader(UploaderConfig{
		Catalog: catalog, Publisher: firstPublisher, ReceiptRoot: filepath.Join(root, "receipts"),
		ScanInterval: time.Hour, RetryMin: time.Minute, RetryMax: 5 * time.Minute,
		Clock: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := first.PublishDue(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := catalog.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	restartedCatalog, err := NewCatalog(catalogPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := restartedCatalog.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer restartedCatalog.Stop(context.Background())
	catalog = restartedCatalog

	secondPublisher := &fakeRemotePublisher{}
	second, err := NewUploader(UploaderConfig{
		Catalog: catalog, Publisher: secondPublisher, ReceiptRoot: filepath.Join(root, "receipts"),
		ScanInterval: time.Hour, RetryMin: time.Minute, RetryMax: 5 * time.Minute,
		Clock: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := second.PublishDue(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(secondPublisher.inputs) != 0 {
		t.Fatal("restarted uploader ignored durable retry backoff")
	}
	now = now.Add(time.Minute)
	if err := second.PublishDue(context.Background()); err != nil {
		t.Fatal(err)
	}
	updated, found, err := catalog.LatestManifest(context.Background(), record.Date)
	if err != nil || !found || updated.State != ManifestStatePublished || len(secondPublisher.inputs) != 1 {
		t.Fatalf("restarted uploader did not retry due manifest: record=%+v found=%v calls=%d err=%v", updated, found, len(secondPublisher.inputs), err)
	}
}

func TestUploaderPriorityWakeupDrainsDueRetry(t *testing.T) {
	root, catalog, record, cleanup := spoolUploaderFixture(t)
	defer cleanup()
	remote := &priorityRemotePublisher{called: make(chan struct{}, 4)}
	priority := &priorityWakeReader{wakeups: make(chan struct{}, 1)}
	uploader, err := NewUploader(UploaderConfig{
		Catalog: catalog, Publisher: remote, ReceiptRoot: filepath.Join(root, "receipts"),
		ScanInterval: time.Hour, RetryMin: time.Nanosecond, RetryMax: time.Second,
		Clock: time.Now, Priority: priority,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := uploader.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer uploader.Stop(context.Background())
	select {
	case <-remote.called:
	case <-time.After(2 * time.Second):
		t.Fatal("initial retry attempt did not run")
	}
	for deadline := time.After(2 * time.Second); ; {
		current, found, readErr := catalog.LatestManifest(context.Background(), record.Date)
		if readErr != nil {
			t.Fatal(readErr)
		}
		if found && current.State == ManifestStateRetryWait && !current.NextRetryAt.After(time.Now()) {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("durable retry did not become due: %+v found=%v", current, found)
		case <-time.After(time.Millisecond):
		}
	}
	if got := remote.callCount(); got != 1 {
		t.Fatalf("uploader retried before priority wakeup: calls=%d", got)
	}
	priority.priority = true
	priority.wake()
	select {
	case <-remote.called:
	case <-time.After(2 * time.Second):
		t.Fatal("priority wakeup did not drain due retry")
	}
	updated, found, err := catalog.LatestManifest(context.Background(), record.Date)
	if err != nil || !found || updated.State != ManifestStatePublished || remote.callCount() != 2 {
		t.Fatalf("priority retry completion = %+v found=%v calls=%d err=%v", updated, found, remote.callCount(), err)
	}
}

func TestUploaderFailsClosedOnImmutableCollision(t *testing.T) {
	root, catalog, record, cleanup := spoolUploaderFixture(t)
	defer cleanup()
	fake := &fakeRemotePublisher{err: r2.ErrImmutableCollision}
	uploader, err := NewUploader(UploaderConfig{
		Catalog: catalog, Publisher: fake, ReceiptRoot: filepath.Join(root, "receipts"),
		ScanInterval: time.Hour, RetryMin: time.Second, RetryMax: time.Minute, Clock: time.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := uploader.PublishDue(context.Background()); !errors.Is(err, r2.ErrImmutableCollision) {
		t.Fatalf("collision error = %v", err)
	}
	unchanged, found, err := catalog.LatestManifest(context.Background(), record.Date)
	if err != nil || !found || unchanged.State != ManifestStateSpooled {
		t.Fatalf("collision changed manifest state: %+v found=%v err=%v", unchanged, found, err)
	}
}

type fakeRemotePublisher struct {
	err    error
	inputs []r2.PublicationInput
}

type priorityRemotePublisher struct {
	mu     sync.Mutex
	inputs []r2.PublicationInput
	called chan struct{}
}

func (p *priorityRemotePublisher) Publish(_ context.Context, input r2.PublicationInput) (r2.VerificationReceipt, error) {
	p.mu.Lock()
	p.inputs = append(p.inputs, input)
	call := len(p.inputs)
	p.mu.Unlock()
	select {
	case p.called <- struct{}{}:
	default:
	}
	if call == 1 {
		return r2.VerificationReceipt{}, context.DeadlineExceeded
	}
	return r2.VerificationReceipt{VerificationComplete: true}, nil
}

func (p *priorityRemotePublisher) callCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.inputs)
}

type fakeRemoteIntentSource struct {
	pending []r2.UnfinishedPublication
}

type uploaderRestartBackend struct {
	objects       map[string][]byte
	failClaimOnce bool
}

func newUploaderRestartBackend() *uploaderRestartBackend {
	return &uploaderRestartBackend{objects: make(map[string][]byte)}
}

func (b *uploaderRestartBackend) PutIfAbsent(_ context.Context, key string, body []byte) error {
	if b.failClaimOnce {
		b.failClaimOnce = false
		return errors.New("simulated claim write failure")
	}
	if _, found := b.objects[key]; found {
		return r2.ErrObjectExists
	}
	b.objects[key] = append([]byte(nil), body...)
	return nil
}

func (b *uploaderRestartBackend) Get(_ context.Context, key string) ([]byte, error) {
	body, found := b.objects[key]
	if !found {
		return nil, r2.ErrObjectNotFound
	}
	return append([]byte(nil), body...), nil
}

func (b *uploaderRestartBackend) Open(ctx context.Context, key string) (io.ReadCloser, int64, error) {
	body, err := b.Get(ctx, key)
	if err != nil {
		return nil, 0, err
	}
	return io.NopCloser(bytes.NewReader(body)), int64(len(body)), nil
}

func (b *uploaderRestartBackend) List(_ context.Context, prefix string) ([]r2.RemoteObject, error) {
	keys := make([]string, 0, len(b.objects))
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

func (b *uploaderRestartBackend) PutFileIfAbsent(_ context.Context, key, path string, expectedSHA256 [32]byte, expectedBytes uint64) (r2.RemoteObjectCommit, error) {
	body, err := os.ReadFile(path)
	if err != nil {
		return r2.RemoteObjectCommit{}, err
	}
	if uint64(len(body)) != expectedBytes || sha256.Sum256(body) != expectedSHA256 {
		return r2.RemoteObjectCommit{}, r2.ErrLocalObjectChanged
	}
	if existing, found := b.objects[key]; found {
		if !bytes.Equal(existing, body) {
			return r2.RemoteObjectCommit{}, r2.ErrImmutableCollision
		}
		return r2.RemoteObjectCommit{ETag: "restart-etag"}, nil
	}
	b.objects[key] = append([]byte(nil), body...)
	return r2.RemoteObjectCommit{ETag: "restart-etag"}, nil
}

func (b *uploaderRestartBackend) VerifyFile(_ context.Context, key, path string, expectedSHA256 [32]byte, expectedBytes uint64) (r2.RemoteObjectVerification, error) {
	body, err := os.ReadFile(path)
	if err != nil {
		return r2.RemoteObjectVerification{}, err
	}
	if uint64(len(body)) != expectedBytes || sha256.Sum256(body) != expectedSHA256 {
		return r2.RemoteObjectVerification{}, r2.ErrLocalObjectChanged
	}
	remote, found := b.objects[key]
	if !found {
		return r2.RemoteObjectVerification{}, r2.ErrObjectNotFound
	}
	if !bytes.Equal(remote, body) {
		return r2.RemoteObjectVerification{}, r2.ErrRemoteCheckMismatch
	}
	return r2.RemoteObjectVerification{ETag: "restart-etag"}, nil
}

var _ r2.WriteBackend = (*uploaderRestartBackend)(nil)

func (s *fakeRemoteIntentSource) ListUnfinished(context.Context) ([]r2.UnfinishedPublication, error) {
	return append([]r2.UnfinishedPublication(nil), s.pending...), nil
}

func (p *fakeRemotePublisher) Publish(_ context.Context, input r2.PublicationInput) (r2.VerificationReceipt, error) {
	p.inputs = append(p.inputs, input)
	if p.err != nil {
		return r2.VerificationReceipt{}, p.err
	}
	return r2.VerificationReceipt{VerificationComplete: true}, nil
}

func spoolUploaderFixture(t *testing.T) (string, *Catalog, ManifestRecord, func()) {
	t.Helper()
	root := t.TempDir()
	store, err := testsupport.NewStartedWAL(filepath.Join(root, "wal"), "gateway-uploader-test")
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := NewCatalog(filepath.Join(root, "catalog.sqlite"))
	if err != nil {
		_ = store.Stop(context.Background())
		t.Fatal(err)
	}
	if err := catalog.Start(context.Background()); err != nil {
		_ = store.Stop(context.Background())
		t.Fatal(err)
	}
	clock := func() time.Time { return time.Date(2024, 3, 9, 12, 0, 0, 0, time.UTC) }
	pipeline, err := NewLocalPipeline(LocalPipelineConfig{
		WAL: store, Catalog: catalog, RawOutboxRoot: filepath.Join(root, "outbox"),
		ManifestRoot: filepath.Join(root, "manifests"), Scope: testScope(), SealMaxBytes: 1,
		SealInterval: time.Hour, ScanInterval: time.Hour, Clock: clock,
	})
	if err != nil {
		_ = catalog.Stop(context.Background())
		_ = store.Stop(context.Background())
		t.Fatal(err)
	}
	frame, err := protocol.EncodeMessage(protocol.BatchFrameV1{
		RequestedFromMSC: time.Date(2024, 3, 9, 0, 0, 0, 0, time.UTC).UnixMilli(),
		SourceSchemaID:   protocol.SourceSchemaMT5, ReturnedCount: 1,
		Records: []protocol.RawMqlTickV1{{TimeMSC: time.Date(2024, 3, 9, 0, 0, 1, 0, time.UTC).UnixMilli(), CaptureSequence: 1, BidBits: math.Float64bits(1.1), AskBits: math.Float64bits(1.2)}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(frame, 1710000000, 1); err != nil {
		t.Fatal(err)
	}
	if err := pipeline.ProcessOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	record, found, err := catalog.LatestManifest(context.Background(), "2024-03-09")
	if err != nil || !found {
		t.Fatalf("fixture manifest found=%v err=%v", found, err)
	}
	return root, catalog, record, func() {
		_ = catalog.Stop(context.Background())
		_ = store.Stop(context.Background())
	}
}
