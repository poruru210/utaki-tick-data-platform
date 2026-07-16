package publication

import (
	"context"
	"errors"
	"math"
	"path/filepath"
	"testing"
	"time"

	"tick-data-platform/internal/protocol"
	"tick-data-platform/internal/r2"
	"tick-data-platform/internal/wal"
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

type fakeRemoteIntentSource struct {
	pending []r2.UnfinishedPublication
}

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
	store, err := wal.Open(filepath.Join(root, "wal"), "gateway-uploader-test")
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := NewCatalog(filepath.Join(root, "catalog.sqlite"))
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	if err := catalog.Start(context.Background()); err != nil {
		_ = store.Close()
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
		_ = store.Close()
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
		_ = store.Close()
	}
}
