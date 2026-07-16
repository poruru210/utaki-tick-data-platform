package publication

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"tick-data-platform/internal/archive"
	"tick-data-platform/internal/protocol"
	"tick-data-platform/internal/wal"
)

func TestAffectedDatesIncludesRecordsAndZeroBatch(t *testing.T) {
	day := time.Date(2024, 3, 9, 0, 0, 0, 0, time.UTC)
	frame, err := protocol.EncodeMessage(protocol.BatchFrameV1{
		RequestedFromMSC: day.UnixMilli(),
		SourceSchemaID:   protocol.SourceSchemaMT5,
		ReturnedCount:    2,
		Records: []protocol.RawMqlTickV1{
			{TimeMSC: day.Add(time.Hour).UnixMilli()},
			{TimeMSC: day.Add(24 * time.Hour).UnixMilli()},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	zeroFrame, err := protocol.EncodeMessage(protocol.BatchFrameV1{
		RequestedFromMSC: day.Add(48 * time.Hour).UnixMilli(),
		SourceSchemaID:   protocol.SourceSchemaMT5,
	})
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	store, err := wal.Open(root, "gateway-publication-test")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := store.Append(frame, 1710000000, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(zeroFrame, 1710000001, 2); err != nil {
		t.Fatal(err)
	}
	segment, err := store.Seal()
	if err != nil {
		t.Fatal(err)
	}
	dates, err := AffectedDates(segment)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"2024-03-09", "2024-03-10", "2024-03-11"}
	if len(dates) != len(want) {
		t.Fatalf("affected dates = %v, want %v", dates, want)
	}
	for i := range want {
		if dates[i] != want[i] {
			t.Fatalf("affected dates = %v, want %v", dates, want)
		}
	}
}

func TestLocalPipelineSealsPromotesAndSpoolsIdempotently(t *testing.T) {
	root := t.TempDir()
	walRoot := filepath.Join(root, "wal")
	outboxRoot := filepath.Join(root, "outbox")
	catalogPath := filepath.Join(root, "catalog.sqlite")
	manifestRoot := filepath.Join(root, "manifests")
	store, err := wal.Open(walRoot, "gateway-publication-test")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	catalog, err := NewCatalogWithClock(catalogPath, func() time.Time {
		return time.Date(2024, 3, 9, 12, 0, 0, 0, time.UTC)
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := catalog.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer catalog.Stop(context.Background())
	pipeline, err := NewLocalPipeline(LocalPipelineConfig{
		WAL:           store,
		Catalog:       catalog,
		RawOutboxRoot: outboxRoot,
		ManifestRoot:  manifestRoot,
		Scope:         testScope(),
		SealMaxBytes:  1,
		SealInterval:  time.Hour,
		ScanInterval:  time.Hour,
		Clock: func() time.Time {
			return time.Date(2024, 3, 9, 12, 0, 0, 0, time.UTC)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	frame, err := protocol.EncodeMessage(protocol.BatchFrameV1{
		RequestedFromMSC: time.Date(2024, 3, 9, 0, 0, 0, 0, time.UTC).UnixMilli(),
		SourceSchemaID:   protocol.SourceSchemaMT5,
		ReturnedCount:    1,
		Records: []protocol.RawMqlTickV1{{
			TimeMSC:         time.Date(2024, 3, 9, 0, 0, 1, 0, time.UTC).UnixMilli(),
			CaptureSequence: 1,
			BidBits:         math.Float64bits(1.1),
			AskBits:         math.Float64bits(1.2),
		}},
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
	segments, err := catalog.ListSegments(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(segments) != 1 || segments[0].State != SegmentStatePromoted {
		t.Fatalf("catalog segments = %+v", segments)
	}
	if _, err := os.Stat(segments[0].RawPath); err != nil {
		t.Fatalf("raw object was not promoted: %v", err)
	}
	manifest, found, err := catalog.LatestManifest(context.Background(), "2024-03-09")
	if err != nil || !found {
		t.Fatalf("latest manifest found=%v err=%v", found, err)
	}
	if manifest.Revision != 1 || manifest.State != ManifestStateSpooled {
		t.Fatalf("manifest record = %+v", manifest)
	}
	data, err := os.ReadFile(manifest.Path)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := archive.VerifyRawDayManifest(data)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.CompletenessStatus != ManifestCompleteness || decoded.TerminalSyncStatus != ManifestTerminalSyncStatus {
		t.Fatalf("manifest status = %s/%s", decoded.CompletenessStatus, decoded.TerminalSyncStatus)
	}
	if decoded.LogicalCloseTimeS != 0 {
		t.Fatalf("automatic manifest logical close time = %d, want 0", decoded.LogicalCloseTimeS)
	}
	if err := pipeline.ProcessOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	again, found, err := catalog.LatestManifest(context.Background(), "2024-03-09")
	if err != nil || !found || again.Revision != 1 || again.SHA256 != manifest.SHA256 {
		t.Fatalf("duplicate process changed manifest: %+v found=%v err=%v", again, found, err)
	}
	if err := catalog.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(catalogPath); err != nil {
		t.Fatal(err)
	}
	rebuilt, err := NewCatalogWithClock(catalogPath, func() time.Time {
		return time.Date(2024, 3, 9, 12, 0, 0, 0, time.UTC)
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := rebuilt.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer rebuilt.Stop(context.Background())
	reconciler, err := NewPlanner(testScope(), manifestRoot, rebuilt, func() time.Time {
		return time.Date(2024, 3, 9, 12, 0, 0, 0, time.UTC)
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := reconciler.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	recovered, found, err := rebuilt.LatestManifest(context.Background(), "2024-03-09")
	if err != nil || !found || recovered.Revision != manifest.Revision || recovered.SHA256 != manifest.SHA256 {
		t.Fatalf("manifest catalog was not rebuilt: %+v found=%v err=%v", recovered, found, err)
	}
}

func TestLocalPipelineUsesInjectedTicker(t *testing.T) {
	root := t.TempDir()
	store, err := wal.Open(filepath.Join(root, "wal"), "gateway-ticker-test")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	catalog, err := NewCatalog(filepath.Join(root, "catalog.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	if err := catalog.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer catalog.Stop(context.Background())
	created := make(chan time.Duration, 1)
	manual := &manualTicker{ticks: make(chan time.Time), stopped: make(chan struct{})}
	pipeline, err := NewLocalPipeline(LocalPipelineConfig{
		WAL: store, Catalog: catalog, RawOutboxRoot: filepath.Join(root, "outbox"),
		ManifestRoot: filepath.Join(root, "manifests"), Scope: testScope(),
		SealMaxBytes: 1, SealInterval: time.Hour, ScanInterval: time.Minute,
		Clock: time.Now,
		TickerFactory: func(interval time.Duration) Ticker {
			created <- interval
			return manual
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := pipeline.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case interval := <-created:
		if interval != time.Minute {
			t.Fatalf("ticker interval = %s, want %s", interval, time.Minute)
		}
	case <-time.After(time.Second):
		t.Fatal("pipeline did not construct injected ticker")
	}
	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := pipeline.Stop(stopCtx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-manual.stopped:
	case <-time.After(time.Second):
		t.Fatal("pipeline did not stop injected ticker")
	}
}

func TestPlannerWaitsForRemotePredecessor(t *testing.T) {
	root := t.TempDir()
	store, err := wal.Open(filepath.Join(root, "wal"), "gateway-predecessor-gate-test")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	clock := func() time.Time { return time.Date(2024, 3, 9, 12, 0, 0, 0, time.UTC) }
	catalog, err := NewCatalogWithClock(filepath.Join(root, "catalog.sqlite"), clock)
	if err != nil {
		t.Fatal(err)
	}
	if err := catalog.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer catalog.Stop(context.Background())
	gate := &testManifestPublicationGate{}
	pipeline, err := NewLocalPipeline(LocalPipelineConfig{
		WAL: store, Catalog: catalog, RawOutboxRoot: filepath.Join(root, "outbox"),
		ManifestRoot: filepath.Join(root, "manifests"), Scope: testScope(),
		SealMaxBytes: 1, SealInterval: time.Hour, ScanInterval: time.Hour,
		Clock: clock, ManifestGate: gate,
	})
	if err != nil {
		t.Fatal(err)
	}
	appendPublicationBatch(t, store, time.Date(2024, 3, 9, 0, 0, 1, 0, time.UTC), 1, 1)
	if err := pipeline.ProcessOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	first, found, err := catalog.LatestManifest(context.Background(), "2024-03-09")
	if err != nil || !found || first.Revision != 1 {
		t.Fatalf("first manifest = %+v found=%v err=%v", first, found, err)
	}

	appendPublicationBatch(t, store, time.Date(2024, 3, 9, 0, 0, 2, 0, time.UTC), 2, 2)
	if err := pipeline.ProcessOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	blocked, found, err := catalog.LatestManifest(context.Background(), "2024-03-09")
	if err != nil || !found || blocked.Revision != 1 {
		t.Fatalf("unpublished predecessor allowed successor: %+v found=%v err=%v", blocked, found, err)
	}

	gate.published = true
	if err := pipeline.ProcessOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	second, found, err := catalog.LatestManifest(context.Background(), "2024-03-09")
	if err != nil || !found || second.Revision != 2 {
		t.Fatalf("published predecessor did not allow successor: %+v found=%v err=%v", second, found, err)
	}
	if gate.calls < 2 {
		t.Fatalf("publication gate calls = %d, want at least 2", gate.calls)
	}
}

type manualTicker struct {
	ticks   chan time.Time
	stopped chan struct{}
	once    sync.Once
}

func (t *manualTicker) C() <-chan time.Time { return t.ticks }
func (t *manualTicker) Stop()               { t.once.Do(func() { close(t.stopped) }) }

type testManifestPublicationGate struct {
	published bool
	calls     int
}

func (g *testManifestPublicationGate) IsPublished(context.Context, archive.RawDayManifest) (bool, error) {
	g.calls++
	return g.published, nil
}

func appendPublicationBatch(t *testing.T, store *wal.Store, timestamp time.Time, sequence, captureSequence uint64) {
	t.Helper()
	frame, err := protocol.EncodeMessage(protocol.BatchFrameV1{
		RequestedFromMSC: timestamp.Add(-time.Second).UnixMilli(),
		SourceSchemaID:   protocol.SourceSchemaMT5,
		ReturnedCount:    1,
		Records: []protocol.RawMqlTickV1{{
			TimeMSC: timestamp.UnixMilli(), CaptureSequence: captureSequence,
			BidBits: math.Float64bits(1.1), AskBits: math.Float64bits(1.2),
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(frame, int64(1710000000+sequence), sequence); err != nil {
		t.Fatal(err)
	}
}

func testScope() archive.ScopeConfig {
	return archive.ScopeConfig{
		DatasetID:               "dataset-test",
		CampaignID:              "campaign-test",
		ProviderID:              "provider-test",
		StableFeedID:            "feed-test",
		ExactSourceSymbol:       "EURUSD",
		BrokerServerFingerprint: "broker-test",
		GatewayBuildIdentity:    "gateway-build-test",
		ProducerBuildIdentity:   "producer-build-test",
		DayDefinitionID:         "utc-day-v1",
		SettlePolicy:            "manual-v1",
		PublisherID:             "gateway-test",
		PublisherEpoch:          1,
	}
}
