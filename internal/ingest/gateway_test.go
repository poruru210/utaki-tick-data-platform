package ingest_test

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	_ "modernc.org/sqlite"
	"tick-data-platform/internal/app"
	"tick-data-platform/internal/archive"
	"tick-data-platform/internal/ingest"
	"tick-data-platform/internal/protocol"
	"tick-data-platform/internal/publication"
	"tick-data-platform/producers/fake"
)

func TestGatewayAcceptsDurableBatchAndIdempotentRetry(t *testing.T) {
	config := testConfig(t)
	gateway, client, stop := startGateway(t, config)
	defer stop()

	batch := testBatch(1, 1000, 1, 0)
	ack, err := client.SendBatch(batch)
	if err != nil {
		t.Fatal(err)
	}
	if ack.Status != protocol.AckAcceptedAdvanced || ack.CommittedCursorMSC != 1000 {
		t.Fatalf("unexpected accepted ack: %+v", ack)
	}
	if gateway.WAL().Count() != 1 {
		t.Fatalf("expected one WAL entry, got %d", gateway.WAL().Count())
	}

	duplicate, err := client.SendBatch(batch)
	if err != nil {
		t.Fatal(err)
	}
	if duplicate.Status != protocol.AckDuplicate || duplicate.GatewayIngestSequence != ack.GatewayIngestSequence {
		t.Fatalf("unexpected duplicate ack: %+v", duplicate)
	}

	conflictBatch := batch
	conflictBatch.Records = append([]protocol.RawMqlTickV1(nil), batch.Records...)
	conflictBatch.Records[0].BidBits++
	conflict, err := client.SendBatch(conflictBatch)
	if err != nil {
		t.Fatal(err)
	}
	if conflict.Status != protocol.AckSourceStateConflict {
		t.Fatalf("unexpected conflict ack: %+v", conflict)
	}
	if gateway.WAL().Count() != 1 {
		t.Fatalf("conflict must not append WAL entry, got %d", gateway.WAL().Count())
	}
}

func TestGatewayStopsNewACKWhenPublicationBacklogReachesLimit(t *testing.T) {
	config := testConfig(t)
	config.MaxPendingSegments = 1
	config.MaxPendingBytes = 1
	gateway, client, stop := startGateway(t, config)
	defer stop()

	gateway.SetPendingPublication(1, 1)
	status, err := gateway.Status()
	if err != nil {
		t.Fatal(err)
	}
	if status.ReadyForACK || status.BlockedReason != "publication_backlog_limit" {
		t.Fatalf("gateway status under publication pressure = %+v", status)
	}
	ack, err := client.SendBatch(testBatch(1, 1000, 1, 0))
	if err != nil {
		t.Fatal(err)
	}
	if ack.Status != protocol.AckRetryableError || gateway.WAL().Count() != 0 {
		t.Fatalf("new batch was accepted under backlog pressure: ack=%+v wal=%d", ack, gateway.WAL().Count())
	}
}

func TestCatalogBacklogReachesGatewayThroughLocalPipeline(t *testing.T) {
	config := testConfig(t)
	config.MaxPendingSegments = 1
	config.MaxPendingBytes = 1
	gateway, client, stop := startGateway(t, config)
	defer stop()

	catalog, err := publication.NewCatalog(filepath.Join(filepath.Dir(config.WALRoot), "publication", "catalog.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	if err := catalog.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer catalog.Stop(context.Background())
	manifestSHA := sha256.Sum256([]byte("pending-manifest"))
	if err := catalog.UpsertManifest(context.Background(), publication.ManifestRecord{
		Date: "2024-03-09", Revision: 1, Path: filepath.Join(filepath.Dir(config.WALRoot), "manifest.json"),
		SHA256: manifestSHA, Bytes: 1, UpdatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	pipeline, err := publication.NewLocalPipeline(publication.LocalPipelineConfig{
		WAL: gateway.WAL(), Catalog: catalog,
		RawOutboxRoot: filepath.Join(filepath.Dir(config.WALRoot), "raw-outbox"),
		ManifestRoot:  filepath.Join(filepath.Dir(config.WALRoot), "manifests"),
		Scope: archive.ScopeConfig{
			DatasetID: "dataset-test", CampaignID: "campaign-test", ProviderID: "provider-test", StableFeedID: "feed-test",
			ExactSourceSymbol: "EURUSD", BrokerServerFingerprint: "broker-test", GatewayBuildIdentity: "gateway-test",
			ProducerBuildIdentity: "producer-test", DayDefinitionID: "utc-day-v1", SettlePolicy: "manual-v1",
			PublisherID: "publisher-test", PublisherEpoch: 1,
			ProtocolLimits: archive.ProtocolLimits{MaxFrameBytes: protocol.MaxFrameBytes, MaxRecords: protocol.MaxRecords},
		},
		SealMaxBytes: 1 << 30, SealInterval: time.Hour, ScanInterval: time.Hour, Clock: time.Now,
		PendingSink: gatewayPendingSink{gateway: gateway},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := pipeline.ProcessOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	status, err := gateway.Status()
	if err != nil {
		t.Fatal(err)
	}
	if status.ReadyForACK || status.BlockedReason != "publication_backlog_limit" {
		t.Fatalf("Catalog backlog did not reach Gateway policy: %+v", status)
	}
	ack, err := client.SendBatch(testBatch(1, 1000, 1, 0))
	if err != nil {
		t.Fatal(err)
	}
	if ack.Status != protocol.AckRetryableError || gateway.WAL().Count() != 0 {
		t.Fatalf("Gateway accepted batch despite Catalog backlog: ack=%+v wal=%d", ack, gateway.WAL().Count())
	}
}

type gatewayPendingSink struct{ gateway *ingest.Gateway }

func (s gatewayPendingSink) SetPendingPublication(segments, bytes uint64) {
	s.gateway.SetPendingPublication(segments, bytes)
}

func TestGatewayRejectsExpiredConnectionAfterSessionReplacement(t *testing.T) {
	config := testConfig(t)
	config.SessionLeaseTimeout = 50 * time.Millisecond
	config.HeartbeatIdleTimeout = time.Second
	gateway, first, stop := startGateway(t, config)
	defer stop()

	time.Sleep(150 * time.Millisecond)
	second, err := fake.Dial(context.Background(), first.Address, first.Hello)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()

	oldAck, err := first.SendBatch(testBatch(1, 1000, 1, 0))
	if err != nil {
		t.Fatal(err)
	}
	if oldAck.Status != protocol.AckSessionLeaseConflict {
		t.Fatalf("expired connection status = %d, want session conflict", oldAck.Status)
	}
	newAck, err := second.SendBatch(testBatch(1, 1000, 1, 0))
	if err != nil {
		t.Fatal(err)
	}
	if newAck.Status != protocol.AckAcceptedAdvanced || gateway.WAL().Count() != 1 {
		t.Fatalf("replacement connection was not accepted: ack=%+v wal=%d", newAck, gateway.WAL().Count())
	}

	_ = first.Close()
	nextAck, err := second.SendBatch(testBatch(2, 2000, 1, 0))
	if err != nil {
		t.Fatal(err)
	}
	if nextAck.Status != protocol.AckAcceptedAdvanced {
		t.Fatalf("old connection release invalidated replacement: %+v", nextAck)
	}
}

func TestGatewayRebuildsJournalAfterDeletionAndResumes(t *testing.T) {
	config := testConfig(t)
	gateway, client, stop := startGateway(t, config)
	batch := testBatch(1, 1000, 1, 0)
	if _, err := client.SendBatch(batch); err != nil {
		t.Fatal(err)
	}
	resumeBefore := client.Resume
	acceptedCursor := int64(1000)
	_ = client.Close()
	stop()

	if err := os.Remove(config.JournalPath); err != nil {
		t.Fatal(err)
	}
	gateway, client, stop = startGateway(t, config)
	defer stop()
	if resumeBefore.CommittedCursorMSC == acceptedCursor || client.Resume.CommittedCursorMSC != acceptedCursor {
		t.Fatalf("unexpected resume cursor after journal rebuild: before=%d after=%d", resumeBefore.CommittedCursorMSC, client.Resume.CommittedCursorMSC)
	}
	if gateway.WAL().Count() != 1 || gateway.Journal() == nil {
		t.Fatalf("WAL/journal was not restored")
	}
	duplicate, err := client.SendBatch(batch)
	if err != nil {
		t.Fatal(err)
	}
	if duplicate.Status != protocol.AckDuplicate {
		t.Fatalf("expected duplicate after resume, got %+v", duplicate)
	}
}

func TestGatewayRebuildsJournalWhenCursorStateDiffers(t *testing.T) {
	config := testConfig(t)
	gateway, client, stop := startGateway(t, config)
	if _, err := client.SendBatch(testBatch(1, 1000, 1, 0)); err != nil {
		t.Fatal(err)
	}
	_ = client.Close()
	stop()

	db, err := sql.Open("sqlite", config.JournalPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE gateway_state SET committed_cursor_msc=9999, next_from_msc=9999`); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	gateway, client, stop = startGateway(t, config)
	defer stop()
	if client.Resume.CommittedCursorMSC != 1000 || client.Resume.NextFromMSC != 1000 {
		t.Fatalf("journal state was not rebuilt: cursor=%d next=%d", client.Resume.CommittedCursorMSC, client.Resume.NextFromMSC)
	}
	if gateway.WAL().Count() != 1 {
		t.Fatalf("unexpected WAL entry count after journal rebuild: %d", gateway.WAL().Count())
	}
}

func TestGatewayRebuildsJournalAcrossSealedAndActiveWAL(t *testing.T) {
	config := testConfig(t)
	gateway, client, stop := startGateway(t, config)
	firstBatch := testBatch(1, 1000, 1, 0)
	firstAck, err := client.SendBatch(firstBatch)
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := gateway.WAL().Seal()
	if err != nil {
		t.Fatal(err)
	}
	secondBatch := testBatch(2, 2000, 1, 0)
	secondAck, err := client.SendBatch(secondBatch)
	if err != nil {
		t.Fatal(err)
	}
	if firstAck.GatewayIngestSequence != 1 ||
		secondAck.GatewayIngestSequence != 2 ||
		secondAck.CommittedCursorMSC != 2000 {
		t.Fatalf("unexpected ACKs before rebuild: first=%+v second=%+v", firstAck, secondAck)
	}
	if secondAck.GatewayIngestSequence != sealed.LastSequence+1 {
		t.Fatalf("active WAL did not continue after sealed sequence %d", sealed.LastSequence)
	}
	_, chainRootBefore := gateway.WAL().Last()
	_ = client.Close()
	stop()

	if err := os.Remove(config.JournalPath); err != nil {
		t.Fatal(err)
	}
	gateway, client, stop = startGateway(t, config)
	defer stop()
	if gateway.WAL().Count() != 2 {
		t.Fatalf("reopened WAL count = %d, want 2", gateway.WAL().Count())
	}
	if len(gateway.WAL().SealedSegments()) != 1 {
		t.Fatalf("reopened sealed segment count = %d, want 1", len(gateway.WAL().SealedSegments()))
	}
	_, chainRootAfter := gateway.WAL().Last()
	if chainRootAfter != chainRootBefore {
		t.Fatalf("chain root changed across rebuild: before=%x after=%x", chainRootBefore, chainRootAfter)
	}
	state, err := gateway.Journal().State()
	if err != nil {
		t.Fatal(err)
	}
	if state.CommittedCursorMSC != 2000 {
		t.Fatalf("rebuilt cursor = %d, want 2000", state.CommittedCursorMSC)
	}
	firstDuplicate, err := client.SendBatch(firstBatch)
	if err != nil {
		t.Fatal(err)
	}
	secondDuplicate, err := client.SendBatch(secondBatch)
	if err != nil {
		t.Fatal(err)
	}
	if firstDuplicate.Status != protocol.AckDuplicate || secondDuplicate.Status != protocol.AckDuplicate {
		t.Fatalf("rebuild did not preserve duplicate identity: first=%+v second=%+v", firstDuplicate, secondDuplicate)
	}
}

func TestGatewayRetriesAfterWALSyncBeforeJournalCommit(t *testing.T) {
	config := testConfig(t)
	gateway, client, stop := startGateway(t, config)
	defer stop()
	called := false
	gateway.SetHooks(ingest.Hooks{
		AfterWALSync: func() error {
			if !called {
				called = true
				return errors.New("injected WAL-sync crash")
			}
			return nil
		},
	})
	batch := testBatch(1, 1000, 1, 0)
	if _, err := client.SendBatch(batch); err == nil {
		t.Fatal("expected connection failure before ACK")
	}
	if gateway.WAL().Count() != 1 {
		t.Fatalf("WAL sync hook must leave one durable entry, got %d", gateway.WAL().Count())
	}
	if err := client.Reconnect(context.Background(), clientAddress(t, client)); err != nil {
		t.Fatal(err)
	}
	ack, err := client.SendBatch(batch)
	if err != nil {
		t.Fatal(err)
	}
	if ack.Status != protocol.AckDuplicate {
		t.Fatalf("expected rebuilt duplicate ACK, got %+v", ack)
	}
}

func TestGatewayStopDoesNotStartHandlerAfterAcceptReturns(t *testing.T) {
	config := testConfig(t)
	config.HeartbeatIdleTimeout = 5 * time.Second
	runtime := newStartedGatewayRuntime(t, config)
	gateway := runtime.Gateway()
	peer, rawConnection := net.Pipe()
	connection := &trackingConn{Conn: rawConnection, closed: make(chan struct{})}
	listener := &closeOnAcceptListener{
		connection: connection,
		closed:     make(chan struct{}),
	}
	accepted := make(chan struct{})
	allowAcceptReturn := make(chan struct{})
	closeErr := make(chan error, 1)
	listener.onAccept = func() {
		close(accepted)
		<-allowAcceptReturn
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	serveDone := make(chan error, 1)
	go func() { serveDone <- gateway.Serve(ctx, listener) }()
	defer peer.Close()

	select {
	case <-accepted:
	case <-time.After(time.Second):
		t.Fatal("listener did not call Accept callback")
	}
	go func() {
		closeErr <- gateway.Stop(context.Background())
	}()
	select {
	case <-listener.closed:
	case <-time.After(time.Second):
		t.Fatal("gateway Stop did not close listener")
	}
	close(allowAcceptReturn)
	select {
	case err := <-serveDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("gateway Serve did not stop")
	}
	if err := <-closeErr; err != nil {
		t.Fatal(err)
	}
	if err := runtime.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-connection.closed:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("accepted connection was registered after gateway close")
	}
}

func TestGatewayRetriesAfterJournalCommitBeforeACK(t *testing.T) {
	config := testConfig(t)
	gateway, client, stop := startGateway(t, config)
	defer stop()
	called := false
	gateway.SetHooks(ingest.Hooks{
		AfterJournalCommit: func() error {
			if !called {
				called = true
				return errors.New("injected ACK-loss crash")
			}
			return nil
		},
	})
	batch := testBatch(1, 1000, 1, 0)
	if _, err := client.SendBatch(batch); err == nil {
		t.Fatal("expected connection failure before ACK")
	}
	ack, err := func() (protocol.AckV1, error) {
		if err := client.Reconnect(context.Background(), clientAddress(t, client)); err != nil {
			return protocol.AckV1{}, err
		}
		return client.SendBatch(batch)
	}()
	if err != nil {
		t.Fatal(err)
	}
	if ack.Status != protocol.AckDuplicate {
		t.Fatalf("expected duplicate ACK after ACK loss, got %+v", ack)
	}
}

func TestGatewayDenseBoundaryDirectiveAndHardCap(t *testing.T) {
	config := testConfig(t)
	config.InitialFromMSC = 1000
	config.InitialBatchCount = 2
	config.MaximumBatchCount = 4
	config.DenseBoundaryHardCap = 4
	gateway, client, stop := startGateway(t, config)
	defer stop()

	full := testBatch(1, 1000, 2, 0)
	full.RequestedCount = 2
	ack, err := client.SendBatch(full)
	if err != nil {
		t.Fatal(err)
	}
	if ack.Status != protocol.AckDenseBoundary || ack.NextRequestedCount != 4 {
		t.Fatalf("expected dense continuation, got %+v", ack)
	}
	fullSecond := testBatch(2, 1000, 4, 0)
	fullSecond.RequestedCount = 4
	ack, err = client.SendBatch(fullSecond)
	if err != nil {
		t.Fatal(err)
	}
	if ack.Status != protocol.AckDenseUnresolved || ack.NextRequestedCount != 4 {
		t.Fatalf("expected dense hard cap, got %+v", ack)
	}
	if gateway.WAL().Count() != 2 {
		t.Fatalf("dense batches must remain raw evidence, got %d", gateway.WAL().Count())
	}
}

func TestGatewayKeepsSourceErrorAtCommittedCursor(t *testing.T) {
	config := testConfig(t)
	gateway, client, stop := startGateway(t, config)
	defer stop()
	first, err := client.SendBatch(testBatch(1, 1000, 1, 0))
	if err != nil {
		t.Fatal(err)
	}
	failed, err := client.SendBatch(testBatch(2, 1000, 0, 4401))
	if err != nil {
		t.Fatal(err)
	}
	if failed.Status != protocol.AckAcceptedNoAdvance || failed.CommittedCursorMSC != first.CommittedCursorMSC {
		t.Fatalf("source error advanced cursor: first=%+v failed=%+v", first, failed)
	}
	if gateway.WAL().Count() != 2 {
		t.Fatalf("source error batch must remain raw evidence, got %d", gateway.WAL().Count())
	}
}

func TestGatewayDoesNotAppendPartialFrame(t *testing.T) {
	config := testConfig(t)
	gateway, client, stop := startGateway(t, config)
	defer stop()
	batch := testBatch(1, 1000, 1, 0)
	frame, err := protocol.EncodeMessage(batch)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Conn().Write(frame[:7]); err != nil {
		t.Fatal(err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if gateway.WAL().Count() != 0 {
		t.Fatalf("partial frame must not reach WAL, got %d", gateway.WAL().Count())
	}
}

func testConfig(t *testing.T) ingest.Config {
	root := t.TempDir()
	return ingest.Config{
		ListenAddress:           "127.0.0.1:0",
		GatewayInstanceID:       "gateway-test-01",
		WALRoot:                 filepath.Join(root, "wal"),
		JournalPath:             filepath.Join(root, "journal", "gateway.sqlite"),
		MaxFrameBytes:           protocol.MaxFrameBytes,
		MaxRecords:              protocol.MaxRecords,
		InitialFromMSC:          0,
		InitialBatchCount:       2,
		MaximumBatchCount:       4,
		DenseBoundaryHardCap:    4,
		SessionLeaseTimeout:     5 * time.Second,
		HeartbeatIdleTimeout:    5 * time.Second,
		ProducerInstanceID:      "fake-01",
		ProducerBuildID:         "fake-test-v1",
		CampaignID:              "campaign-test-01",
		ProviderID:              "provider-test",
		StableFeedID:            "feed-test",
		BrokerServerFingerprint: "broker-test",
		ExactSourceSymbol:       "EURUSD",
	}
}

func testBatch(sequence uint64, timeMSC int64, count int, copyTicksError int32) protocol.BatchFrameV1 {
	records := make([]protocol.RawMqlTickV1, count)
	for i := range records {
		records[i] = protocol.RawMqlTickV1{
			Time:            timeMSC / 1000,
			BidBits:         uint64(100 + i),
			AskBits:         uint64(200 + i),
			LastBits:        uint64(150 + i),
			Volume:          uint64(i + 1),
			TimeMSC:         timeMSC,
			Flags:           3,
			VolumeRealBits:  uint64(300 + i),
			CaptureSequence: uint64(i + 1),
		}
	}
	returned := int32(count)
	if copyTicksError != 0 {
		returned = -1
		records = nil
	}
	return protocol.BatchFrameV1{
		ProducerSessionID:     "session-test-01",
		BatchSequence:         sequence,
		RequestedFromMSC:      timeMSC,
		RequestedCount:        uint32(count),
		FetchWallStartS:       1710000000,
		FetchWallEndS:         1710000001,
		FetchMonotonicStartUS: 100,
		FetchMonotonicEndUS:   200,
		ReturnedCount:         returned,
		CopyTicksError:        copyTicksError,
		SourceSchemaID:        protocol.SourceSchemaMT5,
		Records:               records,
	}
}

func startGateway(t *testing.T, config ingest.Config) (*ingest.Gateway, *fake.Client, func()) {
	t.Helper()
	runtime := newStartedGatewayRuntime(t, config)
	gateway := runtime.Gateway()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = runtime.Stop(context.Background())
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	serveDone := make(chan error, 1)
	go func() { serveDone <- gateway.Serve(ctx, listener) }()
	hello := protocol.HelloV1{
		ProducerInstanceID:      config.ProducerInstanceID,
		ProducerSessionID:       "session-test-01",
		ProducerBuildID:         config.ProducerBuildID,
		MQLCompilerBuild:        "fake",
		TerminalBuild:           "fake",
		OSContract:              "windows-test",
		ClockAPIID:              "test-clock",
		CampaignID:              config.CampaignID,
		ProviderID:              config.ProviderID,
		StableFeedID:            config.StableFeedID,
		BrokerServerFingerprint: config.BrokerServerFingerprint,
		ExactSourceSymbol:       config.ExactSourceSymbol,
		SourceSchemaID:          protocol.SourceSchemaMT5,
		AcquisitionMode:         1,
		InitialFromMSC:          config.InitialFromMSC,
	}
	client, err := fake.Dial(context.Background(), listener.Addr().String(), hello)
	if err != nil {
		cancel()
		_ = runtime.Stop(context.Background())
		t.Fatal(err)
	}
	stop := func() {
		_ = client.Close()
		cancel()
		_ = runtime.Stop(context.Background())
		select {
		case <-serveDone:
		case <-time.After(time.Second):
			t.Fatal("gateway Serve did not stop")
		}
	}
	return gateway, client, stop
}

func newStartedGatewayRuntime(t *testing.T, config ingest.Config) *app.LocalGatewayRuntime {
	t.Helper()
	runtime, err := app.NewLocalGatewayRuntime(config, ingest.DiskWatermarks{
		HighFreeBytes:      config.DiskHighFreeBytes,
		CriticalFreeBytes:  config.DiskCriticalFreeBytes,
		EmergencyFreeBytes: config.DiskEmergencyFreeBytes,
		MaxPendingSegments: config.MaxPendingSegments,
		MaxPendingBytes:    config.MaxPendingBytes,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	return runtime
}

func clientAddress(t *testing.T, client *fake.Client) string {
	t.Helper()
	if client.Address == "" {
		t.Fatal("fake client address missing")
	}
	return client.Address
}

type trackingConn struct {
	net.Conn
	closed    chan struct{}
	closeOnce sync.Once
}

func (c *trackingConn) Close() error {
	c.closeOnce.Do(func() { close(c.closed) })
	return c.Conn.Close()
}

type closeOnAcceptListener struct {
	connection net.Conn
	onAccept   func()
	closed     chan struct{}
	closeOnce  sync.Once
}

func (l *closeOnAcceptListener) Accept() (net.Conn, error) {
	if l.connection != nil {
		connection := l.connection
		l.connection = nil
		l.onAccept()
		return connection, nil
	}
	<-l.closed
	return nil, net.ErrClosed
}

func (l *closeOnAcceptListener) Close() error {
	l.closeOnce.Do(func() { close(l.closed) })
	return nil
}

func (l *closeOnAcceptListener) Addr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)}
}
