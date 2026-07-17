package operations_test

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/gofrs/flock"
	"tick-data-platform/internal/app"
	"tick-data-platform/internal/archive"
	"tick-data-platform/internal/ingest"
	"tick-data-platform/internal/operations"
	"tick-data-platform/producers/fake"
	protocolv1 "tick-data-platform/protocol/v1/go"
)

func integrationScopeConfig(t *testing.T, index int) operations.ScopeProcessConfig {
	t.Helper()
	root := t.TempDir()
	identity := string(rune('0' + index))
	return operations.ScopeProcessConfig{
		Scope: archive.ScopeConfig{
			DatasetID: "dataset-integration-" + identity, ProviderID: "provider-integration-" + identity,
			StableFeedID:            "feed-integration-" + identity,
			ExactSourceSymbol:       "EURUSD.raw",
			BrokerServerFingerprint: "broker-integration-" + identity,
			GatewayBuildIdentity:    "gateway-build-1",
			ProducerBuildIdentity:   "producer-build-1",
			DayDefinitionID:         "utc-day-v1",
			SettlePolicy:            "manual-v1",
			PublisherID:             "publisher-integration-" + identity,
			PublisherEpoch:          1,
		},
		GatewayInstanceID: "gateway-integration-" + identity,
		ListenAddress:     "127.0.0.1:" + strconv.Itoa(18100+index),
		GatewayConfigPath: filepath.Join(root, "gateway.toml"),
		MQLConfigPath:     filepath.Join(root, "mql.toml"),
		WALRoot:           filepath.Join(root, "wal"),
		JournalPath:       filepath.Join(root, "journal", "gateway.sqlite"),
		OutboxRoot:        filepath.Join(root, "outbox"),
		ReceiptRoot:       filepath.Join(root, "receipts"),
		LockRoot:          filepath.Join(root, "locks"),
		CredentialPrefix:  "tick/integration/" + identity,
	}
}

type integrationGateway struct {
	runtime *app.LocalGatewayRuntime
	gateway *ingest.Gateway
	client  *fake.Client
	cancel  context.CancelFunc
	done    <-chan error
}

func openIntegrationGateway(t *testing.T, scope operations.ScopeProcessConfig) integrationGateway {
	t.Helper()
	config := ingest.Config{
		ListenAddress:        scope.ListenAddress,
		GatewayInstanceID:    scope.GatewayInstanceID,
		WALRoot:              scope.WALRoot,
		JournalPath:          scope.JournalPath,
		MaxRecords:           4,
		InitialBatchCount:    1,
		MaximumBatchCount:    4,
		DenseBoundaryHardCap: 4,
		SessionLeaseTimeout:  5 * time.Second,
		HeartbeatIdleTimeout: 5 * time.Second,
		ProducerInstanceID:   "producer-" + scope.GatewayInstanceID,
		ProducerBuildID:      "producer-build-1", ProviderID: scope.Scope.ProviderID,
		StableFeedID:            scope.Scope.StableFeedID,
		BrokerServerFingerprint: scope.Scope.BrokerServerFingerprint,
		ExactSourceSymbol:       scope.Scope.ExactSourceSymbol,
	}
	runtime, err := app.NewLocalGatewayRuntime(config, ingest.DiskWatermarks{})
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	gateway := runtime.Gateway()
	server, clientConn := net.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- gateway.HandleConn(ctx, server) }()
	client, err := fake.New(clientConn, protocolv1.HelloV1{
		ProducerInstanceID: config.ProducerInstanceID,
		ProducerSessionID:  "session-" + scope.GatewayInstanceID,
		ProducerBuildID:    config.ProducerBuildID,
		MQLCompilerBuild:   "fake",
		TerminalBuild:      "fake",
		OSContract:         "windows-test",
		ClockAPIID:         "test-clock", ProviderID: config.ProviderID,
		StableFeedID:            config.StableFeedID,
		BrokerServerFingerprint: config.BrokerServerFingerprint,
		ExactSourceSymbol:       config.ExactSourceSymbol,
		SourceSchemaID:          protocolv1.SourceSchemaMT5,
		AcquisitionMode:         1,
		InitialFromMSC:          config.InitialFromMSC,
	})
	if err != nil {
		cancel()
		_ = runtime.Stop(context.Background())
		t.Fatal(err)
	}
	return integrationGateway{runtime: runtime, gateway: gateway, client: client, cancel: cancel, done: done}
}

func (g integrationGateway) close(t *testing.T) {
	t.Helper()
	_ = g.client.Close()
	g.cancel()
	_ = g.runtime.Stop(context.Background())
	select {
	case <-g.done:
	case <-time.After(time.Second):
		t.Fatal("integration gateway handler did not stop")
	}
}

func integrationBatch(sequence uint64, session string) protocolv1.BatchFrameV1 {
	return protocolv1.BatchFrameV1{
		ProducerSessionID:     session,
		BatchSequence:         sequence,
		RequestedFromMSC:      1000,
		RequestedCount:        1,
		FetchWallStartS:       1710000000,
		FetchWallEndS:         1710000001,
		FetchMonotonicStartUS: 100,
		FetchMonotonicEndUS:   200,
		ReturnedCount:         1,
		SourceSchemaID:        protocolv1.SourceSchemaMT5,
		Records: []protocolv1.RawMqlTickV1{{
			Time: 1, BidBits: 100, AskBits: 200, LastBits: 150, Volume: 1, TimeMSC: 1000,
			Flags: 3, VolumeRealBits: 300, CaptureSequence: 1,
		}},
	}
}

func TestMultiScopeFailureDoesNotBlockOtherACKPath(t *testing.T) {
	left, right := integrationScopeConfig(t, 1), integrationScopeConfig(t, 2)
	if _, err := operations.BuildSupervisorPlan([]operations.ScopeProcessConfig{left, right}); err != nil {
		t.Fatal(err)
	}
	if left.CredentialPrefix == right.CredentialPrefix || left.LockRoot == right.LockRoot {
		t.Fatal("scope credentials or lock roots are shared")
	}
	if err := os.MkdirAll(left.LockRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(right.LockRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	leftLock := flock.New(filepath.Join(left.LockRoot, "publication.lock"))
	rightLock := flock.New(filepath.Join(right.LockRoot, "publication.lock"))
	leftLocked, err := leftLock.TryLock()
	if err != nil || !leftLocked {
		t.Fatalf("could not acquire left scope lock: locked=%v err=%v", leftLocked, err)
	}
	defer leftLock.Unlock()
	rightLocked, err := rightLock.TryLock()
	if err != nil || !rightLocked {
		t.Fatalf("right scope lock was blocked by left scope: locked=%v err=%v", rightLocked, err)
	}
	defer rightLock.Unlock()
	leftGateway := openIntegrationGateway(t, left)
	rightGateway := openIntegrationGateway(t, right)
	defer leftGateway.close(t)
	defer rightGateway.close(t)

	leftGateway.gateway.SetHooks(ingest.Hooks{BeforeACK: func() error { return errors.New("scope-local network failure") }})
	if _, err := leftGateway.client.SendBatch(integrationBatch(1, leftGateway.client.Hello.ProducerSessionID)); err == nil {
		t.Fatal("failed scope unexpectedly returned an ACK")
	}
	ack, err := rightGateway.client.SendBatch(integrationBatch(1, rightGateway.client.Hello.ProducerSessionID))
	if err != nil {
		t.Fatal(err)
	}
	if ack.Status != protocolv1.AckAcceptedAdvanced {
		t.Fatalf("healthy scope did not ACK independently: %+v", ack)
	}
	if leftGateway.gateway.WAL().Count() != 1 || rightGateway.gateway.WAL().Count() != 1 {
		t.Fatalf("scope WALs were not independently durable: left=%d right=%d", leftGateway.gateway.WAL().Count(), rightGateway.gateway.WAL().Count())
	}
	leftKey, err := left.ScopeKey()
	if err != nil {
		t.Fatal(err)
	}
	rightKey, err := right.ScopeKey()
	if err != nil {
		t.Fatal(err)
	}
	healthy := operations.ScopeHealthStatus{ScopeKey: rightKey, PublisherEpoch: 1, TerminalSynchronization: "synced"}
	for _, failure := range []string{"disk_high_watermark", "r2_outage"} {
		aggregate, err := operations.AggregateScopeHealth([]operations.ScopeHealthStatus{
			{ScopeKey: leftKey, PublisherEpoch: 1, BlockedReason: failure}, healthy,
		})
		if err != nil {
			t.Fatal(err)
		}
		for _, status := range aggregate.Scopes {
			if status.ScopeKey == rightKey && status != healthy {
				t.Fatalf("%s in left scope changed right scope status: %+v", failure, status)
			}
		}
	}
}
