package app

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"

	appconfig "tick-data-platform/internal/config"
	"tick-data-platform/internal/credentials"
)

func TestProductionGraphValidates(t *testing.T) {
	if err := fx.ValidateApp(TestOptions(testConfig(t), &staticProvider{})); err != nil {
		t.Fatal(err)
	}
}

func TestProductionGraphRejectsMissingProvider(t *testing.T) {
	err := fx.ValidateApp(fx.Options(
		fx.Supply(testConfig(t)), BaseOptions(),
	))
	if err == nil {
		t.Fatal("graph without credential provider unexpectedly validated")
	}
}

func TestNewProductionAppReturnsConstructionError(t *testing.T) {
	config := testConfig(t)
	config.R2.Endpoint = "http://invalid.example"
	_, err := NewProductionApp(config)
	if err == nil {
		t.Fatal("missing production config unexpectedly constructed an application")
	}
}

func TestApplicationStartStopLoadsCredentialOnce(t *testing.T) {
	provider := &staticProvider{}
	application := fxtest.New(t, TestOptions(testConfig(t), provider))
	application.RequireStart()
	if got := atomic.LoadInt64(&provider.calls); got != 1 {
		t.Fatalf("provider calls after start = %d", got)
	}
	application.RequireStop()
	if got := atomic.LoadInt64(&provider.calls); got != 1 {
		t.Fatalf("provider calls after stop = %d", got)
	}
}

func TestApplicationStartStopCanBeCalledTwice(t *testing.T) {
	application := fx.New(TestOptions(testConfig(t), &staticProvider{}))
	startCtx, cancelStart := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelStart()
	if err := application.Start(startCtx); err != nil {
		t.Fatal(err)
	}
	if err := application.Start(startCtx); err == nil {
		t.Fatal("second application Start unexpectedly succeeded")
	}
	stopCtx, cancelStop := context.WithTimeout(context.Background(), time.Second)
	defer cancelStop()
	if err := application.Stop(stopCtx); err != nil {
		t.Fatal(err)
	}
	if err := application.Stop(stopCtx); err != nil {
		t.Fatalf("second application Stop error = %v", err)
	}
}

func TestApplicationStartFailureRollsBackStartedResources(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	config := testConfig(t)
	config.ListenAddress = listener.Addr().String()
	application := fx.New(TestOptions(config, &staticProvider{}))
	startCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := application.Start(startCtx); err == nil {
		t.Fatal("startup unexpectedly succeeded while listener was occupied")
	}
	stopCtx, stopCancel := context.WithTimeout(context.Background(), time.Second)
	defer stopCancel()
	if err := application.Stop(stopCtx); err != nil {
		t.Fatalf("rollback stop failed: %v", err)
	}
	if err := os.Remove(config.JournalPath); err != nil {
		t.Fatalf("journal remained open after rollback: %v", err)
	}
}

func TestOnStopErrorIsReturned(t *testing.T) {
	want := errors.New("stop failure")
	application := fx.New(fx.Invoke(func(lifecycle fx.Lifecycle) {
		lifecycle.Append(fx.Hook{
			OnStart: func(context.Context) error { return nil },
			OnStop:  func(context.Context) error { return want },
		})
	}))
	startCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := application.Start(startCtx); err != nil {
		t.Fatal(err)
	}
	stopCtx, stopCancel := context.WithTimeout(context.Background(), time.Second)
	defer stopCancel()
	if err := application.Stop(stopCtx); !errors.Is(err, want) {
		t.Fatalf("stop error = %v, want %v", err, want)
	}
}

type staticProvider struct {
	calls int64
}

func (p *staticProvider) Load(context.Context) (credentials.Credentials, error) {
	atomic.AddInt64(&p.calls, 1)
	return credentials.Credentials{AccessKeyID: "app-test-access", SecretAccessKey: "app-test-secret"}, nil
}

func testConfig(t *testing.T) appconfig.Config {
	t.Helper()
	root := t.TempDir()
	return appconfig.Config{
		ListenAddress: "127.0.0.1:0", GatewayInstanceID: "gateway-app-test",
		WALRoot: filepath.Join(root, "wal"), RawOutboxRoot: filepath.Join(root, "outbox"), JournalPath: filepath.Join(root, "journal.sqlite"),
		SessionLeaseTimeoutMS: 30000, HeartbeatIdleTimeoutMS: 60000,
		Credentials: appconfig.CredentialsConfig{Provider: "file", Path: "not-read"},
		R2:          appconfig.R2Config{Endpoint: "https://account.r2.cloudflarestorage.com", Bucket: "tick-raw", Region: "auto", ImmutableRoot: "v1"},
		Publication: appconfig.PublicationConfig{
			CatalogPath: filepath.Join(root, "catalog.sqlite"), RemoteJournalPath: filepath.Join(root, "remote.sqlite"), ManifestRoot: filepath.Join(root, "manifests"), ReceiptRoot: filepath.Join(root, "receipts"),
			SealMaxBytes: 64 << 20, SealIntervalMS: 60000, ScanIntervalMS: 1000,
			RetryMinMS: 1000, RetryMaxMS: 300000, MaxPendingSegments: 1000, MaxPendingBytes: 1 << 30,
		},
		ProducerBuildID: "producer-build-test", DatasetID: "dataset-test", CampaignID: "campaign-test",
		ProviderID: "provider-test", StableFeedID: "feed-test", BrokerServerFingerprint: "broker-test",
		ExactSourceSymbol: "EURUSD", GatewayBuildIdentity: "gateway-build-test", DayDefinitionID: "utc-day-v1",
		SettlePolicy: "manual-v1", PublisherID: "gateway-test", PublisherEpoch: 1,
		DiskHighFreeBytes: 512 << 20, DiskCriticalFreeBytes: 256 << 20, DiskEmergencyFreeBytes: 64 << 20,
	}
}
