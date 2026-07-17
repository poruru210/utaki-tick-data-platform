//go:build r2_smoke

package delivery

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"tick-data-platform/internal/archive"
	"tick-data-platform/internal/credentials"
	"tick-data-platform/internal/r2"
	"tick-data-platform/internal/testsupport"
)

const (
	r2BucketEnv          = "TICK_R2_BUCKET"
	r2ImmutableRootEnv   = "TICK_R2_IMMUTABLE_ROOT"
	r2EndpointEnv        = "TICK_R2_ENDPOINT"
	r2AccessKeyIDEnv     = "TICK_R2_ACCESS_KEY_ID"
	r2SecretAccessKeyEnv = "TICK_R2_SECRET_ACCESS_KEY"
	r2GatewayIDEnv       = "TICK_GATEWAY_INSTANCE_ID"
)

func TestR2Smoke(t *testing.T) {
	r2Smoke(t, "EURUSD")
}

func TestR2SmokeSymbolWithHash(t *testing.T) {
	r2Smoke(t, "EURUSD.pro#")
}

func r2Smoke(t *testing.T, exactSourceSymbol string) {
	t.Helper()
	if err := testsupport.LoadRepositoryEnvLocal(); err != nil {
		t.Fatalf("load repository env.local: %v", err)
	}
	for _, name := range []string{
		r2BucketEnv, r2ImmutableRootEnv, r2EndpointEnv, r2AccessKeyIDEnv, r2SecretAccessKeyEnv, r2GatewayIDEnv,
	} {
		if strings.TrimSpace(os.Getenv(name)) == "" {
			t.Skipf("required smoke variable %s is unavailable", name)
		}
	}
	if !r2SmokeLooksLikeR2SecretAccessKey(os.Getenv(r2SecretAccessKeyEnv)) {
		t.Fatalf("%s must be the R2 S3 Secret Access Key, not the Cloudflare API token value", r2SecretAccessKeyEnv)
	}
	bucket := os.Getenv(r2BucketEnv)
	immutableRoot := r2SmokePublicationRoot(t, os.Getenv(r2ImmutableRootEnv), os.Getenv(r2GatewayIDEnv), time.Now().UTC())

	scope := r2SmokeScope(exactSourceSymbol)
	layout, err := r2.NewLayout(immutableRoot, scope)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(layout.ImmutableScopePrefix()+"/", "symbol="+r2SmokeExactPathComponent(exactSourceSymbol)+"/") {
		t.Fatalf("R2 key prefix does not preserve exact symbol %q: %q", exactSourceSymbol, layout.ImmutableScopePrefix())
	}
	object, _ := m2E2ESealedObject(t)
	manifest, err := archive.BuildRawDayManifest(archive.RawDayManifestInput{
		Scope: scope, Date: "2024-03-09", RawObjects: []archive.RawObject{object},
		TerminalSyncStatus: "complete", CompletenessStatus: "settled_snapshot", LogicalCloseTimeS: 1710028860,
	})
	if err != nil {
		t.Fatal(err)
	}
	manifestBytes, err := archive.ManifestCanonicalJSON(manifest)
	if err != nil {
		t.Fatal(err)
	}
	endpoint := os.Getenv(r2EndpointEnv)
	if err := r2.ValidateHTTPSHostEndpoint(endpoint); err != nil {
		t.Fatalf("R2 endpoint is invalid: %v", err)
	}
	credentialsPath := filepath.Join(t.TempDir(), "credentials.json")
	credentialsBody, err := json.Marshal(struct {
		FormatVersion   int    `json:"format_version"`
		AccessKeyID     string `json:"access_key_id"`
		SecretAccessKey string `json:"secret_access_key"`
	}{
		FormatVersion:   1,
		AccessKeyID:     os.Getenv(r2AccessKeyIDEnv),
		SecretAccessKey: os.Getenv(r2SecretAccessKeyEnv),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(credentialsPath, credentialsBody, 0o600); err != nil {
		t.Fatal(err)
	}
	provider, err := credentials.NewFileProvider(credentials.FileConfig{Path: credentialsPath, Protection: credentials.ProtectionNativeACL})
	if err != nil {
		t.Fatal(err)
	}
	backend, err := r2.NewCredentialBackend(r2.S3BackendConfig{
		Bucket: bucket, Endpoint: endpoint, Region: "auto",
	}, provider)
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = backend.Stop(context.Background()) })
	journal, err := testsupport.NewStartedPublicationJournal(filepath.Join(t.TempDir(), "publication.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = journal.Stop(context.Background()) })
	publisher, err := r2.NewPublisher(layout, backend, journal, "", time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := publisher.Publish(context.Background(), r2.PublicationInput{
		Manifest: manifest, ManifestBytes: manifestBytes, ObjectPaths: map[string]string{object.Key: object.Path},
	}); err != nil {
		t.Fatalf("publish smoke objects to R2: %v", err)
	}
	reader, err := NewArchiveReaderV1(context.Background(), ReaderConfig{
		Version: ReaderConfigVersion, Endpoint: endpoint,
		Bucket: bucket, CredentialsPath: credentialsPath,
		Region: "auto", ImmutableRoot: immutableRoot, CacheRoot: t.TempDir(),
		MaxMetadataBytes: 1 << 20, MaxRawObjectBytes: 1 << 30,
	})
	if err != nil {
		t.Fatalf("open archive reader from R2: %v", err)
	}
	manifestKey, err := layout.ManifestKey(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reader.VerifyDay(context.Background(), SnapshotSelector{Manifest: manifestKey}); err != nil {
		t.Fatalf("verify smoke raw day from R2: %v", err)
	}
	if _, err := reader.VerifyScope(context.Background(), RawScopeSelector{DatasetID: scope.DatasetID, ProviderID: scope.ProviderID, ExactSourceSymbol: scope.ExactSourceSymbol}, hex.EncodeToString(object.Segment.ChainRoot[:])); err != nil {
		t.Fatalf("verify smoke scope from R2: %v", err)
	}
}

func r2SmokePublicationRoot(t *testing.T, baseRoot, gatewayID string, now time.Time) string {
	t.Helper()
	baseRoot = strings.TrimRight(strings.TrimSpace(baseRoot), "/")
	if baseRoot == "" || strings.Contains(baseRoot, "..") || strings.ContainsAny(baseRoot, "\\\r\n") {
		t.Fatalf("%s must be a normal immutable root without traversal", r2ImmutableRootEnv)
	}
	for _, part := range strings.Split(baseRoot, "/") {
		if part == "smoke" {
			t.Fatalf("%s must not contain a smoke path segment; smoke is encoded as source=smoke", r2ImmutableRootEnv)
		}
	}
	publicationRoot, err := r2.PublicationRunRoot(baseRoot, gatewayID, r2.PublicationRunID(now))
	if err != nil {
		t.Fatalf("derive R2 publication root: %v", err)
	}
	return publicationRoot
}

func r2SmokeLooksLikeR2SecretAccessKey(value string) bool {
	value = strings.TrimSpace(value)
	if len(value) != 64 {
		return false
	}
	for _, b := range []byte(value) {
		if (b >= '0' && b <= '9') || (b >= 'a' && b <= 'f') || (b >= 'A' && b <= 'F') {
			continue
		}
		return false
	}
	return true
}

func r2SmokeExactPathComponent(value string) string {
	var encoded strings.Builder
	for _, b := range []byte(value) {
		if (b >= 'A' && b <= 'Z') ||
			(b >= 'a' && b <= 'z') ||
			(b >= '0' && b <= '9') ||
			b == '-' || b == '_' || b == '.' ||
			b == '*' || b == '\'' || b == '(' || b == ')' {
			encoded.WriteByte(b)
			continue
		}
		encoded.WriteString(fmt.Sprintf("!%02X", b))
	}
	return encoded.String()
}

func r2SmokeScope(exactSourceSymbol string) archive.ScopeConfig {
	return archive.ScopeConfig{
		DatasetID: "smoke", ProviderID: "smoke",
		StableFeedID:            "tick",
		ExactSourceSymbol:       exactSourceSymbol,
		BrokerServerFingerprint: "smoke",
		GatewayBuildIdentity:    "gateway",
		ProducerBuildIdentity:   "producer",
		DayDefinitionID:         "utc",
		SettlePolicy:            "manual",
		PublisherID:             "publisher",
		PublisherEpoch:          1,
	}
}
