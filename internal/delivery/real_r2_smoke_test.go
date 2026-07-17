//go:build real_r2_smoke

package delivery

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
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
	realR2EnableEnv    = "TICK_M2_REAL_R2_SMOKE"
	realR2ConfirmEnv   = "TICK_M2_REAL_R2_CONFIRM"
	realR2BucketEnv    = "TICK_M2_REAL_R2_BUCKET"
	realR2PrefixEnv    = "TICK_M2_REAL_R2_PREFIX"
	realR2EndpointEnv  = "TICK_M2_REAL_R2_ENDPOINT"
	realR2AccessEnv    = "AWS_ACCESS_KEY_ID"
	realR2SecretEnv    = "AWS_SECRET_ACCESS_KEY"
	realR2Confirmation = "I_UNDERSTAND_NO_OVERWRITE"
)

func TestOptionalRealR2Smoke(t *testing.T) {
	if os.Getenv(realR2EnableEnv) != "1" {
		t.Skipf("set %s=1 to enable the isolated real-R2 smoke", realR2EnableEnv)
	}
	if os.Getenv(realR2ConfirmEnv) != realR2Confirmation {
		t.Skipf("set %s to the explicit no-overwrite confirmation", realR2ConfirmEnv)
	}
	for _, name := range []string{
		realR2BucketEnv, realR2PrefixEnv, realR2EndpointEnv, realR2AccessEnv, realR2SecretEnv,
	} {
		if strings.TrimSpace(os.Getenv(name)) == "" {
			t.Skipf("required smoke variable %s is unavailable", name)
		}
	}
	bucket := os.Getenv(realR2BucketEnv)
	prefix := strings.Trim(os.Getenv(realR2PrefixEnv), "/")
	if !strings.HasPrefix(prefix, "m2-smoke/") || strings.Contains(prefix, "..") || strings.ContainsAny(prefix, "\\\r\n") {
		t.Skip("the smoke prefix must be an isolated m2-smoke/ prefix without traversal")
	}

	var suffixBytes [16]byte
	if _, err := rand.Read(suffixBytes[:]); err != nil {
		t.Fatal(err)
	}
	suffix := hex.EncodeToString(suffixBytes[:])
	immutableRoot := prefix + "/run-" + suffix
	scope := m2E2EScope()
	scope.CampaignID = "real-r2-smoke-" + suffix
	scope.PublisherID = "real-r2-smoke-publisher-" + suffix
	layout, err := r2.NewLayout(immutableRoot, scope)
	if err != nil {
		t.Fatal(err)
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
	endpoint := os.Getenv(realR2EndpointEnv)
	if err := r2.ValidateHTTPSHostEndpoint(endpoint); err != nil {
		t.Fatalf("real R2 endpoint is invalid: %v", err)
	}
	credentialsPath := filepath.Join(t.TempDir(), "credentials.json")
	credentialsBody, err := json.Marshal(struct {
		FormatVersion   int    `json:"format_version"`
		AccessKeyID     string `json:"access_key_id"`
		SecretAccessKey string `json:"secret_access_key"`
	}{
		FormatVersion:   1,
		AccessKeyID:     os.Getenv(realR2AccessEnv),
		SecretAccessKey: os.Getenv(realR2SecretEnv),
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
		t.Fatal(err)
	}
	reader, err := NewArchiveReaderV1(context.Background(), ReaderConfig{
		Version: ReaderConfigVersion, Endpoint: endpoint,
		Bucket: bucket, CredentialsPath: credentialsPath,
		Region: "auto", ImmutableRoot: immutableRoot, CacheRoot: t.TempDir(),
		MaxMetadataBytes: 1 << 20, MaxRawObjectBytes: 1 << 30,
	})
	if err != nil {
		t.Fatal(err)
	}
	manifestKey, err := layout.ManifestKey(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reader.VerifyDay(context.Background(), SnapshotSelector{Manifest: manifestKey}); err != nil {
		t.Fatal(err)
	}
	if _, err := reader.VerifyCampaign(context.Background(), scope.DatasetID, scope.CampaignID, hex.EncodeToString(object.Segment.ChainRoot[:])); err != nil {
		t.Fatal(err)
	}
}
