//go:build real_r2_smoke

package delivery

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"tick-data-platform/internal/archive"
	"tick-data-platform/internal/r2"
)

const (
	realR2EnableEnv    = "TICK_M2_REAL_R2_SMOKE"
	realR2ConfirmEnv   = "TICK_M2_REAL_R2_CONFIRM"
	realR2BucketEnv    = "TICK_M2_REAL_R2_BUCKET"
	realR2PrefixEnv    = "TICK_M2_REAL_R2_PREFIX"
	realR2EndpointEnv  = "TICK_M2_REAL_R2_ENDPOINT"
	realR2RemoteEnv    = "TICK_M2_REAL_R2_REMOTE"
	realR2BinaryEnv    = "TICK_M2_RCLONE_BINARY"
	realR2ConfigEnv    = "RCLONE_CONFIG"
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
		realR2BucketEnv, realR2PrefixEnv, realR2EndpointEnv, realR2RemoteEnv,
		realR2BinaryEnv, realR2ConfigEnv, realR2AccessEnv, realR2SecretEnv,
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
	remote := os.Getenv(realR2RemoteEnv)
	if !validSmokeRemote(remote) {
		t.Skip("the smoke rclone remote name is not a simple configured remote name")
	}
	if stat, err := os.Stat(os.Getenv(realR2ConfigEnv)); err != nil || !stat.Mode().IsRegular() {
		t.Skip("the configured rclone profile is unavailable")
	}

	lock, err := r2.LoadToolLock(filepath.Join("..", "..", "tools", "tick-data-tools.lock.toml"))
	if err != nil {
		t.Fatal(err)
	}
	rclone, err := r2.NewRcloneRunnerForPlatform(lock, os.Getenv(realR2BinaryEnv), runtime.GOOS, runtime.GOARCH)
	if err != nil {
		t.Skip("the pinned rclone lock has no entry for this smoke platform")
	}

	var suffixBytes [16]byte
	if _, err := rand.Read(suffixBytes[:]); err != nil {
		t.Fatal(err)
	}
	suffix := hex.EncodeToString(suffixBytes[:])
	immutableRoot := prefix + "/run-" + suffix
	rcloneRoot := remote + ":" + bucket + "/" + immutableRoot
	scope := m2E2EScope()
	scope.CampaignID = "real-r2-smoke-" + suffix
	scope.PublisherID = "real-r2-smoke-publisher-" + suffix
	layout, err := r2.NewLayout(immutableRoot, rcloneRoot, scope)
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
	backend, err := r2.NewS3Backend(context.Background(), r2.S3BackendConfig{
		Bucket: bucket, Endpoint: os.Getenv(realR2EndpointEnv), Region: "auto",
	})
	if err != nil {
		t.Fatal(err)
	}
	journal, err := r2.OpenPublicationJournal(filepath.Join(t.TempDir(), "publication.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = journal.Close() })
	publisher, err := r2.NewPublisher(layout, backend, rclone, journal, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := publisher.Publish(context.Background(), r2.PublicationInput{
		Manifest: manifest, ManifestBytes: manifestBytes, ObjectPaths: map[string]string{object.Key: object.Path},
	}); err != nil {
		t.Fatal(err)
	}
	reader, err := NewArchiveReaderV1(context.Background(), ReaderConfig{
		Version: ReaderConfigVersion, Endpoint: os.Getenv(realR2EndpointEnv),
		BucketEnv: realR2BucketEnv, AccessKeyEnv: realR2AccessEnv, SecretKeyEnv: realR2SecretEnv,
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

func validSmokeRemote(value string) bool {
	if value == "" {
		return false
	}
	for _, char := range value {
		if (char < 'a' || char > 'z') && (char < 'A' || char > 'Z') && (char < '0' || char > '9') && char != '_' && char != '-' {
			return false
		}
	}
	return true
}
