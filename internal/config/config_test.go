package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadStrictTypedConfigAndConversion(t *testing.T) {
	t.Setenv(CredentialsPathEnv, "/run/credentials/override")
	path := writeConfig(t, `
listen_address = "127.0.0.1:17001"
gateway_instance_id = "gateway-test"
wal_root = "./wal"
raw_outbox_root = "./outbox"
journal_path = "./journal/gateway.sqlite"
session_lease_timeout_ms = 30000
heartbeat_idle_timeout_ms = 60000
producer_instance_id = "producer"
producer_build_id = "build"
dataset_id = "dataset"
provider_id = "provider"
stable_feed_id = "feed"
broker_server_fingerprint = "broker"
exact_source_symbol = "EURUSD"
gateway_build_identity = "gateway-build"
day_definition_id = "utc-day-v1"
settle_policy = "manual-v1"
publisher_id = "publisher"
publisher_epoch = 1

[credentials]
provider = "file"
path = "/run/credentials/original"
protection = "managed-mount"

[r2]
endpoint = "https://account.r2.cloudflarestorage.com"
bucket = "tick-raw"
region = "auto"
immutable_root = "v1"

[publication]
catalog_path = "./publication/catalog.sqlite"
remote_journal_path = "./publication/remote.sqlite"
manifest_root = "./publication/manifests"
receipt_root = "./publication/receipts"
seal_max_bytes = 67108864
seal_interval_ms = 60000
scan_interval_ms = 1000
retry_min_ms = 1000
retry_max_ms = 300000
max_pending_segments = 1000
max_pending_bytes = 1099511627776
`)
	loaded, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Credentials.Path != "/run/credentials/override" {
		t.Fatalf("credential path = %q", loaded.Credentials.Path)
	}
	if err := loaded.ValidateForRun(); err != nil {
		t.Fatal(err)
	}
	gateway := loaded.Gateway()
	if gateway.SessionLeaseTimeout != 30*time.Second || gateway.HeartbeatIdleTimeout != 60*time.Second {
		t.Fatalf("gateway timeout conversion = %v/%v", gateway.SessionLeaseTimeout, gateway.HeartbeatIdleTimeout)
	}
}

func TestLoadRejectsUnknownFields(t *testing.T) {
	path := writeConfig(t, "listen_address = \"127.0.0.1:1\"\nunknown = true\n")
	if _, err := Load(path); err == nil || !strings.Contains(err.Error(), "decode gateway config") {
		t.Fatalf("unknown field error = %v", err)
	}
}

func TestLoadRejectsRemovedRuntimeCompatibilityFields(t *testing.T) {
	for _, field := range []string{
		"outbox_root = \"./outbox\"",
		"r2_bucket_env = \"TICK_R2_BUCKET\"",
		"r2_prefix = \"smoke/v1\"",
	} {
		t.Run(strings.Split(field, " ")[0], func(t *testing.T) {
			path := writeConfig(t, "listen_address = \"127.0.0.1:1\"\n"+field+"\n")
			if _, err := Load(path); err == nil || !strings.Contains(err.Error(), "decode gateway config") {
				t.Fatalf("removed field %q was accepted: %v", field, err)
			}
		})
	}
}

func TestValidateForRunRejectsIncompletePublication(t *testing.T) {
	config := Config{ListenAddress: "127.0.0.1:1", WALRoot: "wal", RawOutboxRoot: "outbox", JournalPath: "journal", Credentials: CredentialsConfig{Provider: "file", Path: "credentials"}}
	if err := config.ValidateForRun(); err == nil || !strings.Contains(err.Error(), "R2") {
		t.Fatalf("incomplete run config error = %v", err)
	}
}

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "tick-gateway.toml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
