package retention

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const validRetentionConfig = `retention_config_version = "tick-retention-v1"
endpoint = "https://0123456789abcdef0123456789abcdef.r2.cloudflarestorage.com"
bucket = "tick-retention-test"
credentials_path = "./credentials.json"
credentials_protection = "managed-mount"
region = "auto"
immutable_root = "v1"
dataset_id = "dataset"
provider_id = "provider"
stable_feed_id = "feed"
exact_source_symbol = "EURUSD"
broker_server_fingerprint = "broker"
gateway_build_identity = "gateway"
producer_build_identity = "producer"
day_definition_id = "utc-day-v1"
settle_policy = "manual-v1"
publisher_id = "publisher"
publisher_epoch = 1
max_frame_bytes = 1048576
max_records = 4096
max_string_bytes = 255
date = "2024-03-09"
grace_ms = 1000
`

func TestRetentionConfigStrictlyLoadsScope(t *testing.T) {
	path := filepath.Join(t.TempDir(), "retention.toml")
	if err := os.WriteFile(path, []byte(validRetentionConfig), 0o600); err != nil {
		t.Fatal(err)
	}
	config, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	scope, err := config.Scope()
	if err != nil || scope.DatasetID != "dataset" || scope.PublisherEpoch != 1 {
		t.Fatalf("scope=%+v err=%v", scope, err)
	}
}

func TestRetentionConfigRejectsUnknownFieldAndInvalidDate(t *testing.T) {
	for name, content := range map[string]string{
		"unknown":     validRetentionConfig + "unknown = 1\n",
		"date":        strings.Replace(validRetentionConfig, `date = "2024-03-09"`, `date = "2024-02-30"`, 1),
		"date-format": strings.Replace(validRetentionConfig, `date = "2024-03-09"`, `date = "2024-3-09"`, 1),
	} {
		path := filepath.Join(t.TempDir(), name+".toml")
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadConfig(path); err == nil {
			t.Fatalf("%s config unexpectedly loaded", name)
		}
	}
}
