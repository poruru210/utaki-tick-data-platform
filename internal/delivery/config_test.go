package delivery

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadReaderConfigIsVersionedStrictAndDoesNotEchoSecrets(t *testing.T) {
	secret := "reader-secret-sentinel"
	t.Setenv("TICK_READER_TEST_SECRET", secret)
	path := filepath.Join(t.TempDir(), "reader.toml")
	content := strings.Join([]string{
		`reader_config_version = "tick-reader-v1"`,
		`endpoint = "https://reader.invalid"`,
		`bucket_env = "TICK_READER_TEST_BUCKET"`,
		`access_key_env = "TICK_READER_TEST_ACCESS"`,
		`secret_key_env = "TICK_READER_TEST_SECRET"`,
		`region = "auto"`,
		`immutable_root = "v1"`,
		`cache_root = "./cache"`,
		`max_metadata_bytes = 1048576`,
		`max_raw_object_bytes = 8589934592`,
		`unknown = "fail-closed"`,
	}, "\n")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadReaderConfig(path); err == nil || strings.Contains(err.Error(), secret) {
		t.Fatalf("strict config result = %v", err)
	}
}

func TestReaderConfigRejectsNonCanonicalRoots(t *testing.T) {
	config := testReaderConfig(t.TempDir())
	for _, root := range []string{"v1//campaign", "../v1", "C:/v1", `\\server\share`} {
		config.ImmutableRoot = root
		if err := config.Validate(); err == nil {
			t.Fatalf("unsafe reader root %q was accepted", root)
		}
	}
}
