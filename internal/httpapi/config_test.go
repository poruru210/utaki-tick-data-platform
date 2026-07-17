package httpapi

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadConfigRequiresVersionAndRejectsUnknownFields(t *testing.T) {
	valid := strings.Join([]string{
		`api_config_version = "tick-api-v1"`,
		`listen_address = "127.0.0.1:17002"`,
		`reader_config = "./tick-reader.toml"`,
		`cache_control = "no-store"`,
	}, "\n")
	path := filepath.Join(t.TempDir(), "tick-api.toml")
	if err := os.WriteFile(path, []byte(valid), 0o600); err != nil {
		t.Fatal(err)
	}
	config, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if config.Version != APIConfigVersion || config.CacheControl != "no-store" {
		t.Fatalf("loaded config = %+v", config)
	}

	for _, content := range []string{
		strings.Replace(valid, `api_config_version = "tick-api-v1"`, `api_config_version = "future-v1"`, 1),
		valid + `
unknown = "reject"`,
	} {
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadConfig(path); err == nil {
			t.Fatalf("invalid API config was accepted: %s", content)
		}
	}
}
