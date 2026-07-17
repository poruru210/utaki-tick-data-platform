package testsupport

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadEnvFileParsesSupportedLines(t *testing.T) {
	path := filepath.Join(t.TempDir(), "env.local")
	keys := []string{"TICK_TESTSUPPORT_PLAIN", "TICK_TESTSUPPORT_SINGLE", "TICK_TESTSUPPORT_DOUBLE", "TICK_TESTSUPPORT_EMPTY"}
	for _, key := range keys {
		old, existed := os.LookupEnv(key)
		_ = os.Unsetenv(key)
		t.Cleanup(func() {
			if existed {
				_ = os.Setenv(key, old)
			} else {
				_ = os.Unsetenv(key)
			}
		})
	}
	if err := os.WriteFile(path, []byte("# comment\nTICK_TESTSUPPORT_PLAIN=value\nexport TICK_TESTSUPPORT_SINGLE='one two'\nTICK_TESTSUPPORT_DOUBLE=\"line\\nvalue\"\nTICK_TESTSUPPORT_EMPTY=\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := loadEnvFile(path); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		key  string
		want string
	}{
		{key: "TICK_TESTSUPPORT_PLAIN", want: "value"},
		{key: "TICK_TESTSUPPORT_SINGLE", want: "one two"},
		{key: "TICK_TESTSUPPORT_DOUBLE", want: "line\nvalue"},
		{key: "TICK_TESTSUPPORT_EMPTY", want: ""},
	} {
		if got := os.Getenv(test.key); got != test.want {
			t.Fatalf("%s = %q, want %q", test.key, got, test.want)
		}
	}
}

func TestLoadEnvFileDoesNotOverrideExistingEnvironment(t *testing.T) {
	path := filepath.Join(t.TempDir(), "env.local")
	if err := os.WriteFile(path, []byte("EXISTING=from-file\nNEW=from-file\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("EXISTING", "from-process")
	t.Setenv("NEW", "")

	if err := loadEnvFile(path); err != nil {
		t.Fatal(err)
	}
	if got := os.Getenv("EXISTING"); got != "from-process" {
		t.Fatalf("EXISTING = %q, want from-process", got)
	}
	if got := os.Getenv("NEW"); got != "" {
		t.Fatalf("NEW = %q, want existing empty value", got)
	}
}

func TestParseEnvLineRejectsMalformedInput(t *testing.T) {
	for _, line := range []string{"NO_SEPARATOR", "1INVALID=value", "BROKEN='value"} {
		if _, _, _, err := parseEnvLine(line); err == nil {
			t.Fatalf("parseEnvLine(%q) succeeded, want error", line)
		}
	}
}
