package credentials

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const testAccessKey = "AKIA_TEST_ONLY_ACCESS_KEY"
const testSecretKey = "test-secret-that-must-not-appear"

func TestFileProviderLoad(t *testing.T) {
	tests := []struct {
		name string
		body string
		want Credentials
		err  error
	}{
		{name: "valid", body: `{"format_version":1,"access_key_id":"` + testAccessKey + `","secret_access_key":"` + testSecretKey + `"}`, want: Credentials{AccessKeyID: testAccessKey, SecretAccessKey: testSecretKey}},
		{name: "not object", body: `[]`, err: ErrCredentialMalformed},
		{name: "empty", body: ``, err: ErrCredentialMalformed},
		{name: "malformed JSON", body: `{"format_version":1,"access_key_id":"a",`, err: ErrCredentialMalformed},
		{name: "BOM", body: "\xef\xbb\xbf" + `{"format_version":1,"access_key_id":"a","secret_access_key":"b"}`, err: ErrCredentialMalformed},
		{name: "unknown field", body: `{"format_version":1,"access_key_id":"a","secret_access_key":"b","extra":"c"}`, err: ErrCredentialMalformed},
		{name: "duplicate field", body: `{"format_version":1,"access_key_id":"a","access_key_id":"b","secret_access_key":"c"}`, err: ErrCredentialMalformed},
		{name: "trailing value", body: `{"format_version":1,"access_key_id":"a","secret_access_key":"b"} {}`, err: ErrCredentialMalformed},
		{name: "unsupported version", body: `{"format_version":2,"access_key_id":"a","secret_access_key":"b"}`, err: ErrCredentialVersion},
		{name: "missing access key", body: `{"format_version":1,"secret_access_key":"b"}`, err: ErrCredentialIncomplete},
		{name: "missing secret", body: `{"format_version":1,"access_key_id":"a"}`, err: ErrCredentialIncomplete},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := writeCredentialFixture(t, tt.body)
			provider, err := NewFileProvider(FileConfig{Path: path})
			if err != nil {
				t.Fatal(err)
			}
			got, err := provider.Load(context.Background())
			if tt.err != nil {
				if !errors.Is(err, tt.err) {
					t.Fatalf("error = %v, want %v", err, tt.err)
				}
				var classified *CredentialError
				if !errors.As(err, &classified) {
					t.Fatalf("error = %T, want CredentialError", err)
				}
				return
			}
			if err != nil || got != tt.want {
				t.Fatalf("got %#v, error %v; want %#v", got, err, tt.want)
			}
		})
	}
}

func TestCredentialErrorPreservesIsAndAsClassification(t *testing.T) {
	path := writeCredentialFixture(t, `{"format_version":1,"access_key_id":"a"}`)
	provider, err := NewFileProvider(FileConfig{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	got, err := provider.Load(context.Background())
	if got != (Credentials{}) {
		t.Fatalf("incomplete credentials = %+v", got)
	}
	if !errors.Is(err, ErrCredentialIncomplete) {
		t.Fatalf("errors.Is = false for %v", err)
	}
	var classified *CredentialError
	if !errors.As(err, &classified) || classified.Kind != ErrCredentialIncomplete {
		t.Fatalf("classified error = %#v", classified)
	}
}

func TestFileProviderAcceptsExactlyMaxBundleBytes(t *testing.T) {
	body := `{"format_version":1,"access_key_id":"a","secret_access_key":"b"}`
	body += strings.Repeat(" ", int(MaxBundleBytes)-len(body))
	if int64(len(body)) != MaxBundleBytes {
		t.Fatalf("fixture size = %d, want %d", len(body), MaxBundleBytes)
	}
	path := writeCredentialFixture(t, body)
	provider, err := NewFileProvider(FileConfig{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Load(context.Background()); err != nil {
		t.Fatalf("exact-size bundle rejected: %v", err)
	}
}

func TestFileProviderRejectsDirectory(t *testing.T) {
	root := t.TempDir()
	provider, err := NewFileProvider(FileConfig{Path: root})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Load(context.Background()); !errors.Is(err, ErrCredentialFileUnsafe) {
		t.Fatalf("directory error = %v", err)
	}
}

func TestFileProviderUsesSecurityValidatorSeam(t *testing.T) {
	path := writeCredentialFixture(t, `{"format_version":1,"access_key_id":"a","secret_access_key":"b"}`)
	provider, err := NewFileProvider(FileConfig{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	provider.securityValidator = func(string, *os.File, ProtectionMode) error {
		return fmt.Errorf("%w: test validator", ErrCredentialFileUnsafe)
	}
	if _, err := provider.Load(context.Background()); !errors.Is(err, ErrCredentialFileUnsafe) {
		t.Fatalf("validator seam error = %v", err)
	}
}

func TestFileProviderLimitsAndContext(t *testing.T) {
	path := writeCredentialFixture(t, `{"format_version":1,"access_key_id":"a","secret_access_key":"b"}`)
	provider, err := NewFileProvider(FileConfig{Path: path, Protection: ProtectionNativeACL})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Load(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := provider.Load(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled context error = %v", err)
	}

	tooLarge := strings.Repeat(" ", int(MaxBundleBytes)+1)
	if err := os.WriteFile(path, []byte(tooLarge), 0o600); err != nil {
		t.Fatal(err)
	}
	_, loadErr := provider.Load(context.Background())
	if !errors.Is(loadErr, ErrCredentialTooLarge) {
		t.Fatalf("oversized error = %v", loadErr)
	}
	var classified *CredentialError
	if !errors.As(loadErr, &classified) || classified.Kind != ErrCredentialTooLarge {
		t.Fatalf("oversized classified error = %#v", classified)
	}
}

func TestFileProviderRejectsUnsafeModeAndPath(t *testing.T) {
	if _, err := NewFileProvider(FileConfig{}); !errors.Is(err, ErrCredentialPathRequired) {
		t.Fatalf("empty path error = %v", err)
	}
	if _, err := NewFileProvider(FileConfig{Path: "x", Protection: "unknown"}); !errors.Is(err, ErrCredentialFileUnsafe) {
		t.Fatalf("unknown protection error = %v", err)
	}
	provider, err := NewFileProvider(FileConfig{Path: filepath.Join(t.TempDir(), "missing")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Load(context.Background()); !errors.Is(err, ErrCredentialFileUnsafe) {
		t.Fatalf("missing file error = %v", err)
	}
}

func TestCredentialsFormattingIsRedacted(t *testing.T) {
	value := Credentials{AccessKeyID: testAccessKey, SecretAccessKey: testSecretKey}
	for _, format := range []string{"%v", "%+v", "%#v", "%s", "%q"} {
		got := fmt.Sprintf(format, value)
		if strings.Contains(got, testAccessKey) || strings.Contains(got, testSecretKey) {
			t.Fatalf("format %q leaked credential: %q", format, got)
		}
	}
}

func writeCredentialFixture(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "r2-writer.json")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	secureCredentialFixtureForTest(t, path)
	return path
}
