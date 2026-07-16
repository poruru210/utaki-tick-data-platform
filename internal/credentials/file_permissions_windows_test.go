//go:build windows

package credentials

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/windows"
)

func TestFileProviderWindowsNativeACLFixture(t *testing.T) {
	path := filepath.Join(t.TempDir(), "r2-credentials.json")
	if err := os.WriteFile(path, []byte(`{"format_version":1,"access_key_id":"a","secret_access_key":"b"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	provider, err := NewFileProvider(FileConfig{Path: path, Protection: ProtectionNativeACL})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Load(context.Background()); err != nil {
		t.Fatalf("native ACL fixture should be accepted for the current service identity: %v", err)
	}
}

func TestFileProviderWindowsRejectsWorldReadableACL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "r2-world-readable.json")
	if err := os.WriteFile(path, []byte(`{"format_version":1,"access_key_id":"a","secret_access_key":"b"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	descriptor, err := windows.SecurityDescriptorFromString("D:(A;;GR;;;WD)")
	if err != nil {
		t.Fatal(err)
	}
	dacl, _, err := descriptor.DACL()
	if err != nil {
		t.Fatal(err)
	}
	if err := windows.SetNamedSecurityInfo(path, windows.SE_FILE_OBJECT, windows.DACL_SECURITY_INFORMATION|windows.PROTECTED_DACL_SECURITY_INFORMATION, nil, nil, dacl, nil); err != nil {
		t.Fatalf("set broad-read ACL fixture: %v", err)
	}
	provider, err := NewFileProvider(FileConfig{Path: path, Protection: ProtectionNativeACL})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Load(context.Background()); !errors.Is(err, ErrCredentialFileUnsafe) {
		t.Fatalf("world-readable ACL error = %v", err)
	}
}
