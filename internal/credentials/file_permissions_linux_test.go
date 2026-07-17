//go:build linux

package credentials

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestFileProviderLinuxNativePermissions(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "r2-writer.json")
	if err := os.WriteFile(path, []byte(`{"format_version":1,"access_key_id":"a","secret_access_key":"b"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	provider, err := NewFileProvider(FileConfig{Path: path, Protection: ProtectionNativeACL})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Load(context.Background()); err != nil {
		t.Fatalf("0600 should be accepted: %v", err)
	}
	if err := os.Chmod(path, 0o640); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Load(context.Background()); !errors.Is(err, ErrCredentialFileUnsafe) {
		t.Fatalf("group-readable file error = %v", err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Load(context.Background()); !errors.Is(err, ErrCredentialFileUnsafe) {
		t.Fatalf("owner-executable file error = %v", err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(root, 0o777); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Load(context.Background()); !errors.Is(err, ErrCredentialFileUnsafe) {
		t.Fatalf("world-writable parent error = %v", err)
	}
	if err := os.Chmod(root, 0o1777); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Load(context.Background()); !errors.Is(err, ErrCredentialFileUnsafe) {
		t.Fatalf("sticky world-writable parent error = %v", err)
	}
}

func TestFileProviderLinuxManagedMountDoesNotApplyNativeMode(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "r2-writer")
	if err := os.WriteFile(path, []byte(`{"format_version":1,"access_key_id":"a","secret_access_key":"b"}`), 0o400); err != nil {
		t.Fatal(err)
	}
	provider, err := NewFileProvider(FileConfig{Path: path, Protection: ProtectionManagedMount})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Load(context.Background()); err != nil {
		t.Fatalf("managed mount should not require native mode: %v", err)
	}
}

func TestFileProviderLinuxNativeRejectsSymlink(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "target.json")
	link := filepath.Join(root, "link.json")
	if err := os.WriteFile(target, []byte(`{"format_version":1,"access_key_id":"a","secret_access_key":"b"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	provider, err := NewFileProvider(FileConfig{Path: link, Protection: ProtectionNativeACL})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Load(context.Background()); !errors.Is(err, ErrCredentialFileUnsafe) {
		t.Fatalf("symlink error = %v", err)
	}
}
