package r2

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

type recordingExecutor struct {
	calls  [][]string
	output string
	err    error
}

func (e *recordingExecutor) run(_ context.Context, executable string, args ...string) (string, error) {
	call := append([]string{executable}, args...)
	e.calls = append(e.calls, call)
	if e.err != nil {
		return "", e.err
	}
	return e.output, nil
}

func TestRcloneRunnerUsesOnlyPinnedArguments(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rclone.exe")
	data := []byte("fake-rclone")
	if err := os.WriteFile(path, data, 0o700); err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(data)
	executor := &recordingExecutor{output: "rclone v1.74.4\n"}
	runner := &RcloneRunner{
		binaryPath: path,
		tool: RcloneTool{
			GOOS: "windows", GOARCH: "amd64", BinaryBytes: uint64(len(data)), BinarySHA256: fmt.Sprintf("%x", hash),
		},
		executor: executor,
	}
	ctx := context.Background()
	if err := runner.CopyToImmutable(ctx, "local.bin", "r2:remote/key"); err != nil {
		t.Fatal(err)
	}
	if err := runner.CheckDownload(ctx, "local.bin", "r2:remote/key"); err != nil {
		t.Fatal(err)
	}
	want := [][]string{
		{path, "version"},
		{path, "copyto", "--immutable", "local.bin", "r2:remote/key"},
		{path, "version"},
		{path, "check", "--download", "local.bin", "r2:remote/key"},
	}
	if !reflect.DeepEqual(executor.calls, want) {
		t.Fatalf("rclone argv = %#v, want %#v", executor.calls, want)
	}
	for _, call := range executor.calls {
		if strings.Contains(strings.Join(call, " "), "secret") {
			t.Fatal("secret leaked into rclone argv")
		}
	}
}

func TestRcloneRunnerRejectsTamperedBinaryAndVersion(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rclone")
	data := []byte("fake-rclone")
	if err := os.WriteFile(path, data, 0o700); err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(data)
	tool := RcloneTool{GOOS: "linux", GOARCH: "amd64", BinaryBytes: uint64(len(data)), BinarySHA256: fmt.Sprintf("%x", hash)}
	executor := &recordingExecutor{output: "rclone v0.1.0\n"}
	runner := &RcloneRunner{binaryPath: path, tool: tool, executor: executor}
	if err := runner.Version(context.Background()); !errors.Is(err, ErrRcloneVersion) {
		t.Fatalf("version error = %v, want ErrRcloneVersion", err)
	}
	if err := os.WriteFile(path, []byte("tampered"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := runner.Version(context.Background()); !errors.Is(err, ErrRcloneBinary) {
		t.Fatalf("tampered error = %v, want ErrRcloneBinary", err)
	}
}

func TestLoadToolLockRejectsUnknownField(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tools.toml")
	if err := os.WriteFile(path, []byte("lock_version=\"tick-data-tools-lock-v1\"\nrclone_version=\"v1.74.4\"\nunknown=1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadToolLock(path); err == nil {
		t.Fatal("unknown TOML field was accepted")
	}
}

func TestLoadPinnedToolLock(t *testing.T) {
	lock, err := LoadToolLock(filepath.Join("..", "..", "tools", "tick-data-tools.lock.toml"))
	if err != nil {
		t.Fatal(err)
	}
	if tool, err := lock.Select("windows", "amd64"); err != nil || tool.BinaryBytes != 78797824 {
		t.Fatalf("windows lock entry = %+v, err=%v", tool, err)
	}
}
