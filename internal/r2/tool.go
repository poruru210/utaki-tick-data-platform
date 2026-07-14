package r2

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"
	"strings"

	"github.com/pelletier/go-toml/v2"
)

const RcloneVersion = "v1.74.4"

var (
	ErrRcloneBinary  = errors.New("rclone binary integrity failure")
	ErrRcloneCommand = errors.New("rclone command failed")
	ErrRcloneVersion = errors.New("rclone version mismatch")
)

type RcloneTool struct {
	GOOS           string `toml:"goos"`
	GOARCH         string `toml:"goarch"`
	ArchiveURL     string `toml:"archive_url"`
	ArchiveSHA256  string `toml:"archive_sha256"`
	BinarySHA256   string `toml:"binary_sha256"`
	BinaryBytes    uint64 `toml:"binary_bytes"`
	ExecutableName string `toml:"executable_name"`
}

type ToolLock struct {
	LockVersion   string       `toml:"lock_version"`
	RcloneVersion string       `toml:"rclone_version"`
	Rclone        []RcloneTool `toml:"rclone"`
}

func LoadToolLock(path string) (ToolLock, error) {
	if path == "" {
		return ToolLock{}, fmt.Errorf("tool lock path is empty")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return ToolLock{}, fmt.Errorf("read tool lock: %w", err)
	}
	var lock ToolLock
	decoder := toml.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&lock); err != nil {
		return ToolLock{}, fmt.Errorf("decode tool lock: %w", err)
	}
	if err := lock.Validate(); err != nil {
		return ToolLock{}, err
	}
	return lock, nil
}

func (lock ToolLock) Validate() error {
	if lock.LockVersion != "tick-data-tools-lock-v1" || lock.RcloneVersion != RcloneVersion {
		return fmt.Errorf("unsupported rclone tool lock")
	}
	if len(lock.Rclone) != 3 {
		return fmt.Errorf("rclone tool lock must contain the three supported platforms")
	}
	seen := make(map[string]struct{}, len(lock.Rclone))
	for _, tool := range lock.Rclone {
		if tool.GOOS == "" || tool.GOARCH == "" {
			return fmt.Errorf("rclone tool lock has an empty platform")
		}
		platform := tool.GOOS + "/" + tool.GOARCH
		if _, ok := seen[platform]; ok {
			return fmt.Errorf("rclone tool lock duplicates %s", platform)
		}
		seen[platform] = struct{}{}
		wantURL, ok := map[string]string{
			"windows/amd64": "https://downloads.rclone.org/v1.74.4/rclone-v1.74.4-windows-amd64.zip",
			"linux/amd64":   "https://downloads.rclone.org/v1.74.4/rclone-v1.74.4-linux-amd64.zip",
			"linux/arm64":   "https://downloads.rclone.org/v1.74.4/rclone-v1.74.4-linux-arm64.zip",
		}[platform]
		if !ok || tool.ArchiveURL != wantURL {
			return fmt.Errorf("rclone tool lock has an unsupported platform or URL: %s", platform)
		}
		wantMetadata := map[string]struct {
			archiveSHA string
			binarySHA  string
			bytes      uint64
		}{
			"windows/amd64": {"ef097ef9de37a57feb7d9f9c7afb34148ad3c65be8025f1d8f7f521554a701ea", "492648a3867dbc620188a305e05ff3216aecbf4622bf1a6b5b978ed9c939e18c", 78797824},
			"linux/amd64":   {"fe435e0c36228e7c2f116a8701f01127bb1f694005fc11d1f27186c8bca4115d", "9f56ca5edfac24a3ed37226c2ba1de69f1ec9e05fa2526cddee5cd97e202be6b", 79036578},
			"linux/arm64":   {"97685285c9ad6a0cf17d5844115d2a67245af6444db672187074bd9c358de419", "e062d30596c386046c8471f3035611d0438c22ef5fa42d3d6128dbf48ed5c76c", 72482978},
		}[platform]
		if tool.ArchiveSHA256 != wantMetadata.archiveSHA || tool.BinarySHA256 != wantMetadata.binarySHA || tool.BinaryBytes != wantMetadata.bytes {
			return fmt.Errorf("rclone tool lock has incorrect binary metadata for %s", platform)
		}
		if platform == "windows/amd64" && tool.ExecutableName != "rclone.exe" {
			return fmt.Errorf("rclone tool lock has invalid Windows executable name")
		}
		if strings.HasPrefix(platform, "linux/") && tool.ExecutableName != "rclone" {
			return fmt.Errorf("rclone tool lock has invalid Linux executable name")
		}
	}
	for _, platform := range []string{"windows/amd64", "linux/amd64", "linux/arm64"} {
		if _, ok := seen[platform]; !ok {
			return fmt.Errorf("rclone tool lock is missing %s", platform)
		}
	}
	return nil
}

func (lock ToolLock) Select(goos, goarch string) (RcloneTool, error) {
	if err := lock.Validate(); err != nil {
		return RcloneTool{}, err
	}
	for _, tool := range lock.Rclone {
		if tool.GOOS == goos && tool.GOARCH == goarch {
			return tool, nil
		}
	}
	return RcloneTool{}, fmt.Errorf("rclone is not pinned for %s/%s", goos, goarch)
}

func (lock ToolLock) Current() (RcloneTool, error) {
	return lock.Select(runtime.GOOS, runtime.GOARCH)
}

func VerifyRcloneBinary(path string, tool RcloneTool) error {
	if path == "" {
		return fmt.Errorf("%w: binary path is empty", ErrRcloneBinary)
	}
	stat, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("%w: binary is unavailable", ErrRcloneBinary)
	}
	if !stat.Mode().IsRegular() || uint64(stat.Size()) != tool.BinaryBytes {
		return fmt.Errorf("%w: binary size does not match the lock", ErrRcloneBinary)
	}
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("%w: binary cannot be opened", ErrRcloneBinary)
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return fmt.Errorf("%w: binary cannot be hashed", ErrRcloneBinary)
	}
	if hex.EncodeToString(hash.Sum(nil)) != tool.BinarySHA256 {
		return fmt.Errorf("%w: binary SHA-256 does not match the lock", ErrRcloneBinary)
	}
	return nil
}

type commandExecutor interface {
	run(ctx context.Context, executable string, args ...string) (string, error)
}

// RcloneExecutorFunc is an in-process command seam for network-free integration tests.
// Production callers should use NewRcloneRunner or NewRcloneRunnerForPlatform.
type RcloneExecutorFunc func(ctx context.Context, executable string, args ...string) (string, error)

func (f RcloneExecutorFunc) run(ctx context.Context, executable string, args ...string) (string, error) {
	return f(ctx, executable, args...)
}

// NewRcloneRunnerWithExecutor creates a runner with a test-controlled command
// executor while retaining the same binary and version checks as production.
func NewRcloneRunnerWithExecutor(binaryPath string, tool RcloneTool, executor RcloneExecutorFunc) (*RcloneRunner, error) {
	if binaryPath == "" || executor == nil {
		return nil, fmt.Errorf("rclone test runner dependencies are incomplete")
	}
	return &RcloneRunner{binaryPath: binaryPath, tool: tool, executor: executor}, nil
}

type osCommandExecutor struct{}

func (osCommandExecutor) run(ctx context.Context, executable string, args ...string) (string, error) {
	command := exec.CommandContext(ctx, executable, args...)
	output, err := command.Output()
	if err != nil {
		return "", ErrRcloneCommand
	}
	return string(output), nil
}

type RcloneRunner struct {
	binaryPath string
	tool       RcloneTool
	executor   commandExecutor
}

func (r *RcloneRunner) Tool() RcloneTool {
	return r.tool
}

func NewRcloneRunner(lock ToolLock, binaryPath string) (*RcloneRunner, error) {
	tool, err := lock.Current()
	if err != nil {
		return nil, err
	}
	return &RcloneRunner{binaryPath: binaryPath, tool: tool, executor: osCommandExecutor{}}, nil
}

func NewRcloneRunnerForPlatform(lock ToolLock, binaryPath, goos, goarch string) (*RcloneRunner, error) {
	tool, err := lock.Select(goos, goarch)
	if err != nil {
		return nil, err
	}
	return &RcloneRunner{binaryPath: binaryPath, tool: tool, executor: osCommandExecutor{}}, nil
}

func (r *RcloneRunner) Version(ctx context.Context) error {
	_, err := r.runVerified(ctx)
	return err
}

func (r *RcloneRunner) CopyToImmutable(ctx context.Context, localPath, remoteKey string) error {
	if localPath == "" || remoteKey == "" {
		return fmt.Errorf("%w: copyto paths are empty", ErrRcloneCommand)
	}
	if _, err := r.runVerified(ctx); err != nil {
		return err
	}
	_, err := r.executor.run(ctx, r.binaryPath, "copyto", "--immutable", localPath, remoteKey)
	return err
}

func (r *RcloneRunner) CheckDownload(ctx context.Context, localPath, remoteKey string) error {
	if localPath == "" || remoteKey == "" {
		return fmt.Errorf("%w: check paths are empty", ErrRcloneCommand)
	}
	if _, err := r.runVerified(ctx); err != nil {
		return err
	}
	_, err := r.executor.run(ctx, r.binaryPath, "check", "--download", localPath, remoteKey)
	return err
}

func (r *RcloneRunner) runVerified(ctx context.Context) (string, error) {
	if err := VerifyRcloneBinary(r.binaryPath, r.tool); err != nil {
		return "", err
	}
	output, err := r.executor.run(ctx, r.binaryPath, "version")
	if err != nil {
		return "", err
	}
	firstLine := strings.TrimSpace(strings.SplitN(output, "\n", 2)[0])
	if firstLine != "rclone "+RcloneVersion {
		return "", ErrRcloneVersion
	}
	return output, nil
}
