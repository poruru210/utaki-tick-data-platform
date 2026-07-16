package delivery

import (
	"fmt"
	"os"
	"strings"

	"github.com/pelletier/go-toml/v2"

	"tick-data-platform/internal/r2"
)

const ReaderConfigVersion = "tick-reader-v1"

const (
	defaultMaxMetadataBytes  = uint64(1 << 20)
	defaultMaxRawObjectBytes = uint64(8 << 30)
	defaultMaxRemoteObjects  = uint64(1 << 20)
	maxRemoteObjects         = uint64(1 << 20)
)

type ReaderConfig struct {
	Version               string `toml:"reader_config_version"`
	Endpoint              string `toml:"endpoint"`
	Bucket                string `toml:"bucket"`
	CredentialsPath       string `toml:"credentials_path"`
	CredentialsProtection string `toml:"credentials_protection"`
	Region                string `toml:"region"`
	ImmutableRoot         string `toml:"immutable_root"`
	CacheRoot             string `toml:"cache_root"`
	MaxMetadataBytes      uint64 `toml:"max_metadata_bytes"`
	MaxRawObjectBytes     uint64 `toml:"max_raw_object_bytes"`
	MaxRemoteObjects      uint64 `toml:"max_remote_objects"`
}

func LoadReaderConfig(path string) (ReaderConfig, error) {
	if path == "" {
		return ReaderConfig{}, fmt.Errorf("reader config path is empty")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return ReaderConfig{}, fmt.Errorf("read reader config")
	}
	var config ReaderConfig
	decoder := toml.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&config); err != nil {
		return ReaderConfig{}, fmt.Errorf("decode reader config")
	}
	if err := config.Validate(); err != nil {
		return ReaderConfig{}, err
	}
	return config, nil
}

func (c *ReaderConfig) Validate() error {
	if c.Version != ReaderConfigVersion {
		return fmt.Errorf("unsupported reader config version")
	}
	if err := r2.ValidateHTTPSHostEndpoint(c.Endpoint); err != nil {
		return fmt.Errorf("reader %w", err)
	}
	if strings.TrimSpace(c.Bucket) == "" {
		return fmt.Errorf("reader bucket is required")
	}
	if strings.TrimSpace(c.CredentialsPath) == "" {
		return fmt.Errorf("reader credentials_path is required")
	}
	if c.CredentialsProtection != "" && c.CredentialsProtection != "native-acl" && c.CredentialsProtection != "managed-mount" {
		return fmt.Errorf("reader credentials_protection is unsupported")
	}
	if c.Region != "auto" {
		return fmt.Errorf("reader region must be auto")
	}
	if err := validateRemoteRoot(c.ImmutableRoot); err != nil {
		return err
	}
	if c.CacheRoot == "" {
		return fmt.Errorf("reader cache_root is required")
	}
	if c.MaxMetadataBytes == 0 {
		c.MaxMetadataBytes = defaultMaxMetadataBytes
	}
	if c.MaxRawObjectBytes == 0 {
		c.MaxRawObjectBytes = defaultMaxRawObjectBytes
	}
	if c.MaxRemoteObjects == 0 {
		c.MaxRemoteObjects = defaultMaxRemoteObjects
	}
	if c.MaxMetadataBytes > 16<<20 || c.MaxRawObjectBytes > 64<<30 || c.MaxRemoteObjects > maxRemoteObjects {
		return fmt.Errorf("reader size limits are too large")
	}
	return nil
}

func (c ReaderConfig) BucketName() (string, error) {
	if err := c.Validate(); err != nil {
		return "", err
	}
	return c.Bucket, nil
}

func validateRemoteRoot(root string) error {
	if root == "" || strings.HasPrefix(root, "/") || strings.HasPrefix(root, "//") || strings.Contains(root, "//") || strings.ContainsAny(root, "\\\r\n") || strings.HasSuffix(root, "/") {
		return fmt.Errorf("immutable_root is not canonical")
	}
	for _, part := range strings.Split(root, "/") {
		if part == "" || part == "." || part == ".." {
			return fmt.Errorf("immutable_root contains an empty or dot segment")
		}
	}
	if len(root) >= 2 && root[1] == ':' && ((root[0] >= 'A' && root[0] <= 'Z') || (root[0] >= 'a' && root[0] <= 'z')) {
		return fmt.Errorf("immutable_root must not be a drive path")
	}
	return nil
}
