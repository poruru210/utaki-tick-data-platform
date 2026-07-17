package httpapi

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/pelletier/go-toml/v2"
	"tick-data-platform/internal/operations"
)

const APIConfigVersion = "tick-api-v1"

// Config contains only HTTP process policy. R2 credentials remain in the
// delivery ReaderConfig and are loaded through environment names; this type
// never contains credential values or a write-capable backend.
type Config struct {
	Version       string `toml:"api_config_version"`
	ListenAddress string `toml:"listen_address"`
	ReaderConfig  string `toml:"reader_config"`
	CacheControl  string `toml:"cache_control"`
	Limits        operations.ResourceLimits

	Authenticate         AuthenticateFunc         `toml:"-"`
	RateLimit            RateLimitFunc            `toml:"-"`
	TrustedProxy         TrustedProxyFunc         `toml:"-"`
	ShortLivedCredential ShortLivedCredentialFunc `toml:"-"`
	Health               HealthFunc               `toml:"-"`
}

func (c Config) withDefaults() Config {
	if c.ListenAddress == "" {
		c.ListenAddress = "127.0.0.1:17002"
	}
	if c.CacheControl == "" {
		c.CacheControl = "no-store"
	}
	if c.Limits == (operations.ResourceLimits{}) {
		c.Limits = operations.DefaultResourceLimits
	}
	return c
}

func (c Config) Validate() error {
	c = c.withDefaults()
	if c.Version != APIConfigVersion {
		return fmt.Errorf("unsupported API config version")
	}
	if c.ReaderConfig == "" {
		return fmt.Errorf("reader config is required")
	}
	if c.CacheControl != "no-store" {
		return fmt.Errorf("cache control must be no-store")
	}
	host, portText, err := net.SplitHostPort(strings.TrimSpace(c.ListenAddress))
	if err != nil || host == "" {
		return fmt.Errorf("listen address is invalid")
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 1 || port > 65535 {
		return fmt.Errorf("listen address port is invalid")
	}
	host = strings.ToLower(strings.TrimSuffix(host, "."))
	isLoopback := false
	if host == "localhost" {
		isLoopback = true
	} else if ip := net.ParseIP(host); ip != nil {
		isLoopback = ip.IsLoopback()
	}
	if !isLoopback && (c.Authenticate == nil || c.RateLimit == nil || c.TrustedProxy == nil || c.ShortLivedCredential == nil) {
		return fmt.Errorf("non-loopback API bind requires authentication, rate limit, trusted proxy, and short-lived credential policy")
	}
	if err := c.Limits.Validate(); err != nil {
		return err
	}
	return nil
}

func LoadConfig(path string) (Config, error) {
	if path == "" {
		return Config{}, fmt.Errorf("API config path is empty")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read API config")
	}
	var config Config
	decoder := toml.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&config); err != nil {
		return Config{}, fmt.Errorf("decode API config")
	}
	config = config.withDefaults()
	if err := config.Validate(); err != nil {
		return Config{}, err
	}
	return config, nil
}

// AuthenticateFunc is intentionally an adapter hook. Production non-loopback
// profiles must provide an explicit implementation; the loopback profile may
// leave it nil.
type AuthenticateFunc func(*http.Request) error

// RateLimitFunc must enforce the deployment's request-rate policy and return
// a release function for any acquired slot. It is a real policy hook rather
// than a configuration attestation boolean.
type RateLimitFunc func(*http.Request) (release func(), err error)

type TrustedProxyFunc func(*http.Request) error

type ShortLivedCredentialFunc func(*http.Request) error

type HealthSnapshot struct {
	Status   string `json:"status"`
	ReadOnly bool   `json:"read_only"`
}

type HealthFunc func(context.Context) (HealthSnapshot, error)

func defaultHealth(context.Context) (HealthSnapshot, error) {
	return HealthSnapshot{Status: "ok", ReadOnly: true}, nil
}
