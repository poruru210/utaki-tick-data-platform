package retention

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/pelletier/go-toml/v2"

	"tick-data-platform/internal/archive"
	"tick-data-platform/internal/protocol"
	"tick-data-platform/internal/r2"
)

const ConfigVersion = "tick-retention-v1"

// Config is the strict, secret-free configuration needed by the operator
// prune command. Credential values are read only from the named environment
// variables by the read-only R2 backend.
type Config struct {
	Version       string `toml:"retention_config_version"`
	Endpoint      string `toml:"endpoint"`
	BucketEnv     string `toml:"bucket_env"`
	AccessKeyEnv  string `toml:"access_key_env"`
	SecretKeyEnv  string `toml:"secret_key_env"`
	Region        string `toml:"region"`
	ImmutableRoot string `toml:"immutable_root"`

	DatasetID               string `toml:"dataset_id"`
	CampaignID              string `toml:"campaign_id"`
	ProviderID              string `toml:"provider_id"`
	StableFeedID            string `toml:"stable_feed_id"`
	ExactSourceSymbol       string `toml:"exact_source_symbol"`
	BrokerServerFingerprint string `toml:"broker_server_fingerprint"`
	GatewayBuildIdentity    string `toml:"gateway_build_identity"`
	ProducerBuildIdentity   string `toml:"producer_build_identity"`
	DayDefinitionID         string `toml:"day_definition_id"`
	SettlePolicy            string `toml:"settle_policy"`
	PublisherID             string `toml:"publisher_id"`
	PublisherEpoch          uint64 `toml:"publisher_epoch"`
	MaxFrameBytes           uint32 `toml:"max_frame_bytes"`
	MaxRecords              uint32 `toml:"max_records"`
	MaxStringBytes          uint16 `toml:"max_string_bytes"`

	Date    string `toml:"date"`
	GraceMS uint64 `toml:"grace_ms"`
}

func LoadConfig(path string) (Config, error) {
	if strings.TrimSpace(path) == "" {
		return Config{}, fmt.Errorf("retention config path is empty")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read retention config")
	}
	var config Config
	decoder := toml.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&config); err != nil {
		return Config{}, fmt.Errorf("decode retention config")
	}
	if err := config.Validate(); err != nil {
		return Config{}, err
	}
	return config, nil
}

func (c Config) Validate() error {
	if c.Version != ConfigVersion {
		return fmt.Errorf("unsupported retention config version")
	}
	if err := r2.ValidateHTTPSHostEndpoint(c.Endpoint); err != nil {
		return fmt.Errorf("retention %w", err)
	}
	for name, value := range map[string]string{
		"bucket_env": c.BucketEnv, "access_key_env": c.AccessKeyEnv, "secret_key_env": c.SecretKeyEnv,
	} {
		if !validEnvironmentName(value) {
			return fmt.Errorf("%s is not a valid environment variable name", name)
		}
	}
	if c.Region != "auto" {
		return fmt.Errorf("retention region must be auto")
	}
	if c.ImmutableRoot == "" || strings.HasPrefix(c.ImmutableRoot, "/") || strings.Contains(c.ImmutableRoot, "//") || strings.ContainsAny(c.ImmutableRoot, "\\\r\n") || strings.HasSuffix(c.ImmutableRoot, "/") {
		return fmt.Errorf("retention immutable_root is not canonical")
	}
	for _, part := range strings.Split(c.ImmutableRoot, "/") {
		if part == "" || part == "." || part == ".." {
			return fmt.Errorf("retention immutable_root contains an empty or dot segment")
		}
	}
	if c.PublisherEpoch == 0 || c.GraceMS == 0 {
		return fmt.Errorf("publisher_epoch and grace_ms are required")
	}
	parsedDate, err := time.Parse("2006-01-02", c.Date)
	if err != nil || parsedDate.Format("2006-01-02") != c.Date {
		return fmt.Errorf("retention date must be YYYY-MM-DD")
	}
	scope, err := c.Scope()
	if err != nil {
		return err
	}
	if _, err := r2.NewLayout(c.ImmutableRoot, "", scope); err != nil {
		return fmt.Errorf("retention layout is invalid")
	}
	return nil
}

func (c Config) Scope() (archive.ScopeConfig, error) {
	scope := archive.ScopeConfig{
		DatasetID: c.DatasetID, CampaignID: c.CampaignID, ProviderID: c.ProviderID,
		StableFeedID: c.StableFeedID, ExactSourceSymbol: c.ExactSourceSymbol,
		BrokerServerFingerprint: c.BrokerServerFingerprint, GatewayBuildIdentity: c.GatewayBuildIdentity,
		ProducerBuildIdentity: c.ProducerBuildIdentity, DayDefinitionID: c.DayDefinitionID,
		SettlePolicy: c.SettlePolicy, PublisherID: c.PublisherID, PublisherEpoch: c.PublisherEpoch,
		ProtocolVersion: protocol.ProtocolVersion,
		ProtocolLimits:  archive.ProtocolLimits{MaxFrameBytes: c.MaxFrameBytes, MaxRecords: c.MaxRecords, MaxStringBytes: c.MaxStringBytes},
	}
	if _, err := scope.ConfigHash(); err != nil {
		return archive.ScopeConfig{}, fmt.Errorf("retention scope is invalid")
	}
	return scope, nil
}

func validEnvironmentName(value string) bool {
	if value == "" || strings.ContainsAny(value, "=\x00\r\n") {
		return false
	}
	for index, char := range value {
		if (char < 'A' || char > 'Z') && (char < 'a' || char > 'z') && (index == 0 || char < '0' || char > '9') && char != '_' {
			return false
		}
	}
	return true
}
