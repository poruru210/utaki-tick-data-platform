// Package config owns the strict, secret-free application configuration file.
package config

import (
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/pelletier/go-toml/v2"
)

const CredentialsPathEnv = "TICK_R2_CREDENTIALS_FILE"

type CredentialsConfig struct {
	Provider   string `toml:"provider"`
	Path       string `toml:"path"`
	Protection string `toml:"protection"`
}

type R2Config struct {
	Endpoint      string `toml:"endpoint"`
	Bucket        string `toml:"bucket"`
	Region        string `toml:"region"`
	ImmutableRoot string `toml:"immutable_root"`
}

type PublicationConfig struct {
	CatalogPath        string `toml:"catalog_path"`
	RemoteJournalPath  string `toml:"remote_journal_path"`
	ManifestRoot       string `toml:"manifest_root"`
	ReceiptRoot        string `toml:"receipt_root"`
	SealMaxBytes       uint64 `toml:"seal_max_bytes"`
	SealIntervalMS     uint64 `toml:"seal_interval_ms"`
	ScanIntervalMS     uint64 `toml:"scan_interval_ms"`
	RetryMinMS         uint64 `toml:"retry_min_ms"`
	RetryMaxMS         uint64 `toml:"retry_max_ms"`
	MaxPendingSegments uint64 `toml:"max_pending_segments"`
	MaxPendingBytes    uint64 `toml:"max_pending_bytes"`
}

type GatewayConfig struct {
	ListenAddress           string
	GatewayInstanceID       string
	WALRoot                 string
	RawOutboxRoot           string
	JournalPath             string
	MaxFrameBytes           uint32
	MaxRecords              uint32
	InitialFromMSC          int64
	InitialBatchCount       uint32
	MaximumBatchCount       uint32
	DenseBoundaryHardCap    uint32
	SessionLeaseTimeout     time.Duration
	HeartbeatIdleTimeout    time.Duration
	DiskHighFreeBytes       uint64
	DiskCriticalFreeBytes   uint64
	DiskEmergencyFreeBytes  uint64
	ProducerInstanceID      string
	ProducerBuildID         string
	DatasetID               string
	CampaignID              string
	ProviderID              string
	StableFeedID            string
	BrokerServerFingerprint string
	ExactSourceSymbol       string
	GatewayBuildIdentity    string
	DayDefinitionID         string
	SettlePolicy            string
	PublisherID             string
	PublisherEpoch          uint64
}

type Config struct {
	ListenAddress           string `toml:"listen_address"`
	GatewayInstanceID       string `toml:"gateway_instance_id"`
	WALRoot                 string `toml:"wal_root"`
	RawOutboxRoot           string `toml:"raw_outbox_root"`
	JournalPath             string `toml:"journal_path"`
	MaxFrameBytes           uint32 `toml:"max_frame_bytes"`
	MaxRecords              uint32 `toml:"max_records"`
	InitialFromMSC          int64  `toml:"initial_from_msc"`
	InitialBatchCount       uint32 `toml:"initial_batch_count"`
	MaximumBatchCount       uint32 `toml:"maximum_batch_count"`
	DenseBoundaryHardCap    uint32 `toml:"dense_boundary_hard_cap"`
	SessionLeaseTimeoutMS   uint64 `toml:"session_lease_timeout_ms"`
	HeartbeatIdleTimeoutMS  uint64 `toml:"heartbeat_idle_timeout_ms"`
	DiskHighFreeBytes       uint64 `toml:"disk_high_free_bytes"`
	DiskCriticalFreeBytes   uint64 `toml:"disk_critical_free_bytes"`
	DiskEmergencyFreeBytes  uint64 `toml:"disk_emergency_free_bytes"`
	ProducerInstanceID      string `toml:"producer_instance_id"`
	ProducerBuildID         string `toml:"producer_build_id"`
	DatasetID               string `toml:"dataset_id"`
	CampaignID              string `toml:"campaign_id"`
	ProviderID              string `toml:"provider_id"`
	StableFeedID            string `toml:"stable_feed_id"`
	BrokerServerFingerprint string `toml:"broker_server_fingerprint"`
	ExactSourceSymbol       string `toml:"exact_source_symbol"`
	GatewayBuildIdentity    string `toml:"gateway_build_identity"`
	DayDefinitionID         string `toml:"day_definition_id"`
	SettlePolicy            string `toml:"settle_policy"`
	PublisherID             string `toml:"publisher_id"`
	PublisherEpoch          uint64 `toml:"publisher_epoch"`

	Credentials CredentialsConfig `toml:"credentials"`
	R2          R2Config          `toml:"r2"`
	Publication PublicationConfig `toml:"publication"`
}

func Load(path string) (Config, error) {
	if strings.TrimSpace(path) == "" {
		return Config{}, fmt.Errorf("config path is empty")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read gateway config")
	}
	var config Config
	decoder := toml.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&config); err != nil {
		return Config{}, fmt.Errorf("decode gateway config")
	}
	if override, ok := os.LookupEnv(CredentialsPathEnv); ok && strings.TrimSpace(override) != "" {
		config.Credentials.Path = override
	}
	if err := config.Validate(); err != nil {
		return Config{}, err
	}
	return config, nil
}

func (c Config) Gateway() GatewayConfig {
	return GatewayConfig{
		ListenAddress: c.ListenAddress, GatewayInstanceID: c.GatewayInstanceID,
		WALRoot: c.WALRoot, RawOutboxRoot: c.RawOutboxRoot, JournalPath: c.JournalPath,
		MaxFrameBytes: c.MaxFrameBytes, MaxRecords: c.MaxRecords,
		InitialFromMSC: c.InitialFromMSC, InitialBatchCount: c.InitialBatchCount,
		MaximumBatchCount: c.MaximumBatchCount, DenseBoundaryHardCap: c.DenseBoundaryHardCap,
		SessionLeaseTimeout:  time.Duration(c.SessionLeaseTimeoutMS) * time.Millisecond,
		HeartbeatIdleTimeout: time.Duration(c.HeartbeatIdleTimeoutMS) * time.Millisecond,
		DiskHighFreeBytes:    c.DiskHighFreeBytes, DiskCriticalFreeBytes: c.DiskCriticalFreeBytes,
		DiskEmergencyFreeBytes: c.DiskEmergencyFreeBytes,
		ProducerInstanceID:     c.ProducerInstanceID, ProducerBuildID: c.ProducerBuildID,
		DatasetID: c.DatasetID, CampaignID: c.CampaignID, ProviderID: c.ProviderID,
		StableFeedID: c.StableFeedID, BrokerServerFingerprint: c.BrokerServerFingerprint,
		ExactSourceSymbol: c.ExactSourceSymbol, GatewayBuildIdentity: c.GatewayBuildIdentity,
		DayDefinitionID: c.DayDefinitionID, SettlePolicy: c.SettlePolicy,
		PublisherID: c.PublisherID, PublisherEpoch: c.PublisherEpoch,
	}
}

func (c Config) Validate() error {
	if c.Credentials.Provider != "" && c.Credentials.Provider != "file" {
		return fmt.Errorf("unsupported credential provider")
	}
	if c.Credentials.Protection != "" && c.Credentials.Protection != "native-acl" && c.Credentials.Protection != "managed-mount" {
		return fmt.Errorf("unsupported credential protection")
	}
	if c.R2.Endpoint != "" {
		if err := validateR2Endpoint(c.R2.Endpoint); err != nil {
			return err
		}
	}
	return nil
}

func validateR2Endpoint(value string) error {
	parsed, err := url.Parse(value)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil || parsed.Path != "" || parsed.RawPath != "" || parsed.RawQuery != "" || parsed.Fragment != "" {
		return fmt.Errorf("R2 endpoint must be an HTTPS host-only URL")
	}
	host := strings.ToLower(parsed.Hostname())
	if host == "r2.cloudflarestorage.com" || !strings.HasSuffix(host, ".r2.cloudflarestorage.com") {
		return fmt.Errorf("R2 endpoint host is not allowed")
	}
	return nil
}

func (c Config) ValidateForRun() error {
	if err := c.Validate(); err != nil {
		return err
	}
	if strings.TrimSpace(c.ListenAddress) == "" || strings.TrimSpace(c.WALRoot) == "" || strings.TrimSpace(c.JournalPath) == "" || strings.TrimSpace(c.RawOutboxRoot) == "" {
		return fmt.Errorf("run requires complete gateway configuration")
	}
	if c.Credentials.Provider != "file" || strings.TrimSpace(c.Credentials.Path) == "" {
		return fmt.Errorf("run requires file credentials configuration")
	}
	if c.R2.Endpoint == "" || c.R2.Bucket == "" || c.R2.Region == "" || c.R2.ImmutableRoot == "" {
		return fmt.Errorf("run requires complete R2 configuration")
	}
	if c.Publication.CatalogPath == "" || c.Publication.RemoteJournalPath == "" || c.Publication.ManifestRoot == "" || c.Publication.ReceiptRoot == "" {
		return fmt.Errorf("run requires complete publication configuration")
	}
	if c.Publication.SealMaxBytes == 0 || c.Publication.SealIntervalMS == 0 || c.Publication.ScanIntervalMS == 0 || c.Publication.RetryMinMS == 0 || c.Publication.RetryMaxMS == 0 || c.Publication.RetryMinMS > c.Publication.RetryMaxMS {
		return fmt.Errorf("run requires valid publication timing and size limits")
	}
	if c.Publication.MaxPendingSegments == 0 || c.Publication.MaxPendingBytes == 0 {
		return fmt.Errorf("run requires pending publication limits")
	}
	return nil
}
