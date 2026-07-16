package ingest

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"tick-data-platform/internal/protocol"
)

type Config struct {
	ListenAddress          string
	GatewayInstanceID      string
	WALRoot                string
	RawOutboxRoot          string
	JournalPath            string
	MaxFrameBytes          uint32
	MaxRecords             uint32
	InitialFromMSC         int64
	InitialBatchCount      uint32
	MaximumBatchCount      uint32
	DenseBoundaryHardCap   uint32
	SessionLeaseTimeout    time.Duration
	HeartbeatIdleTimeout   time.Duration
	DiskHighFreeBytes      uint64
	DiskCriticalFreeBytes  uint64
	DiskEmergencyFreeBytes uint64

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

func DefaultConfig() Config {
	return Config{
		ListenAddress:          "127.0.0.1:17001",
		GatewayInstanceID:      "gateway-local-01",
		MaxFrameBytes:          protocol.MaxFrameBytes,
		MaxRecords:             protocol.MaxRecords,
		InitialBatchCount:      128,
		MaximumBatchCount:      protocol.MaxRecords,
		DenseBoundaryHardCap:   protocol.MaxRecords,
		SessionLeaseTimeout:    30 * time.Second,
		HeartbeatIdleTimeout:   60 * time.Second,
		DiskHighFreeBytes:      512 << 20,
		DiskCriticalFreeBytes:  256 << 20,
		DiskEmergencyFreeBytes: 64 << 20,
	}
}

func (c Config) withDefaults() Config {
	d := DefaultConfig()
	if c.ListenAddress == "" {
		c.ListenAddress = d.ListenAddress
	}
	if c.GatewayInstanceID == "" {
		c.GatewayInstanceID = d.GatewayInstanceID
	}
	if c.MaxFrameBytes == 0 {
		c.MaxFrameBytes = d.MaxFrameBytes
	}
	if c.MaxRecords == 0 {
		c.MaxRecords = d.MaxRecords
	}
	if c.InitialBatchCount == 0 {
		c.InitialBatchCount = d.InitialBatchCount
	}
	if c.MaximumBatchCount == 0 {
		c.MaximumBatchCount = c.MaxRecords
	}
	if c.DenseBoundaryHardCap == 0 {
		c.DenseBoundaryHardCap = c.MaximumBatchCount
	}
	if c.SessionLeaseTimeout == 0 {
		c.SessionLeaseTimeout = d.SessionLeaseTimeout
	}
	if c.HeartbeatIdleTimeout == 0 {
		c.HeartbeatIdleTimeout = d.HeartbeatIdleTimeout
	}
	if c.DiskHighFreeBytes == 0 {
		c.DiskHighFreeBytes = d.DiskHighFreeBytes
	}
	if c.DiskCriticalFreeBytes == 0 {
		c.DiskCriticalFreeBytes = d.DiskCriticalFreeBytes
	}
	if c.DiskEmergencyFreeBytes == 0 {
		c.DiskEmergencyFreeBytes = d.DiskEmergencyFreeBytes
	}
	return c
}

func (c Config) Validate() error {
	c = c.withDefaults()
	if c.ListenAddress == "" || c.WALRoot == "" || c.JournalPath == "" {
		return fmt.Errorf("listen address, WAL root, and journal path are required")
	}
	if c.GatewayInstanceID == "" || len(c.GatewayInstanceID) > 255 {
		return fmt.Errorf("gateway instance id must be 1..255 bytes")
	}
	if c.MaxFrameBytes < protocol.MinFrameBytes || c.MaxFrameBytes > protocol.MaxFrameBytes {
		return fmt.Errorf("max frame bytes must be between %d and %d", protocol.MinFrameBytes, protocol.MaxFrameBytes)
	}
	if c.MaxRecords == 0 || c.MaxRecords > protocol.MaxRecords {
		return fmt.Errorf("max records must be between 1 and %d", protocol.MaxRecords)
	}
	if c.InitialBatchCount == 0 || c.InitialBatchCount > c.MaxRecords {
		return fmt.Errorf("initial batch count is outside max records")
	}
	if c.MaximumBatchCount < c.InitialBatchCount || c.MaximumBatchCount > c.MaxRecords {
		return fmt.Errorf("maximum batch count is outside configured range")
	}
	if c.DenseBoundaryHardCap < c.InitialBatchCount || c.DenseBoundaryHardCap > c.MaximumBatchCount {
		return fmt.Errorf("dense boundary hard cap is outside configured range")
	}
	if c.SessionLeaseTimeout <= 0 || c.HeartbeatIdleTimeout <= 0 {
		return fmt.Errorf("timeouts must be positive")
	}
	if err := (DiskWatermarks{HighFreeBytes: c.DiskHighFreeBytes, CriticalFreeBytes: c.DiskCriticalFreeBytes, EmergencyFreeBytes: c.DiskEmergencyFreeBytes}).Validate(); err != nil {
		return err
	}
	return nil
}

func LoadConfig(path string) (Config, error) {
	config := DefaultConfig()
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read gateway config: %w", err)
	}
	for lineNumber, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if comment := strings.IndexByte(line, '#'); comment >= 0 {
			line = strings.TrimSpace(line[:comment])
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			return Config{}, fmt.Errorf("config line %d is not key=value", lineNumber+1)
		}
		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])
		if err := setConfigValue(&config, key, value); err != nil {
			return Config{}, fmt.Errorf("config line %d: %w", lineNumber+1, err)
		}
	}
	config = config.withDefaults()
	if err := config.Validate(); err != nil {
		return Config{}, err
	}
	return config, nil
}

func setConfigValue(config *Config, key, raw string) error {
	stringValue := func() (string, error) {
		if len(raw) >= 2 && raw[0] == '"' && raw[len(raw)-1] == '"' {
			value, err := strconv.Unquote(raw)
			if err != nil {
				return "", fmt.Errorf("invalid string for %s: %w", key, err)
			}
			return value, nil
		}
		return raw, nil
	}
	uintValue := func() (uint32, error) {
		value, err := strconv.ParseUint(raw, 10, 32)
		if err != nil {
			return 0, fmt.Errorf("invalid integer for %s: %w", key, err)
		}
		return uint32(value), nil
	}
	uint64Value := func() (uint64, error) {
		value, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid integer for %s: %w", key, err)
		}
		return value, nil
	}
	intValue := func() (int64, error) {
		value, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid integer for %s: %w", key, err)
		}
		return value, nil
	}
	setString := func(destination *string) error {
		value, err := stringValue()
		if err != nil {
			return err
		}
		*destination = value
		return nil
	}
	setUint := func(destination *uint32) error {
		value, err := uintValue()
		if err != nil {
			return err
		}
		*destination = value
		return nil
	}
	setInt := func(destination *int64) error {
		value, err := intValue()
		if err != nil {
			return err
		}
		*destination = value
		return nil
	}
	switch key {
	case "listen_address":
		return setString(&config.ListenAddress)
	case "gateway_instance_id":
		return setString(&config.GatewayInstanceID)
	case "wal_root":
		return setString(&config.WALRoot)
	case "raw_outbox_root":
		return setString(&config.RawOutboxRoot)
	case "journal_path":
		return setString(&config.JournalPath)
	case "max_frame_bytes":
		return setUint(&config.MaxFrameBytes)
	case "max_records":
		return setUint(&config.MaxRecords)
	case "initial_from_msc":
		return setInt(&config.InitialFromMSC)
	case "initial_batch_count":
		return setUint(&config.InitialBatchCount)
	case "maximum_batch_count":
		return setUint(&config.MaximumBatchCount)
	case "dense_boundary_hard_cap":
		return setUint(&config.DenseBoundaryHardCap)
	case "session_lease_timeout_ms":
		var value uint32
		if err := setUint(&value); err != nil {
			return err
		}
		config.SessionLeaseTimeout = time.Duration(value) * time.Millisecond
		return nil
	case "heartbeat_idle_timeout_ms":
		var value uint32
		if err := setUint(&value); err != nil {
			return err
		}
		config.HeartbeatIdleTimeout = time.Duration(value) * time.Millisecond
		return nil
	case "disk_high_free_bytes":
		value, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid integer for %s: %w", key, err)
		}
		config.DiskHighFreeBytes = value
		return nil
	case "disk_critical_free_bytes":
		value, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid integer for %s: %w", key, err)
		}
		config.DiskCriticalFreeBytes = value
		return nil
	case "disk_emergency_free_bytes":
		value, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid integer for %s: %w", key, err)
		}
		config.DiskEmergencyFreeBytes = value
		return nil
	case "producer_instance_id":
		return setString(&config.ProducerInstanceID)
	case "producer_build_id":
		return setString(&config.ProducerBuildID)
	case "dataset_id":
		return setString(&config.DatasetID)
	case "campaign_id":
		return setString(&config.CampaignID)
	case "provider_id":
		return setString(&config.ProviderID)
	case "stable_feed_id":
		return setString(&config.StableFeedID)
	case "broker_server_fingerprint":
		return setString(&config.BrokerServerFingerprint)
	case "exact_source_symbol":
		return setString(&config.ExactSourceSymbol)
	case "gateway_build_identity":
		return setString(&config.GatewayBuildIdentity)
	case "day_definition_id":
		return setString(&config.DayDefinitionID)
	case "settle_policy":
		return setString(&config.SettlePolicy)
	case "publisher_id":
		return setString(&config.PublisherID)
	case "publisher_epoch":
		value, err := uint64Value()
		if err != nil {
			return err
		}
		config.PublisherEpoch = value
		return nil
	default:
		return nil
	}
}
